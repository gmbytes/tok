//! Core NATS 的可信服务器事件示例；客户端请求表不注册内部增量事件。
use crate::protocol::{GameMessage, v1};
use prost::Message;
use tok::events::Event;

/// 本示例固定的区服主题；不按 player_id 建立订阅。
pub const TOPIC: &str = "realm:demo";

/// 可信服务端发给 player 的增量事件，示例业务不提供跨发布去重。
#[derive(Clone, PartialEq, Message)]
pub struct AddValue {
    /// 增量，生产场景应由业务规则和幂等机制校验。
    #[prost(int64, tag = "1")]
    pub delta: i64,
    /// 订阅者按业务正文选择目标，通用总线不解释玩家身份。
    #[prost(string, tag = "2")]
    pub player_id: String,
}
impl Event for AddValue {
    const NAME: &'static str = "demo.player.add_value.v1";
    fn topic(&self) -> Result<tok::events::Topic, tok::events::Error> {
        tok::events::Topic::named(TOPIC)
    }
}
/// 服务器值变更事件；网关将其转换为客户端 ValueNotice。
#[derive(Clone, PartialEq, Message)]
pub struct ValueChanged {
    /// 玩家业务 ID。
    #[prost(string, tag = "1")]
    pub player_id: String,
    /// 当前值。
    #[prost(int64, tag = "2")]
    pub value: i64,
}
impl Event for ValueChanged {
    const NAME: &'static str = "demo.player.value_changed.v1";
    fn topic(&self) -> Result<tok::events::Topic, tok::events::Error> {
        tok::events::Topic::named(TOPIC)
    }
}

/// 独立的玩家转发组件；消费组由部署组装显式选择。
pub fn player_forwarder(
    identity: tok::core::service::Identity,
    sessions: tok::gateway::player::SessionManager,
    topics: tok::events::TopicFilter,
    group: Option<&str>,
) -> Result<tok::events::Subscriber, tok::events::Error> {
    use tok::gateway::player::events::{Target, forward};
    let handler = forward::<AddValue, _>(sessions, |event| {
        Ok(Target::Players(vec![event.payload.player_id.clone()]))
    })?;
    tok::events::Subscriber::new(identity, std::sync::Arc::new(handler), topics, group)
}

/// 独立的通知组件；可与转发组件分别启停、选择消费组或替换处理逻辑。
pub fn value_notifier(
    identity: tok::core::service::Identity,
    sessions: tok::gateway::player::SessionManager,
    topics: tok::events::TopicFilter,
    group: Option<&str>,
) -> Result<tok::events::Subscriber, tok::events::Error> {
    use tok::gateway::player::events::{Target, notify};
    let handler = notify::<ValueChanged, _>(sessions, v1::ValueNotice::MESSAGE_ID, |event| {
        Ok((
            Target::Players(vec![event.payload.player_id.clone()]),
            v1::ValueNotice {
                value: event.payload.value,
            }
            .encode_to_vec(),
        ))
    })?;
    tok::events::Subscriber::new(identity, std::sync::Arc::new(handler), topics, group)
}

/// 真实 TCP + NATS + RPC 验证：内部事件修改状态，广播进入客户端 Notify。
pub async fn smoke(
    address: std::net::SocketAddr,
    config: tok::events::nats::Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tok::events::{Envelope, EventId, Message as EventMessage, nats::Bus};
    use tok::gateway::{
        codec::{FrameCodec, VERSION},
        protocol::{self, Frame, frame::Content},
    };
    let bus = Bus::connect(config).await?;
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    let codec = FrameCodec::new(1_048_576, std::time::Duration::from_secs(2))?;
    codec
        .write(
            &mut stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Login(protocol::LoginRequest {
                    request_id: 1,
                    credential: "demo:4242".into(),
                    resume_token: String::new(),
                })),
            },
        )
        .await?;
    let Some(Content::LoggedIn(login)) = codec
        .read(&mut stream)
        .await?
        .and_then(|frame| frame.content)
    else {
        return Err(std::io::Error::other("缺少登录回应").into());
    };
    if login.error_code != 0 {
        return Err(std::io::Error::other("登录失败").into());
    }
    bus.publish(&EventMessage::new(&Envelope::new(
        EventId::generate(),
        AddValue {
            delta: 7,
            player_id: "4242".into(),
        },
    )?)?)
    .await?;
    let mut request_id = 1;
    loop {
        request_id += 1;
        codec
            .write(
                &mut stream,
                &Frame {
                    version: VERSION,
                    content: Some(Content::Request(protocol::BusinessRequest {
                        request_id,
                        message_id: v1::GetPlayerRequest::MESSAGE_ID,
                        body: vec![],
                    })),
                },
            )
            .await?;
        let Some(Content::Response(response)) = codec
            .read(&mut stream)
            .await?
            .and_then(|frame| frame.content)
        else {
            return Err(std::io::Error::other("缺少玩家回应").into());
        };
        if response.error_code != 0 {
            return Err(std::io::Error::other("玩家查询失败").into());
        }
        if v1::GetPlayerResponse::decode(response.body.as_slice())?.value == 7 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    bus.publish(&EventMessage::new(&Envelope::new(
        EventId::generate(),
        ValueChanged {
            player_id: "4242".into(),
            value: 7,
        },
    )?)?)
    .await?;
    let Some(Content::Notify(notify)) = codec
        .read(&mut stream)
        .await?
        .and_then(|frame| frame.content)
    else {
        return Err(std::io::Error::other("缺少广播通知").into());
    };
    if notify.message_id != v1::ValueNotice::MESSAGE_ID
        || notify.server_seq == 0
        || v1::ValueNotice::decode(notify.body.as_slice())?.value != 7
    {
        return Err(std::io::Error::other("广播内容或序列无效").into());
    }
    bus.close().await?;
    Ok(())
}
