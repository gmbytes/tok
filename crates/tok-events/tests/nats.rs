//! 真实 Core NATS：独立订阅、环境隔离、坏包跳过和断线退出。
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tok_events::{
    Envelope, Event, EventId, Handler, Message, Outcome, Task,
    nats::{Bus, Config},
};
use tokio_util::sync::CancellationToken;
#[derive(Clone, PartialEq, prost::Message)]
struct Changed {
    #[prost(int64, tag = "1")]
    value: i64,
}
impl Event for Changed {
    const NAME: &'static str = "test.changed.v1";
}
fn config() -> Config {
    Config {
        url: std::env::var("TOK_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
        namespace: format!("test-{}", uuid::Uuid::new_v4()),
        operation_timeout_ms: 500,
        ..Config::default()
    }
}
fn message() -> Message {
    Message::new(&Envelope::new(EventId::generate(), Changed { value: 7 }).unwrap()).unwrap()
}
#[derive(Default)]
struct Capture(Mutex<Vec<Arc<Message>>>);
impl Handler for Capture {
    fn event_types(&self) -> Vec<&str> {
        vec![Changed::NAME]
    }
    fn handle(&self, request: tok_execution::Request<Arc<Message>>) -> Task<'_> {
        Box::pin(async move {
            self.0.lock().unwrap().push(request.input);
            Ok(tok_execution::Response::new(Outcome::Handled))
        })
    }
}

struct Interest(&'static str, Arc<Capture>);
impl Handler for Interest {
    fn event_types(&self) -> Vec<&str> {
        vec![self.0]
    }
    fn handle(&self, request: tok_execution::Request<Arc<Message>>) -> Task<'_> {
        self.1.handle(request)
    }
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn groups_share_within_each_group_and_independent_subscribers_receive_every_event() {
    let bus = Bus::connect(Config {
        max_message_bytes: 4096,
        ..config()
    })
    .await
    .unwrap();
    let stop_a = CancellationToken::new();
    let stop_b = CancellationToken::new();
    let stop_c = CancellationToken::new();
    let stop_d = CancellationToken::new();
    let a = Arc::new(Capture::default());
    let b = Arc::new(Capture::default());
    let c = Arc::new(Capture::default());
    let d = Arc::new(Capture::default());
    let tasks = [
        tokio::spawn(
            bus.subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::<Capture>::clone(&a),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    Some("mail.reward"),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop_a.clone()),
        ),
        tokio::spawn(
            bus.subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::<Capture>::clone(&b),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    Some("mail.reward"),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop_b.clone()),
        ),
        tokio::spawn(
            bus.subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::<Capture>::clone(&c),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    Some("statistics"),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop_c.clone()),
        ),
        tokio::spawn(
            bus.subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::<Capture>::clone(&d),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop_d.clone()),
        ),
    ];
    for value in 0..20 {
        bus.publish_event(Envelope::new(EventId::generate(), Changed { value }).unwrap())
            .await
            .unwrap();
    }
    wait(&c, 20).await;
    wait(&d, 20).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while a.0.lock().unwrap().len() + b.0.lock().unwrap().len() < 20 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let ids: std::collections::BTreeSet<_> =
        a.0.lock()
            .unwrap()
            .iter()
            .chain(b.0.lock().unwrap().iter())
            .map(|event| event.event_id.clone())
            .collect();
    assert_eq!(a.0.lock().unwrap().len() + b.0.lock().unwrap().len(), 20);
    assert_eq!(ids.len(), 20);
    let values: Vec<_> =
        d.0.lock()
            .unwrap()
            .iter()
            .map(|event| {
                use prost::Message as _;
                Changed::decode(event.body.as_slice()).unwrap().value
            })
            .collect();
    assert_eq!(values, (0..20).collect::<Vec<_>>()); // 单类型、单发布者的顺序。
    stop_a.cancel();
    let [first, second, third, fourth] = tasks;
    first.await.unwrap().unwrap();
    // 退订一个成员不关闭共享连接，其余成员仍消费。
    bus.publish(&message()).await.unwrap();
    wait(&c, 21).await;
    wait(&d, 21).await;
    stop_b.cancel();
    stop_c.cancel();
    stop_d.cancel();
    second.await.unwrap().unwrap();
    third.await.unwrap().unwrap();
    fourth.await.unwrap().unwrap();
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn subject_interest_filters_unrelated_traffic_and_rejects_forged_body_types() {
    let config = Config {
        subscription_capacity: 1,
        max_message_bytes: 4096,
        ..config()
    };
    let bus = Bus::connect(config.clone()).await.unwrap();
    let capture = Arc::new(Capture::default());
    let stop = CancellationToken::new();
    let task = tokio::spawn(
        bus.subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(Interest(Recorded::NAME, Arc::clone(&capture))),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .run(stop.clone()),
    );
    for _ in 0..8 {
        bus.publish(&message()).await.unwrap();
    }
    assert!(capture.0.lock().unwrap().is_empty());
    let raw = async_nats::connect(config.url.as_str()).await.unwrap();
    raw.publish(
        format!("tok.events.{}.d.{}", config.namespace, Recorded::NAME),
        message().encode(4096).unwrap().into(),
    )
    .await
    .unwrap();
    // 同连接回环只确认 broker 已接收，不依赖 flush 的本地写出语义。
    use futures_util::StreamExt;
    let inbox = raw.new_inbox();
    let mut confirmation = raw.subscribe(inbox.clone()).await.unwrap();
    raw.publish(inbox, Vec::new().into()).await.unwrap();
    confirmation.next().await.unwrap();
    assert!(capture.0.lock().unwrap().is_empty());
    bus.publish_event(Envelope::new(EventId::generate(), Recorded { value: 9 }).unwrap())
        .await
        .unwrap();
    wait(&capture, 1).await;
    assert_eq!(capture.0.lock().unwrap()[0].event_type, Recorded::NAME);
    assert_eq!(capture.0.lock().unwrap().len(), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
    bus.publish(&message()).await.unwrap();
    bus.close().await.unwrap();
    raw.drain().await.unwrap();
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn subscription_buffer_reservations_are_bounded_and_returned_on_drop() {
    let bus = Bus::connect(Config {
        max_message_bytes: 1024,
        subscription_capacity: 1,
        max_buffer_bytes: 1024,
        max_subscriptions: 2,
        ..config()
    })
    .await
    .unwrap();
    let first = bus
        .subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        bus.subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into()
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                Some("other")
            )
            .unwrap()
        )
        .await,
        Err(tok_events::Error::CapacityExceeded)
    ));
    drop(first);
    let second = bus
        .subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                Some("other"),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    drop(second);
    bus.publish(&message()).await.unwrap();
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn handler_timeout_does_not_retry_or_block_later_events() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let bus = Bus::connect(Config {
        handler_timeout_ms: 30,
        ..config()
    })
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let mut handlers = tok_events::Registry::new(1).unwrap();
    let called = Arc::clone(&calls);
    let completed = Arc::clone(&done);
    handlers
        .register::<Changed, _>(move |request| {
            let event = &request.input.event.payload;
            called.fetch_add(1, Ordering::SeqCst);
            let done = Arc::clone(&completed);
            Box::pin(async move {
                if event.value == 7 {
                    std::future::pending::<()>().await;
                }
                done.notify_one();
                Ok(tok_execution::Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    handlers.enable_protobuf::<Changed>().unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(
        bus.subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(handlers),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .run(stop.clone()),
    );
    bus.publish(&message()).await.unwrap();
    bus.publish_event(Envelope::new(EventId::generate(), Changed { value: 8 }).unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), done.notified())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    stop.cancel();
    task.await.unwrap().unwrap();
    bus.close().await.unwrap();
}
async fn wait(capture: &Capture, count: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while capture.0.lock().unwrap().len() < count {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
}

#[derive(Clone, PartialEq, prost::Message)]
struct Recorded {
    #[prost(int64, tag = "1")]
    value: i64,
}
impl Event for Recorded {
    const NAME: &'static str = "test.audit.recorded";
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn ordinary_service_can_subscribe_handle_and_publish_another_event() {
    let config = config();
    let service = Bus::connect(config.clone()).await.unwrap();
    let audit = Bus::connect(config.clone()).await.unwrap();
    let publisher = Bus::connect(config).await.unwrap();
    let mut handlers = tok_events::Registry::new(1).unwrap();
    let output = service.clone();
    handlers
        .register::<Changed, _>(move |request| {
            let event = &request.input.event.payload;
            let output = output.clone();
            Box::pin(async move {
                output
                    .publish_event(Envelope::new(
                        EventId::generate(),
                        Recorded { value: event.value },
                    )?)
                    .await
                    .map(|()| tok_execution::Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    handlers.enable_protobuf::<Changed>().unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let result = Arc::clone(&captured);
    let mut audit_handlers = tok_events::Registry::new(1).unwrap();
    audit_handlers
        .register::<Recorded, _>(move |request| {
            let event = &request.input.event.payload;
            let result = Arc::clone(&result);
            Box::pin(async move {
                result.lock().unwrap().push(event.value);
                Ok(tok_execution::Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    audit_handlers.enable_protobuf::<Recorded>().unwrap();
    let stop = CancellationToken::new();
    let service_task = tokio::spawn(
        service
            .subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::new(handlers),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop.clone()),
    );
    let audit_task = tokio::spawn(
        audit
            .subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::new(audit_handlers),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop.clone()),
    );
    publisher
        .publish_event(Envelope::new(EventId::generate(), Changed { value: 7 }).unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while captured.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*captured.lock().unwrap(), vec![7]);
    stop.cancel();
    service_task.await.unwrap().unwrap();
    audit_task.await.unwrap().unwrap();
    publisher.close().await.unwrap();
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn shutdown_cancels_inflight_async_handler() {
    let config = config();
    let subscriber = Bus::connect(config.clone()).await.unwrap();
    let publisher = Bus::connect(config).await.unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let captured = Arc::clone(&started);
    let mut handlers = tok_events::Registry::new(1).unwrap();
    handlers
        .register::<Changed, _>(move |_request| {
            captured.notify_one();
            Box::pin(std::future::pending())
        })
        .unwrap();
    handlers.enable_protobuf::<Changed>().unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(
        subscriber
            .subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::new(handlers),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop.clone()),
    );
    publisher.publish(&message()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    publisher.close().await.unwrap();
}
#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn independent_subscriptions_receive_same_event_and_namespaces_are_isolated() {
    let config = config();
    let a = Bus::connect(Config {
        max_subscriptions: 1,
        ..config.clone()
    })
    .await
    .unwrap();
    let b = Bus::connect(config.clone()).await.unwrap();
    let c = Bus::connect(Config {
        namespace: format!("{}-other", config.namespace),
        ..config.clone()
    })
    .await
    .unwrap();
    let publisher = Bus::connect(config).await.unwrap();
    let stop = CancellationToken::new();
    let ca = Arc::new(Capture::default());
    let cb = Arc::new(Capture::default());
    let cc = Arc::new(Capture::default());
    let sa = a
        .subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::<Capture>::clone(&ca),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        a.subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into()
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None
            )
            .unwrap()
        )
        .await,
        Err(tok_events::Error::CapacityExceeded)
    ));
    let tasks = [
        tokio::spawn(sa.run(stop.clone())),
        tokio::spawn(
            b.subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::<Capture>::clone(&cb),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop.clone()),
        ),
        tokio::spawn(
            c.subscribe(
                tok_events::Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::<Capture>::clone(&cc),
                    tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop.clone()),
        ),
    ];
    let mut message = message();
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    message.trace_context.traceparent = Some(traceparent.into());
    publisher.publish(&message).await.unwrap();
    publisher.publish(&message).await.unwrap();
    wait(&ca, 2).await;
    wait(&cb, 2).await;
    assert!(cc.0.lock().unwrap().is_empty());
    {
        let captured = ca.0.lock().unwrap();
        assert_eq!(captured[0].event_id, captured[1].event_id);
        assert_eq!(
            captured[0].trace_context.traceparent.as_deref(),
            Some(traceparent)
        );
    }
    stop.cancel();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    publisher.close().await.unwrap();
}
#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn invalid_and_oversized_packets_do_not_reach_sink_or_stop_later_events() {
    let config = Config {
        max_message_bytes: 1024,
        ..config()
    };
    let bus = Bus::connect(config.clone()).await.unwrap();
    let capture = Arc::new(Capture::default());
    let stop = CancellationToken::new();
    let task = tokio::spawn(
        bus.subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::<Capture>::clone(&capture),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .run(stop.clone()),
    );
    let raw = async_nats::connect(config.url.as_str()).await.unwrap();
    let subject = format!("tok.events.{}.d.{}", config.namespace, Changed::NAME);
    raw.publish(subject.clone(), vec![255].into())
        .await
        .unwrap();
    raw.publish(subject, vec![1; 2048].into()).await.unwrap();
    raw.flush().await.unwrap();
    let publisher = Bus::connect(config).await.unwrap();
    publisher.publish(&message()).await.unwrap();
    wait(&capture, 1).await;
    assert_eq!(capture.0.lock().unwrap().len(), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
    publisher.close().await.unwrap();
    raw.drain().await.unwrap();
}
#[tokio::test]
async fn invalid_configuration_is_rejected_without_a_connection() {
    let config = config();
    assert!(
        Config {
            namespace: "bad.*".into(),
            ..config.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            client_capacity: 0,
            ..config.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        !format!(
            "{:?}",
            Config {
                url: "nats://user:secret@localhost".into(),
                ..config
            }
        )
        .contains("secret")
    );
    let error = Bus::connect(Config {
        url: "nats://127.0.0.1:1".into(),
        operation_timeout_ms: 30,
        ..Config::default()
    })
    .await;
    assert!(matches!(error, Err(tok_events::Error::Unavailable(_))));
}
#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn dropping_consumer_future_keeps_shared_connection_usable() {
    let bus = Bus::connect(config()).await.unwrap();
    let subscription = bus
        .subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let task = tokio::spawn(subscription.run(CancellationToken::new()));
    tokio::task::yield_now().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    bus.publish(&message()).await.unwrap();
    let _result = bus.close().await;
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn subscription_overflow_is_reported_without_disabling_publishing() {
    let config = Config {
        subscription_capacity: 1,
        ..config()
    };
    let bus = Bus::connect(config.clone()).await.unwrap();
    let subscription = bus
        .subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let publisher = Bus::connect(config).await.unwrap();
    for _ in 0..4 {
        publisher.publish(&message()).await.unwrap();
    }
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        subscription.run(CancellationToken::new()),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(tok_events::Error::Unavailable(_))));
    bus.publish(&message()).await.unwrap();
    publisher.close().await.unwrap();
}
#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn transport_disconnect_fails_consumer_and_stops_new_publications() {
    let config = config();
    let address = config.url.parse::<async_nats::ServerAddr>().unwrap();
    let host = address.host().to_string();
    let port = address.port();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let stop = CancellationToken::new();
    let signal = stop.clone();
    let proxy = tokio::spawn(async move {
        let (mut input, _) = listener.accept().await.unwrap();
        let mut output = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .unwrap();
        tokio::select! {result=tokio::io::copy_bidirectional(&mut input,&mut output)=>{result.unwrap();},()=signal.cancelled()=>{}}
    });
    let (_, rest) = config.url.split_once("://").unwrap();
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let auth = authority
        .rsplit_once('@')
        .map(|(auth, _)| format!("{auth}@"))
        .unwrap_or_default();
    let bus = Bus::connect(Config {
        url: format!("nats://{auth}127.0.0.1:{proxy_port}/{path}"),
        ..config
    })
    .await
    .unwrap();
    let task = tokio::spawn(
        bus.subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .run(CancellationToken::new()),
    );
    stop.cancel();
    proxy.await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap(),
        Err(tok_events::Error::Unavailable(_))
    ));
    assert!(bus.publish(&message()).await.is_err());
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn publishing_needs_no_reply_but_subscription_setup_requires_broker_readiness() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let config = config();
    let address = config.url.parse::<async_nats::ServerAddr>().unwrap();
    let host = address.host().to_string();
    let port = address.port();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let mute = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let probes = Arc::clone(&subscriptions);
    let sent = Arc::new(tokio::sync::Notify::new());
    let stop = CancellationToken::new();
    let (muted, observed, notified, signal) = (
        Arc::clone(&mute),
        Arc::clone(&writes),
        Arc::clone(&sent),
        stop.clone(),
    );
    let proxy = tokio::spawn(async move {
        let (input, _) = listener.accept().await.unwrap();
        let output = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .unwrap();
        let (mut input, mut back) = input.into_split();
        let (mut replies, mut commands) = output.into_split();
        let forward = async {
            let mut buffer = [0u8; 4096];
            let mut tail = Vec::new();
            let mut subscription_tail = Vec::new();
            loop {
                let count = input.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                tail.extend_from_slice(&buffer[..count]);
                let marker = b"PUB tok.events.";
                let publications = tail
                    .windows(marker.len())
                    .filter(|window| *window == marker)
                    .count();
                if publications > 0 {
                    observed.fetch_add(publications, Ordering::SeqCst);
                    notified.notify_one();
                }
                subscription_tail.extend_from_slice(&buffer[..count]);
                let probe_marker = b"SUB _INBOX.";
                probes.fetch_add(
                    subscription_tail
                        .windows(probe_marker.len())
                        .filter(|window| *window == probe_marker)
                        .count(),
                    Ordering::SeqCst,
                );
                subscription_tail = subscription_tail[subscription_tail
                    .len()
                    .saturating_sub(probe_marker.len() - 1)..]
                    .to_vec();
                tail = tail[tail.len().saturating_sub(marker.len() - 1)..].to_vec();
                commands.write_all(&buffer[..count]).await?;
            }
            Ok::<(), std::io::Error>(())
        };
        let backward = async {
            let mut buffer = [0u8; 4096];
            loop {
                let count = replies.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                if !muted.load(Ordering::SeqCst) {
                    back.write_all(&buffer[..count]).await?;
                }
            }
            Ok::<(), std::io::Error>(())
        };
        tokio::select! {result=async{tokio::try_join!(forward,backward)}=>{result.unwrap();},()=signal.cancelled()=>{}}
    });
    let (_, rest) = config.url.split_once("://").unwrap();
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let auth = authority
        .rsplit_once('@')
        .map(|(auth, _)| format!("{auth}@"))
        .unwrap_or_default();
    let bus = Arc::new(
        Bus::connect(Config {
            url: format!("nats://{auth}127.0.0.1:{proxy_port}/{path}"),
            client_capacity: 1,
            operation_timeout_ms: 300,
            max_pending_subscriptions: 1,
            ..config
        })
        .await
        .unwrap(),
    );
    // 订阅初始化先完成回环确认；之后屏蔽所有服务器回应，但保留上行连接。
    let ready = bus
        .subscribe(
            tok_events::Subscriber::new(
                tok_core::service::Identity {
                    node_id: "test".into(),
                    service_name: "events".into(),
                },
                Arc::new(Capture::default()),
                tok_events::TopicFilter::Exact(tok_events::Topic::default()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let before = subscriptions.load(Ordering::SeqCst);
    mute.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_millis(100), bus.publish(&message()))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), sent.notified())
        .await
        .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        subscriptions.load(Ordering::SeqCst),
        before,
        "发布不能创建临时回环订阅"
    );
    // 第一份建立等待 broker；探测 SID 也有容量，第二份不能继续创建探测。
    let subscriber = || {
        tok_events::Subscriber::new(
            tok_core::service::Identity {
                node_id: "test".into(),
                service_name: "events".into(),
            },
            Arc::new(Capture::default()),
            tok_events::TopicFilter::Exact(tok_events::Topic::default()),
            None,
        )
        .unwrap()
    };
    let opening = tokio::spawn({
        let bus = Arc::clone(&bus);
        let subscriber = subscriber();
        async move { bus.subscribe(subscriber).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while subscriptions.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        bus.subscribe(subscriber()).await,
        Err(tok_events::Error::CapacityExceeded)
    ));
    assert!(matches!(
        opening.await.unwrap(),
        Err(tok_events::Error::Unavailable("NATS 订阅超时"))
    ));
    // 建立失败释放本次探测名额，后续显式建立可进入就绪等待，而不是容量泄漏。
    assert!(matches!(
        bus.subscribe(subscriber()).await,
        Err(tok_events::Error::Unavailable("NATS 订阅超时"))
    ));
    // 订阅初始化失败不影响客户端继续接受发布，也不重发之前的事件。
    bus.publish(&message()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while writes.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    drop(ready);
    stop.cancel();
    proxy.await.unwrap();
}

#[derive(Clone, PartialEq, prost::Message)]
struct Routed {
    #[prost(int64, tag = "1")]
    value: i64,
    #[prost(string, optional, tag = "2")]
    route: Option<String>,
}
impl Event for Routed {
    const NAME: &'static str = "test.routed.v1";
    fn topic(&self) -> Result<tok_events::Topic, tok_events::Error> {
        self.route.as_ref().map_or_else(
            || Ok(tok_events::Topic::default()),
            tok_events::Topic::named,
        )
    }
}
#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn topic_routes_isolate_default_named_namespaces_and_groups_and_reject_forged_routes() {
    use prost::Message as _;
    use tok_events::{Subscriber, Topic, TopicFilter};
    let config = Config {
        max_message_bytes: 4096,
        ..config()
    };
    let bus = Bus::connect(config.clone()).await.unwrap();
    let other = Bus::connect(Config {
        namespace: format!("other-{}", config.namespace),
        ..config.clone()
    })
    .await
    .unwrap();
    let a = Topic::named("chat:7.区服").unwrap();
    let b = Topic::named("chat/7").unwrap();
    let named_default = Topic::named("default").unwrap();
    let captures: Vec<_> = (0..8).map(|_| Arc::new(Capture::default())).collect();
    let interests = [
        (TopicFilter::Exact(Topic::default()), None),
        (TopicFilter::Exact(named_default.clone()), None),
        (TopicFilter::Exact(a.clone()), None),
        (TopicFilter::Exact(b.clone()), None),
        (TopicFilter::All, None),
        (TopicFilter::Exact(a.clone()), Some("workers")),
        (TopicFilter::Exact(a.clone()), Some("workers")),
        (TopicFilter::Exact(a.clone()), Some("audit")),
    ];
    let stop = CancellationToken::new();
    let mut tasks = Vec::new();
    for ((topics, group), capture) in interests.into_iter().zip(&captures) {
        tasks.push(tokio::spawn(
            bus.subscribe(
                Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "events".into(),
                    },
                    Arc::new(Interest(Routed::NAME, Arc::clone(capture))),
                    topics,
                    group,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .run(stop.clone()),
        ));
    }
    let event = |topic: Topic, value| {
        Envelope::new(
            EventId::generate(),
            Routed {
                value,
                route: topic.as_str().map(str::to_owned),
            },
        )
        .unwrap()
    };
    other.publish_event(event(a.clone(), 999)).await.unwrap();
    for topic in [Topic::default(), named_default.clone(), b.clone()] {
        bus.publish_event(event(topic, -1)).await.unwrap();
    }
    for value in 0..20 {
        bus.publish_event(event(a.clone(), value)).await.unwrap();
    }
    wait(&captures[0], 1).await;
    wait(&captures[1], 1).await;
    wait(&captures[2], 20).await;
    wait(&captures[3], 1).await;
    wait(&captures[4], 23).await;
    wait(&captures[7], 20).await;

    // 原始发送者恶意交换正文路由，或使用非规范 subject；All 也必须拒绝。
    let raw = async_nats::connect(config.url.as_str()).await.unwrap();
    let token = format!(
        "n{}",
        a.as_str()
            .unwrap()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let subject = format!("tok.events.{}.{token}.{}", config.namespace, Routed::NAME);
    let forged = Message::new(&event(b.clone(), 888)).unwrap();
    raw.publish(subject.clone(), forged.encode(4096).unwrap().into())
        .await
        .unwrap();
    let correct = Message::new(&event(a.clone(), 777)).unwrap();
    raw.publish(
        format!("tok.events.{}.d.{}", config.namespace, Routed::NAME),
        correct.encode(4096).unwrap().into(),
    )
    .await
    .unwrap();
    raw.publish(
        format!("tok.events.{}.invalid.{}", config.namespace, Routed::NAME),
        correct.encode(4096).unwrap().into(),
    )
    .await
    .unwrap();
    let mut invalid =
        tok_proto::internal::v1::EventEnvelope::decode(correct.encode(4096).unwrap().as_slice())
            .unwrap();
    invalid.topic = Some(String::new());
    raw.publish(subject.clone(), invalid.encode_to_vec().into())
        .await
        .unwrap();
    let mut wrong_type = correct.clone();
    wrong_type.event_type = Recorded::NAME.into();
    raw.publish(subject.clone(), wrong_type.encode(4096).unwrap().into())
        .await
        .unwrap();
    // 同一连接的有效尾标保证前序坏包已在每个匹配订阅中被处理。
    raw.publish(subject, correct.encode(4096).unwrap().into())
        .await
        .unwrap();
    wait(&captures[2], 21).await;
    wait(&captures[4], 24).await;
    wait(&captures[7], 21).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while captures[5].0.lock().unwrap().len() + captures[6].0.lock().unwrap().len() < 21 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for (capture, expected) in captures.iter().zip([1, 1, 21, 1, 24, 0, 0, 21]) {
        let messages = capture.0.lock().unwrap();
        if expected > 0 {
            assert_eq!(messages.len(), expected);
        }
        assert!(messages.iter().all(|message| !matches!(
            Changed::decode(message.body.as_slice()).unwrap().value,
            888 | 999
        )));
    }
    let grouped: std::collections::BTreeSet<_> = captures[5]
        .0
        .lock()
        .unwrap()
        .iter()
        .chain(captures[6].0.lock().unwrap().iter())
        .map(|event| event.event_id.clone())
        .collect();
    assert_eq!(
        captures[5].0.lock().unwrap().len() + captures[6].0.lock().unwrap().len(),
        21
    );
    assert_eq!(grouped.len(), 21);
    assert_eq!(captures[0].0.lock().unwrap()[0].topic, Topic::default());
    assert_eq!(captures[1].0.lock().unwrap()[0].topic, named_default);
    assert!(
        captures[2]
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|event| event.topic == a)
    );
    stop.cancel();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    raw.drain().await.unwrap();
    other.close().await.unwrap();
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn scoped_publisher_keeps_local_extensions_and_receiver_installs_its_own_scope() {
    use tower::ServiceExt as _;
    let bus = Bus::connect(config()).await.unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    let mut registry = tok_events::Registry::default();
    registry
        .register::<Changed, _>(move |request| {
            let seen = Arc::clone(&seen);
            assert_eq!(request.context.service().service_name, "consumer");
            assert_eq!(
                request.extensions().get::<String>().unwrap(),
                "consumer-capability"
            );
            Box::pin(async move {
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(tok_execution::Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    registry.enable_protobuf::<Changed>().unwrap();
    let mut extensions = tok_core::Extensions::default();
    extensions.insert(String::from("consumer-capability"));
    let subscriber = tok_events::Subscriber::new(
        tok_core::Identity {
            node_id: "test".into(),
            service_name: "consumer".into(),
        },
        Arc::new(registry),
        tok_events::TopicFilter::Exact(tok_events::Topic::default()),
        None,
    )
    .unwrap()
    .with_extensions(extensions);
    let subscription = bus.subscribe(subscriber).await.unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(subscription.run(stop.clone()));
    let context = tok_execution::Context::new(
        tok_core::Identity {
            node_id: "test".into(),
            service_name: "producer".into(),
        },
        "publish",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap();
    let mut extensions = tok_core::Extensions::default();
    extensions.insert(String::from("producer-capability"));
    let policy = tower::ServiceBuilder::new()
        .map_request(|request: tok_execution::Request<Message>| {
            assert_eq!(
                request.extensions().get::<String>().unwrap(),
                "producer-capability"
            );
            request
        })
        .service(bus.publisher());
    let cancelled = context.clone().with_cancellation(CancellationToken::new());
    cancelled.cancellation().cancel();
    let result = bus
        .publish_request(tok_execution::Request::new(
            cancelled,
            Envelope::new(EventId::generate(), Changed { value: 1 }).unwrap(),
        ))
        .await;
    assert!(matches!(
        result,
        Err(tok_events::Error::Stopped(
            tok_execution::StopReason::Cancelled
        ))
    ));
    let expired = context.clone().with_budget_limit(Duration::ZERO);
    assert!(matches!(
        policy
            .clone()
            .oneshot(tok_execution::Request::from_parts(
                expired,
                message(),
                extensions.clone()
            ))
            .await,
        Err(tok_events::Error::Stopped(
            tok_execution::StopReason::DeadlineExceeded
        ))
    ));
    tokio::spawn(policy.oneshot(tok_execution::Request::from_parts(
        context,
        message(),
        extensions,
    )))
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while count.load(std::sync::atomic::Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
    bus.close().await.unwrap();
}
