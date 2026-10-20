//! TCP 长度分帧与版本化客户端 protobuf 信封。

use crate::{ErrorCode, Result, protocol::Frame};
use prost::Message;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// 客户端协议的当前版本。
pub const VERSION: u32 = 3;

/// 有长度上限和整帧期限的 TCP 编解码器，不能取消半帧读取后继续复用连接。
#[derive(Debug, Clone)]
pub struct FrameCodec {
    limit: usize,
    timeout: Duration,
}

impl FrameCodec {
    /// 指定正文上限和帧读写期限，拒绝零值和超过 16 MiB 的限制。
    pub fn new(limit: usize, timeout: Duration) -> Result<Self> {
        if limit == 0
            || limit > 16 * 1024 * 1024
            || timeout.is_zero()
            || timeout > Duration::from_secs(86_400)
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        Ok(Self { limit, timeout })
    }

    /// 读取完整帧；帧边界上的 EOF 返回 None，半帧或非法版本返回错误。
    pub async fn read<R: AsyncRead + Unpin>(&self, reader: &mut R) -> Result<Option<Frame>> {
        tokio::time::timeout(self.timeout, async {
            let mut length = [0u8; 4];
            let count = reader
                .read(&mut length[..1])
                .await
                .map_err(|_| ErrorCode::SessionClosed)?;
            if count == 0 {
                return Ok(None);
            }
            reader
                .read_exact(&mut length[1..])
                .await
                .map_err(|_| ErrorCode::SessionClosed)?;
            let size = u32::from_be_bytes(length) as usize;
            if size == 0 {
                return Err(ErrorCode::InvalidRequest.into());
            }
            if size > self.limit {
                return Err(ErrorCode::FrameTooLarge.into());
            }
            let mut body = vec![0u8; size];
            reader
                .read_exact(&mut body)
                .await
                .map_err(|_| ErrorCode::SessionClosed)?;
            let frame = Frame::decode(body.as_slice()).map_err(|_| ErrorCode::InvalidRequest)?;
            if frame.version != VERSION || frame.content.is_none() {
                return Err(ErrorCode::InvalidRequest.into());
            }
            Ok(Some(frame))
        })
        .await
        .map_err(|_| ErrorCode::Timeout)?
    }

    /// 检查长度后写入整帧，不自动重发失败的写入。
    pub async fn write<W: AsyncWrite + Unpin>(&self, writer: &mut W, frame: &Frame) -> Result<()> {
        let size = frame.encoded_len();
        if size == 0 || frame.version != VERSION || frame.content.is_none() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if size > self.limit {
            return Err(ErrorCode::FrameTooLarge.into());
        }
        self.write_body(writer, &frame.encode_to_vec()).await
    }

    // 发送已经检查并缓存的最终正文；不再重编码或替换业务内容。
    pub(crate) async fn write_body<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        body: &[u8],
    ) -> Result<()> {
        let size = body.len();
        if size == 0 || size > self.limit {
            return Err(ErrorCode::FrameTooLarge.into());
        }
        tokio::time::timeout(self.timeout, async {
            writer
                .write_all(&(size as u32).to_be_bytes())
                .await
                .map_err(|_| ErrorCode::SessionClosed)?;
            writer
                .write_all(body)
                .await
                .map_err(|_| ErrorCode::SessionClosed)?;
            writer.flush().await.map_err(|_| ErrorCode::SessionClosed)?;
            Ok(())
        })
        .await
        .map_err(|_| ErrorCode::Timeout)?
    }
}
