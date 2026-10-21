//! 客户端与服务端共用的 gRPC 超时解析。
use std::time::Duration;
use tonic::Status;

pub(super) fn request_budget(
    metadata: &tonic::metadata::MetadataMap,
    limit: Duration,
) -> Result<Duration, Status> {
    let Some(value) = metadata.get("grpc-timeout") else {
        return Ok(limit);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::invalid_argument("RPC 超时格式无效"))?;
    if !(2..=9).contains(&value.len()) {
        return Err(Status::invalid_argument("RPC 超时格式无效"));
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Status::invalid_argument("RPC 超时格式无效"));
    }
    let count = digits
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("RPC 超时格式无效"))?;
    let duration = match unit {
        "H" => Duration::from_secs(count * 3600),
        "M" => Duration::from_secs(count * 60),
        "S" => Duration::from_secs(count),
        "m" => Duration::from_millis(count),
        "u" => Duration::from_micros(count),
        "n" => Duration::from_nanos(count),
        _ => return Err(Status::invalid_argument("RPC 超时单位无效")),
    };
    Ok(duration.min(limit))
}
