use crate::Request;

/// 将同步处理函数适配为相同的 Tower Service，不创建线程或任务。
pub fn sync_handler<F>(handler: F) -> SyncHandler<F> {
    SyncHandler(std::sync::Arc::new(handler))
}

/// 同步回调的 Tower 适配；输入可以包含当前调用的借用。
pub struct SyncHandler<F>(std::sync::Arc<F>);
impl<F> Clone for SyncHandler<F> {
    fn clone(&self) -> Self {
        Self(std::sync::Arc::clone(&self.0))
    }
}
impl<T, O, E, F> tower::Service<Request<T>> for SyncHandler<F>
where
    F: Fn(Request<T>) -> Result<O, E>,
{
    type Response = O;
    type Error = E;
    type Future = std::future::Ready<Result<O, E>>;
    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), E>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        std::future::ready((self.0)(request))
    }
}
