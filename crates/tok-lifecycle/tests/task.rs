//! 通用任务范围：容量、退出完成与外部跟踪边界。
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_lifecycle::task::{Error, State, Supervisor, Task};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
const LIMIT: Duration = Duration::from_secs(2);
struct Released(Arc<AtomicUsize>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn scope(capacity: usize) -> Supervisor {
    Supervisor::new(capacity, CancellationToken::new()).unwrap()
}
#[tokio::test]
async fn reserved_queued_and_running_tasks_share_one_capacity() {
    let mut scope = scope(1);
    let handle = scope.handle();
    let reservation = handle.reserve().unwrap();
    assert_eq!(handle.used_capacity(), 1);
    assert_eq!(
        handle.reserve().err().unwrap(),
        tok_lifecycle::task::SubmitError::CapacityExceeded
    );
    drop(reservation);
    let (done, completion) = oneshot::channel();
    handle
        .launch(
            "work",
            Box::pin(async move {
                completion.await.unwrap();
                Ok(())
            }),
        )
        .await
        .unwrap();
    assert!(handle.reserve().is_err());
    let (signal, mut signals) = mpsc::unbounded_channel();
    let tasks = handle.clone();
    let task = tokio::spawn(async move {
        let completed = scope.next().await;
        assert_eq!(completed.name, "work");
        completed.result.unwrap();
        signal.send(()).unwrap();
        tasks.cancellation().cancelled().await;
        scope.shutdown(Some(LIMIT)).await.unwrap();
    });
    done.send(()).unwrap();
    signals.recv().await.unwrap();
    assert_eq!(handle.used_capacity(), 0);
    let reservation = handle.reserve().unwrap();
    handle.request_stop();
    let count = Arc::new(AtomicUsize::new(0));
    let released = Released(Arc::clone(&count));
    assert!(
        reservation
            .launch(
                "late",
                Box::pin(async move {
                    let _released = released;
                    Ok(())
                })
            )
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    task.await.unwrap();
    handle.wait_finished().await;
}

#[tokio::test]
async fn dropping_owner_reclaims_queued_and_running_work_before_finished() {
    let mut scope = scope(2);
    let handle = scope.handle();
    let count = Arc::new(AtomicUsize::new(0));
    let running_guard = Released(Arc::clone(&count));
    let queued_guard = Released(Arc::clone(&count));
    let (began, started) = oneshot::channel();
    handle
        .launch(
            "running",
            Box::pin(async move {
                let _guard = running_guard;
                began.send(()).unwrap();
                pending().await
            }),
        )
        .await
        .unwrap();
    tokio::select! { _ = scope.next() => panic!("work unexpectedly completed"), _ = started => {} }
    handle
        .launch(
            "queued",
            Box::pin(async move {
                let _guard = queued_guard;
                pending().await
            }),
        )
        .await
        .unwrap();
    drop(scope);
    tokio::time::timeout(LIMIT, handle.wait_finished())
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(handle.state(), State::Stopped);
    assert!(handle.reserve().is_err());
}

#[tokio::test]
async fn parent_completion_includes_dropped_child_scopes_and_their_tasks() {
    let parent = scope(1);
    let handle = parent.handle();
    let mut child = Supervisor::child(1, &handle).unwrap();
    let child_handle = child.handle();
    let released = Arc::new(AtomicUsize::new(0));
    let guard = Released(Arc::clone(&released));
    let (began, started) = oneshot::channel();
    child_handle
        .launch(
            "child-work",
            Box::pin(async move {
                let _guard = guard;
                began.send(()).unwrap();
                pending().await
            }),
        )
        .await
        .unwrap();
    tokio::select! { _ = child.next() => panic!("work unexpectedly completed"), _ = started => {} }
    drop(parent);
    assert!(child_handle.cancellation().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), handle.wait_finished())
            .await
            .is_err()
    );
    drop(child);
    tokio::time::timeout(LIMIT, handle.wait_finished())
        .await
        .unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 1);
    assert_eq!(child_handle.state(), State::Stopped);
    assert!(Supervisor::child(1, &handle).is_err());
}

#[tokio::test]
async fn shutdown_success_means_finished_including_external_work_and_child_scopes() {
    let parent = scope(1);
    let handle = parent.handle();
    assert_eq!(handle.state(), State::Open);
    let child = Supervisor::child(1, &handle).unwrap();
    let child_handle = child.handle();
    let (release, released) = oneshot::channel();
    let (began, starting) = oneshot::channel();
    let tracked = parent.track(async move {
        began.send(()).unwrap();
        released.await.unwrap();
    });
    let work = tokio::spawn(tracked);
    starting.await.unwrap();
    let closing = tokio::spawn(parent.shutdown(Some(LIMIT)));
    child_handle.cancellation().cancelled().await;
    assert_eq!(handle.state(), State::Stopping);
    assert!(!closing.is_finished());
    child.shutdown(Some(LIMIT)).await.unwrap();
    assert!(!closing.is_finished());
    release.send(()).unwrap();
    work.await.unwrap();
    closing.await.unwrap().unwrap();
    assert_eq!(handle.state(), State::Stopped);
    tokio::time::timeout(Duration::ZERO, handle.wait_finished())
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_timeout_reclaims_owned_tasks_but_reports_retained_external_work() {
    let scope = scope(1);
    let handle = scope.handle();
    let count = Arc::new(AtomicUsize::new(0));
    let guard = Released(Arc::clone(&count));
    handle
        .launch(
            "owned",
            Box::pin(async move {
                let _guard = guard;
                pending().await
            }) as Task,
        )
        .await
        .unwrap();
    let external = scope.track(pending::<()>());
    assert!(matches!(
        scope.shutdown(Some(Duration::from_millis(10))).await,
        Err(Error::ShutdownTimeout)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(handle.used_capacity(), 0);
    assert_eq!(handle.state(), State::Stopping);
    drop(external);
    handle.wait_finished().await;
    assert_eq!(handle.state(), State::Stopped);
}

#[tokio::test]
async fn rejected_submissions_report_the_reason_and_release_capacity() {
    use tok_lifecycle::task::SubmitError;
    let scope = scope(1);
    let handle = scope.handle();
    for name in [" ".to_owned(), "中".repeat(86)] {
        let error = handle
            .launch(name, Box::pin(async { Ok(()) }))
            .await
            .unwrap_err();
        assert_eq!(error, SubmitError::InvalidName);
        assert_eq!(handle.used_capacity(), 0);
    }
    let reservation = handle.reserve().unwrap();
    handle.request_stop();
    assert_eq!(handle.reserve().err().unwrap(), SubmitError::Stopping);
    assert_eq!(
        reservation
            .launch("late", Box::pin(async { Ok(()) }))
            .await
            .unwrap_err(),
        SubmitError::Stopping
    );
    assert_eq!(handle.used_capacity(), 0);
    scope.shutdown(Some(LIMIT)).await.unwrap();
}

#[tokio::test]
async fn panicked_tasks_keep_their_names_and_do_not_consume_future_capacity() {
    let mut scope = scope(2);
    let handle = scope.handle();
    for name in ["first", "second"] {
        handle
            .launch(name, Box::pin(async { panic!("test panic") }))
            .await
            .unwrap();
    }
    let mut names = Vec::new();
    for _ in 0..2 {
        let completion = tokio::time::timeout(LIMIT, scope.next()).await.unwrap();
        assert!(matches!(completion.result, Err(Error::Join(error)) if error.is_panic()));
        names.push(completion.name);
    }
    names.sort();
    assert_eq!(names, ["first", "second"]);
    assert_eq!(handle.used_capacity(), 0);
    handle
        .launch("healthy", Box::pin(async { Ok(()) }))
        .await
        .unwrap();
    let completion = scope.next().await;
    assert_eq!(completion.name, "healthy");
    completion.result.unwrap();
    scope.shutdown(Some(LIMIT)).await.unwrap();
}

#[tokio::test]
async fn rejecting_child_creation_does_not_keep_the_stopping_parent_alive() {
    let parent = scope(1);
    let handle = parent.handle();
    handle.request_stop();
    assert!(matches!(
        Supervisor::child(1, &handle),
        Err(tok_lifecycle::task::CreateError::ParentStopping)
    ));
    parent.shutdown(Some(LIMIT)).await.unwrap();
    tokio::time::timeout(LIMIT, handle.wait_finished())
        .await
        .unwrap();
}
