//! 本地分发的执行顺序、容量、错误及当前调用边界。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};
use tok_events::{Error, Event, local::Dispatcher};

struct Changed(i32);
impl Event for Changed {
    const NAME: &'static str = "test.changed.v1";
}

#[test]
fn listeners_run_in_registration_order_with_shared_mutable_state() {
    let mut dispatcher = Dispatcher::<Changed, Vec<i32>, ()>::new(3).unwrap();
    dispatcher
        .register(|state, event| {
            state.push(event.0);
            Ok(())
        })
        .unwrap();
    dispatcher
        .register(|state, _| {
            state.push(state[0] + 1);
            Ok(())
        })
        .unwrap();
    let mut state = vec![];
    assert_eq!(dispatcher.dispatch(&mut state, &Changed(7)), Ok(2));
    assert_eq!(state, [7, 8]);
}

#[test]
fn first_error_is_preserved_and_partial_updates_are_not_rolled_back_or_retried() {
    let original = Arc::new(String::from("业务失败"));
    let mut dispatcher = Dispatcher::<Changed, Vec<i32>, Arc<String>>::new(3).unwrap();
    dispatcher
        .register(|state, event| {
            state.push(event.0);
            Ok(())
        })
        .unwrap();
    let failure = Arc::clone(&original);
    dispatcher
        .register(move |_, _| Err(Arc::clone(&failure)))
        .unwrap();
    dispatcher
        .register(|_, _| panic!("首错后不得执行"))
        .unwrap();
    let mut state = vec![];
    let returned = dispatcher.dispatch(&mut state, &Changed(1)).unwrap_err();
    assert!(Arc::ptr_eq(&returned, &original));
    assert_eq!(state, [1]);
    // 调用方再次分发就是再次执行业务，分发器不去重。
    assert!(dispatcher.dispatch(&mut state, &Changed(1)).is_err());
    assert_eq!(state, [1, 1]);
}

#[test]
fn registration_is_bounded_and_rejection_keeps_existing_handlers() {
    assert!(matches!(
        Dispatcher::<Changed, i32, ()>::new(0),
        Err(Error::InvalidCapacity)
    ));
    let mut dispatcher = Dispatcher::<Changed, i32, ()>::new(1).unwrap();
    dispatcher
        .register(|state, _| {
            *state += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        dispatcher.register(|_, _| panic!("超限订阅者不得注册")),
        Err(Error::CapacityExceeded)
    );
    let mut state = 0;
    assert_eq!(dispatcher.dispatch(&mut state, &Changed(0)), Ok(1));
    assert_eq!(state, 1);
}

#[test]
fn no_subscribers_is_a_successful_noop() {
    let dispatcher = Dispatcher::<Changed, i32, ()>::new(1).unwrap();
    let mut state = 5;
    assert_eq!(dispatcher.dispatch(&mut state, &Changed(1)), Ok(0));
    assert_eq!(state, 5);
}

#[test]
fn immutable_dispatcher_is_shared_but_execution_stays_on_each_callers_thread() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut dispatcher = Dispatcher::<Changed, (thread::ThreadId, i32), ()>::new(1).unwrap();
    let seen = Arc::clone(&calls);
    dispatcher
        .register(move |state, event| {
            assert_eq!(thread::current().id(), state.0);
            state.1 += event.0;
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    thread::scope(|scope| {
        for _ in 0..4 {
            let dispatcher = &dispatcher;
            scope.spawn(move || {
                let mut state = (thread::current().id(), 0);
                dispatcher.dispatch(&mut state, &Changed(3)).unwrap();
                assert_eq!(state.1, 3);
            });
        }
    });
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

struct Invalid;
impl Event for Invalid {
    const NAME: &'static str = "test..invalid";
}
#[test]
fn invalid_event_name_is_rejected_before_registration() {
    assert!(matches!(
        Dispatcher::<Invalid, (), ()>::new(1),
        Err(Error::InvalidInput(_))
    ));
}
