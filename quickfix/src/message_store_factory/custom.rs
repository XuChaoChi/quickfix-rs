use std::{ffi, marker::PhantomData, mem::ManuallyDrop, panic::catch_unwind};

use quickfix_ffi::{
    FixCustomMessageStoreFactory_new, FixMessageStoreCallbacks_t, FixMessageStoreFactory_delete,
    FixMessageStoreFactory_t, FixSessionID_t,
};

use crate::{utils::from_ffi_str, QuickFixError, SessionId};

use super::FfiMessageStoreFactory;

/// Trait for per-session FIX message stores.
///
/// Implementors must be `Send + 'static` since each store is heap-allocated
/// and owned by the C++ QuickFIX engine for the duration of the session.
#[allow(unused_variables)]
pub trait MessageStoreCallback: Send + 'static {
    /// Store a message at the given sequence number. Returns true on success.
    fn set(&mut self, seq_num: u64, msg: &str) -> bool;

    /// Retrieve messages in the inclusive range [begin_seq_num, end_seq_num].
    fn get(&self, begin_seq_num: u64, end_seq_num: u64, msgs: &mut Vec<String>);

    /// Get the next expected sender sequence number.
    fn next_sender_seq_num(&self) -> u64;

    /// Get the next expected target sequence number.
    fn next_target_seq_num(&self) -> u64;

    /// Set the next expected sender sequence number.
    fn set_next_sender_seq_num(&mut self, value: u64);

    /// Set the next expected target sequence number.
    fn set_next_target_seq_num(&mut self, value: u64);

    /// Increment the next expected sender sequence number.
    fn incr_next_sender_seq_num(&mut self);

    /// Increment the next expected target sequence number.
    fn incr_next_target_seq_num(&mut self);

    /// Return the store creation time as Unix timestamp (seconds since epoch).
    fn creation_time(&self) -> i64;

    /// Reset the store. `now` is Unix timestamp in seconds (UTC).
    fn reset(&mut self, now: i64);

    /// Refresh store state from backing storage.
    fn refresh(&mut self);
}

/// Factory trait for creating per-session message stores.
///
/// Must be `Send + Sync + 'static` because the factory pointer is shared
/// across session threads.
pub trait MessageStoreFactoryCallback: Send + Sync + 'static {
    /// The concrete store type this factory produces.
    type Store: MessageStoreCallback;

    /// Create a new store for the given session.
    /// `now` is the creation timestamp as Unix seconds (UTC).
    fn create(&self, session: &SessionId, now: i64) -> Self::Store;
}

/// Rust-implemented message store factory.
///
/// Wraps a user-supplied [`MessageStoreFactoryCallback`] and bridges it to
/// the C++ `MessageStoreFactory` interface required by QuickFIX.
///
/// # Example
///
/// ```rust,ignore
/// use quickfix::{CustomMessageStoreFactory, MessageStoreCallback, MessageStoreFactoryCallback, SessionId};
/// use std::collections::HashMap;
///
/// struct MyStore { msgs: HashMap<u64, String>, sender: u64, target: u64, created: i64 }
///
/// impl MessageStoreCallback for MyStore {
///     fn set(&mut self, seq_num: u64, msg: &str) -> bool {
///         self.msgs.insert(seq_num, msg.to_owned()); true
///     }
///     fn get(&self, begin: u64, end: u64, out: &mut Vec<String>) {
///         for seq in begin..=end {
///             if let Some(m) = self.msgs.get(&seq) { out.push(m.clone()); }
///         }
///     }
///     fn next_sender_seq_num(&self) -> u64 { self.sender }
///     fn next_target_seq_num(&self) -> u64 { self.target }
///     fn set_next_sender_seq_num(&mut self, v: u64) { self.sender = v; }
///     fn set_next_target_seq_num(&mut self, v: u64) { self.target = v; }
///     fn incr_next_sender_seq_num(&mut self) { self.sender += 1; }
///     fn incr_next_target_seq_num(&mut self) { self.target += 1; }
///     fn creation_time(&self) -> i64 { self.created }
///     fn reset(&mut self, now: i64) { self.msgs.clear(); self.sender = 1; self.target = 1; self.created = now; }
///     fn refresh(&mut self) {}
/// }
///
/// struct MyFactory;
/// impl MessageStoreFactoryCallback for MyFactory {
///     type Store = MyStore;
///     fn create(&self, _session: &SessionId, now: i64) -> MyStore {
///         MyStore { msgs: HashMap::new(), sender: 1, target: 1, created: now }
///     }
/// }
///
/// let factory = MyFactory;
/// let store_factory = CustomMessageStoreFactory::try_new(&factory)?;
/// ```
pub struct CustomMessageStoreFactory<'a, F: MessageStoreFactoryCallback>(
    pub(crate) FixMessageStoreFactory_t,
    PhantomData<&'a F>,
);

impl<'a, F: MessageStoreFactoryCallback + 'static> CustomMessageStoreFactory<'a, F> {
    /// Create a new custom store factory from a Rust factory implementation.
    pub fn try_new(factory: &'a F) -> Result<Self, QuickFixError> {
        match unsafe {
            FixCustomMessageStoreFactory_new(
                factory as *const F as *const ffi::c_void,
                &Self::CALLBACKS,
            )
        } {
            Some(ptr) => Ok(Self(ptr, PhantomData)),
            None => Err(QuickFixError::from_last_error()),
        }
    }

    const CALLBACKS: FixMessageStoreCallbacks_t = FixMessageStoreCallbacks_t {
        onCreate: Self::on_create,
        onDestroy: Self::on_destroy,
        set: Self::store_set,
        get: Self::store_get,
        getNextSenderSeqNum: Self::get_next_sender_seq_num,
        getNextTargetSeqNum: Self::get_next_target_seq_num,
        setNextSenderSeqNum: Self::set_next_sender_seq_num,
        setNextTargetSeqNum: Self::set_next_target_seq_num,
        incrNextSenderSeqNum: Self::incr_next_sender_seq_num,
        incrNextTargetSeqNum: Self::incr_next_target_seq_num,
        getCreationTime: Self::get_creation_time,
        reset: Self::store_reset,
        refresh: Self::store_refresh,
    };

    extern "C" fn on_create(
        factory_data: *const ffi::c_void,
        session_ptr: Option<FixSessionID_t>,
        now: i64,
    ) -> *mut ffi::c_void {
        let result = catch_unwind(|| {
            let factory = unsafe { &*(factory_data as *const F) };
            let session_id = session_ptr.map(|ptr| ManuallyDrop::new(SessionId(ptr)));
            match session_id.as_deref() {
                Some(session_id) => {
                    let store = factory.create(session_id, now);
                    Box::into_raw(Box::new(store)) as *mut ffi::c_void
                }
                None => std::ptr::null_mut(),
            }
        });
        result.unwrap_or(std::ptr::null_mut())
    }

    extern "C" fn on_destroy(_factory_data: *const ffi::c_void, store_data: *mut ffi::c_void) {
        if !store_data.is_null() {
            let _ = catch_unwind(|| unsafe {
                drop(Box::from_raw(store_data as *mut F::Store));
            });
        }
    }

    extern "C" fn store_set(
        store_data: *mut ffi::c_void,
        seq_num: u64,
        msg: *const ffi::c_char,
    ) -> i8 {
        catch_unwind(|| {
            let store = unsafe { &mut *(store_data as *mut F::Store) };
            let msg = unsafe { from_ffi_str(msg) };
            store.set(seq_num, msg) as i8
        })
        .unwrap_or(0)
    }

    extern "C" fn store_get(
        store_data: *mut ffi::c_void,
        begin: u64,
        end: u64,
        push_ctx: *mut ffi::c_void,
        push_fn: extern "C" fn(*mut ffi::c_void, *const ffi::c_char),
    ) {
        let _ = catch_unwind(|| {
            let store = unsafe { &*(store_data as *const F::Store) };
            let mut msgs: Vec<String> = Vec::new();
            store.get(begin, end, &mut msgs);
            for msg in msgs {
                match std::ffi::CString::new(msg) {
                    Ok(c_str) => push_fn(push_ctx, c_str.as_ptr()),
                    Err(_) => {}
                }
            }
        });
    }

    extern "C" fn get_next_sender_seq_num(store_data: *mut ffi::c_void) -> u64 {
        catch_unwind(|| {
            let store = unsafe { &*(store_data as *const F::Store) };
            store.next_sender_seq_num()
        })
        .unwrap_or(1)
    }

    extern "C" fn get_next_target_seq_num(store_data: *mut ffi::c_void) -> u64 {
        catch_unwind(|| {
            let store = unsafe { &*(store_data as *const F::Store) };
            store.next_target_seq_num()
        })
        .unwrap_or(1)
    }

    extern "C" fn set_next_sender_seq_num(store_data: *mut ffi::c_void, value: u64) {
        let _ = catch_unwind(|| {
            let store = unsafe { &mut *(store_data as *mut F::Store) };
            store.set_next_sender_seq_num(value);
        });
    }

    extern "C" fn set_next_target_seq_num(store_data: *mut ffi::c_void, value: u64) {
        let _ = catch_unwind(|| {
            let store = unsafe { &mut *(store_data as *mut F::Store) };
            store.set_next_target_seq_num(value);
        });
    }

    extern "C" fn incr_next_sender_seq_num(store_data: *mut ffi::c_void) {
        let _ = catch_unwind(|| {
            let store = unsafe { &mut *(store_data as *mut F::Store) };
            store.incr_next_sender_seq_num();
        });
    }

    extern "C" fn incr_next_target_seq_num(store_data: *mut ffi::c_void) {
        let _ = catch_unwind(|| {
            let store = unsafe { &mut *(store_data as *mut F::Store) };
            store.incr_next_target_seq_num();
        });
    }

    extern "C" fn get_creation_time(store_data: *mut ffi::c_void) -> i64 {
        catch_unwind(|| {
            let store = unsafe { &*(store_data as *const F::Store) };
            store.creation_time()
        })
        .unwrap_or(0)
    }

    extern "C" fn store_reset(store_data: *mut ffi::c_void, now: i64) {
        let _ = catch_unwind(|| {
            let store = unsafe { &mut *(store_data as *mut F::Store) };
            store.reset(now);
        });
    }

    extern "C" fn store_refresh(store_data: *mut ffi::c_void) {
        let _ = catch_unwind(|| {
            let store = unsafe { &mut *(store_data as *mut F::Store) };
            store.refresh();
        });
    }
}

impl<F: MessageStoreFactoryCallback> FfiMessageStoreFactory for CustomMessageStoreFactory<'_, F> {
    fn as_ffi_ptr(&self) -> FixMessageStoreFactory_t {
        self.0
    }
}

impl<F: MessageStoreFactoryCallback> std::fmt::Debug for CustomMessageStoreFactory<'_, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CustomMessageStoreFactory").finish()
    }
}

impl<F: MessageStoreFactoryCallback> Drop for CustomMessageStoreFactory<'_, F> {
    fn drop(&mut self) {
        unsafe { FixMessageStoreFactory_delete(self.0) }
    }
}
