use rlu::{
    rlu_abort, rlu_assign_ptr, rlu_dereference, rlu_free, rlu_reader_lock, rlu_reader_unlock,
    rlu_thread_init, rlu_try_lock, GlobalRlu, RluObj, RluObjHdr, RluThread, WsHdr, PTR_ID_OBJ_COPY,
};
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread;
use std::time;

#[derive(Copy, Clone)]
pub struct RluIntWrapper {
    pub obj: *mut RluInt,
    pub rlu: *mut GlobalRlu<RluInt>,
} //I'm so sorry, Rust...lets me treat raw pointers as Sync and Send

unsafe impl Send for RluIntWrapper {}
unsafe impl Sync for RluIntWrapper {}

pub struct RluInt {
    hdr: RluObjHdr<RluInt>,
    data: u64,
}

unsafe impl Send for RluInt {}
unsafe impl Sync for RluInt {}

impl RluObj for RluInt {
    fn copy_back_to_original(&self) {
        assert!(self.has_ws_hdr());
        self.hdr.ws_hdr.as_ref().map(|hdr| unsafe {
            (*hdr.p_obj_actual)
                .hdr
                .p_obj_copy
                .store(ptr::null_mut(), Ordering::Relaxed);
            (*hdr.p_obj_actual).data = self.data;
        });
    }
    fn unlock(&self) {
        assert!(!self.has_ws_hdr());
        assert!(self.is_locked());
        self.hdr
            .p_obj_copy
            .store(ptr::null_mut(), Ordering::Relaxed);
    }
    fn unlock_original(&self) {
        unsafe {
            (*self.get_p_original()).unlock();
        }
    }
    fn get_p_obj_copy(&self) -> *mut Self {
        self.hdr.p_obj_copy.load(Ordering::Relaxed)
    }
    fn is_locked(&self) -> bool {
        !self.get_p_obj_copy().is_null()
    }
    fn is_copy(&self) -> bool {
        unsafe { self.get_p_obj_copy() == mem::transmute(PTR_ID_OBJ_COPY) }
    }
    fn has_ws_hdr(&self) -> bool {
        self.hdr.ws_hdr.is_some()
    }
    fn get_p_original(&self) -> *mut Self {
        assert!(self.has_ws_hdr());
        self.hdr
            .ws_hdr
            .as_ref()
            .map(|hdr| hdr.p_obj_actual)
            .unwrap()
    }
    fn get_locking_thread_from_ws_obj(&self) -> usize {
        assert!(self.has_ws_hdr());
        self.hdr.ws_hdr.as_ref().map(|hdr| hdr.thread_id).unwrap()
    }
    fn get_ws_run_counter(&self) -> u64 {
        assert!(self.has_ws_hdr());
        self.hdr.ws_hdr.as_ref().map(|hdr| hdr.run_counter).unwrap()
    }
    fn get_copy_with_ws_hdr(&self, run_counter: u64, thread_id: usize) -> Self {
        RluInt {
            hdr: RluObjHdr {
                p_obj_copy: AtomicPtr::new(unsafe { mem::transmute(PTR_ID_OBJ_COPY) }),
                ws_hdr: Some(WsHdr {
                    p_obj_actual: unsafe { mem::transmute(self) },
                    run_counter: run_counter,
                    thread_id: thread_id,
                }),
            },
            data: 0,
        }
    }
    fn cas(&self, new_obj: *mut Self) -> bool {
        self.hdr
            .p_obj_copy
            .compare_and_swap(ptr::null_mut(), new_obj, Ordering::Relaxed)
            .is_null()
    }
}

#[test]
fn rlu_single_thread() {
    let rlu_ptr: *mut GlobalRlu<RluInt> = GlobalRlu::init_rlu();
    assert!(!rlu_ptr.is_null());
    let obj = Box::into_raw(Box::new(RluInt {
        hdr: RluObjHdr {
            p_obj_copy: AtomicPtr::new(ptr::null_mut()),
            ws_hdr: None,
        },
        data: 2,
    }));
    let thread_id = rlu_thread_init(rlu_ptr);

    rlu_reader_lock(rlu_ptr, thread_id);
    let mut obj1 = rlu_dereference(rlu_ptr, thread_id, obj);
    assert!(obj1 == obj); //should return same pointer for unmodified object
    unsafe {
        assert!((*obj1).data == 2);
    }

    assert!(rlu_try_lock(rlu_ptr, thread_id, &mut obj1));
    assert!(obj1 != obj); //locking should return pointer to object in write log
    unsafe {
        (*obj1).data = 5;
    }
    assert!(unsafe { (*obj).data == 2 }); //havent written back yet!
    rlu_reader_unlock(rlu_ptr, thread_id);
    assert!(unsafe { (*obj).data == 5 }); //have written back!

    rlu_reader_lock(rlu_ptr, thread_id);
    let obj2 = rlu_dereference(rlu_ptr, thread_id, obj);
    assert!(obj2 == obj); //after reader_unlock() writeback should have occurred
    unsafe {
        assert!((*obj2).data == 5);
    }
    rlu_reader_unlock(rlu_ptr, thread_id);

    rlu_reader_lock(rlu_ptr, thread_id);
    let mut obj3 = rlu_dereference(rlu_ptr, thread_id, obj);
    assert!(rlu_try_lock(rlu_ptr, thread_id, &mut obj3));
    unsafe {
        (*obj3).data = 6;
    }
    rlu_reader_unlock(rlu_ptr, thread_id);

    rlu_reader_lock(rlu_ptr, thread_id);
    let obj4 = rlu_dereference(rlu_ptr, thread_id, obj);
    unsafe {
        assert!((*obj4).data == 6);
    }
    unsafe {
        rlu_free(rlu_ptr, thread_id, obj3);
    }
    rlu_reader_unlock(rlu_ptr, thread_id);
}

#[test]
fn rlu_two_thread() {
    let obj_wrap = RluIntWrapper {
        obj: Box::into_raw(Box::new(RluInt {
            hdr: RluObjHdr {
                p_obj_copy: AtomicPtr::new(ptr::null_mut()),
                ws_hdr: None,
            },
            data: 2,
        })),
        rlu: GlobalRlu::init_rlu(),
    };

    let reader = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        rlu_reader_lock(rlu, id);
        let obj1 = rlu_dereference(rlu, id, obj);
        let before = (*obj1).data;
        thread::sleep(time::Duration::from_millis(100));
        assert!((*obj1).data == before);
        rlu_reader_unlock(rlu, id);

        rlu_reader_lock(rlu, id);
        let obj1 = rlu_dereference(rlu, id, obj);
        assert!((*obj1).data == 5);
        rlu_reader_unlock(rlu, id);
    });

    let writer = thread::spawn(move || {
        unsafe {
            let obj = obj_wrap.obj;
            let rlu = obj_wrap.rlu;
            let id = rlu_thread_init(rlu);
            rlu_reader_lock(rlu, id);
            let mut obj2 = rlu_dereference(rlu, id, obj);
            assert!((*obj2).data == 2);
            assert!(rlu_try_lock(rlu, id, &mut obj2)); //shouldn't fail with no other writers!
            (*obj2).data = 5;
            assert!((*obj2).data == 5);
            rlu_reader_unlock(rlu, id);
        }
    });

    reader.join().unwrap();
    writer.join().unwrap();
}

#[test]
fn rlu_lock_contend() {
    let obj_wrap = RluIntWrapper {
        obj: Box::into_raw(Box::new(RluInt {
            hdr: RluObjHdr {
                p_obj_copy: AtomicPtr::new(ptr::null_mut()),
                ws_hdr: None,
            },
            data: 2,
        })),
        rlu: GlobalRlu::init_rlu(),
    };

    let reader = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        rlu_reader_lock(rlu, id);
        let obj1 = rlu_dereference(rlu, id, obj);
        let before = (*obj1).data;
        thread::sleep(time::Duration::from_millis(100));
        assert!((*obj1).data == before);
        rlu_reader_unlock(rlu, id);

        rlu_reader_lock(rlu, id);
        let obj1 = rlu_dereference(rlu, id, obj);
        assert!((*obj1).data == 5 || (*obj1).data == 6);
        rlu_reader_unlock(rlu, id);
    });

    let writer1 = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        loop {
            rlu_reader_lock(rlu, id);
            let mut obj2 = rlu_dereference(rlu, id, obj);
            if !rlu_try_lock(rlu, id, &mut obj2) {
                rlu_abort(rlu, id);
                continue;
            }
            (*obj2).data = 5;
            assert!((*obj2).data == 5);
            break;
        }
        rlu_reader_unlock(rlu, id);
    });

    let writer2 = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        loop {
            rlu_reader_lock(rlu, id);
            let mut obj2 = rlu_dereference(rlu, id, obj);
            if !rlu_try_lock(rlu, id, &mut obj2) {
                rlu_abort(rlu, id);
                continue;
            }
            (*obj2).data = 6;
            assert!((*obj2).data == 6);
            break;
        }
        rlu_reader_unlock(rlu, id);
    });

    reader.join().unwrap();
    writer1.join().unwrap();
    writer2.join().unwrap();
}

#[test]
fn rlu_thread_contend_more() {
    let obj_wrap = RluIntWrapper {
        obj: Box::into_raw(Box::new(RluInt {
            hdr: RluObjHdr {
                p_obj_copy: AtomicPtr::new(ptr::null_mut()),
                ws_hdr: None,
            },
            data: 2,
        })),
        rlu: GlobalRlu::init_rlu(),
    };

    let reader = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        rlu_reader_lock(rlu, id);
        let obj1 = rlu_dereference(rlu, id, obj);
        let before = (*obj1).data;
        thread::sleep(time::Duration::from_millis(100));
        assert!((*obj1).data == before);
        rlu_reader_unlock(rlu, id);

        rlu_reader_lock(rlu, id);
        let obj1 = rlu_dereference(rlu, id, obj);
        assert!((*obj1).data == 5 || (*obj1).data == 6 || (*obj1).data == 7 || (*obj1).data == 8);
        rlu_reader_unlock(rlu, id);
    });

    let writer1 = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        loop {
            rlu_reader_lock(rlu, id);
            let mut obj2 = rlu_dereference(rlu, id, obj);
            if !rlu_try_lock(rlu, id, &mut obj2) {
                rlu_abort(rlu, id);
                continue;
            }
            (*obj2).data = 5;
            assert!((*obj2).data == 5);
            break;
        }
        rlu_reader_unlock(rlu, id);
    });

    let writer2 = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        loop {
            rlu_reader_lock(rlu, id);
            let mut obj2 = rlu_dereference(rlu, id, obj);
            if !rlu_try_lock(rlu, id, &mut obj2) {
                rlu_abort(rlu, id);
                continue;
            }
            (*obj2).data = 6;
            assert!((*obj2).data == 6);
            break;
        }
        rlu_reader_unlock(rlu, id);
    });

    let writer3 = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        loop {
            rlu_reader_lock(rlu, id);
            let mut obj2 = rlu_dereference(rlu, id, obj);
            if !rlu_try_lock(rlu, id, &mut obj2) {
                rlu_abort(rlu, id);
                continue;
            }
            (*obj2).data = 7;
            assert!((*obj2).data == 7);
            break;
        }
        rlu_reader_unlock(rlu, id);
    });

    let writer4 = thread::spawn(move || unsafe {
        let obj = obj_wrap.obj;
        let rlu = obj_wrap.rlu;
        let id = rlu_thread_init(rlu);
        loop {
            rlu_reader_lock(rlu, id);
            let mut obj2 = rlu_dereference(rlu, id, obj);
            if !rlu_try_lock(rlu, id, &mut obj2) {
                rlu_abort(rlu, id);
                continue;
            }
            (*obj2).data = 8;
            assert!((*obj2).data == 8);
            break;
        }
        rlu_reader_unlock(rlu, id);
    });

    reader.join().unwrap();
    writer1.join().unwrap();
    writer2.join().unwrap();
    writer3.join().unwrap();
    writer4.join().unwrap();
}
