use crate::concurrent_set::ConcurrentSet;
use crate::rlu::{
    rlu_abort, rlu_assign_ptr, rlu_dereference, rlu_free, rlu_reader_lock, rlu_reader_unlock,
    rlu_thread_init, rlu_try_lock, GlobalRlu, RluObj, RluObjHdr, WsHdr, PTR_ID_OBJ_COPY,
};
use std::fmt::Debug;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

pub struct Node<T: 'static + Clone> {
    hdr: RluObjHdr<Node<T>>,
    next: NodePtr<T>,
    data: T,
}
type NodePtr<T> = *mut Node<T>;

impl<T: 'static> RluObj for Node<T>
where
    T: Clone,
{
    fn copy_back_to_original(&self) {
        assert!(self.has_ws_hdr());
        self.hdr.ws_hdr.as_ref().map(|hdr| unsafe {
            (*hdr.p_obj_actual).next = self.next;
            (*hdr.p_obj_actual).data = self.data.clone();
            (*hdr.p_obj_actual)
                .hdr
                .p_obj_copy
                .store(ptr::null_mut(), Ordering::SeqCst);
        });
    }
    fn unlock(&self) {
        assert!(!self.has_ws_hdr());
        assert!(self.is_locked());
        self.hdr.p_obj_copy.store(ptr::null_mut(), Ordering::SeqCst);
    }
    fn unlock_original(&self) {
        unsafe {
            (*self.get_p_original()).unlock();
        }
    }
    fn get_p_obj_copy(&self) -> *mut Self {
        self.hdr.p_obj_copy.load(Ordering::SeqCst)
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
        Node {
            hdr: RluObjHdr {
                p_obj_copy: AtomicPtr::new(unsafe { mem::transmute(PTR_ID_OBJ_COPY) }),
                ws_hdr: Some(WsHdr {
                    p_obj_actual: unsafe { mem::transmute(self) },
                    run_counter: run_counter,
                    thread_id: thread_id,
                }),
            },
            next: self.next,
            data: self.data.clone(),
        }
    }
    fn cas(&self, new_obj: *mut Self) -> bool {
        self.hdr
            .p_obj_copy
            .compare_and_swap(ptr::null_mut(), new_obj, Ordering::SeqCst)
            .is_null()
    }
}

pub struct RluSet<T: 'static + Clone> {
    head: NodePtr<T>,
    rlu_ptr: *mut GlobalRlu<Node<T>>,
    thread_id: usize,
}

unsafe impl<T: Clone> Send for RluSet<T> {}
unsafe impl<T: Clone> Sync for RluSet<T> {}

pub fn rlu_new_node<T: Clone>(value: T) -> *mut Node<T> {
    let node = Box::new(Node {
        hdr: RluObjHdr {
            p_obj_copy: AtomicPtr::new(ptr::null_mut()),
            ws_hdr: None,
        },
        next: ptr::null_mut(),
        data: value,
    });

    let tmp = Box::into_raw(node);
    tmp
}

impl<T> RluSet<T>
where
    T: PartialEq + PartialOrd + Copy + Clone + Debug,
{
    pub fn new() -> RluSet<T> {
        let rlu_ptr: *mut GlobalRlu<Node<T>> = GlobalRlu::init_rlu();
        let thread_id = rlu_thread_init(rlu_ptr);
        RluSet {
            rlu_ptr: rlu_ptr,
            head: Box::into_raw(Box::new(Node {
                hdr: RluObjHdr {
                    p_obj_copy: AtomicPtr::new(ptr::null_mut()),
                    ws_hdr: None,
                },
                next: ptr::null_mut(),
                data: unsafe { mem::MaybeUninit::zeroed().assume_init() }, // Okay bc this value will never be accessed
            })),
            thread_id: thread_id,
        }
    }

    // This function does not use RLU when traversing, is just a simple function
    // for single threaded debugging
    pub fn to_string(&self) -> String {
        let mut ret = String::from("{");
        unsafe {
            let mut node_ptr = (*self.head).next;
            loop {
                if node_ptr.is_null() {
                    break;
                } else {
                    ret.push_str(&format!("{:?}, ", (*node_ptr).data));
                    node_ptr = (*node_ptr).next;
                }
            }
        }
        ret.push('}');
        ret
    }
}

impl<T> ConcurrentSet<T> for RluSet<T>
where
    T: PartialEq + PartialOrd + Copy + Clone + Debug + Unpin,
{
    fn contains(&self, value: T) -> bool {
        let mut ret = false;
        rlu_reader_lock(self.rlu_ptr, self.thread_id);
        //println!("Contains {:?}?", value);
        let mut node_ptr = rlu_dereference(self.rlu_ptr, self.thread_id, self.head);
        let mut first = true;
        loop {
            if node_ptr.is_null() {
                break;
            } else if first {
                first = false;
                node_ptr =
                    rlu_dereference(self.rlu_ptr, self.thread_id, unsafe { (*node_ptr).next });
                continue;
            } else {
                let v = unsafe { (*node_ptr).data };
                if v > value {
                    break;
                }
                if v == value {
                    ret = true;
                    break;
                }
                node_ptr =
                    rlu_dereference(self.rlu_ptr, self.thread_id, unsafe { (*node_ptr).next });
            }
        }
        rlu_reader_unlock(self.rlu_ptr, self.thread_id);
        ret
    }

    fn len(&self) -> usize {
        let mut len = 0;
        rlu_reader_lock(self.rlu_ptr, self.thread_id);
        let mut node_ptr = rlu_dereference(self.rlu_ptr, self.thread_id, self.head);
        let mut first = true;
        loop {
            if node_ptr.is_null() {
                break;
            } else {
                if !first {
                    len += 1;
                } else {
                    first = false;
                }
                node_ptr =
                    rlu_dereference(self.rlu_ptr, self.thread_id, unsafe { (*node_ptr).next });
            }
        }
        rlu_reader_unlock(self.rlu_ptr, self.thread_id);
        len
    }

    fn insert(&self, value: T) -> bool {
        loop {
            rlu_reader_lock(self.rlu_ptr, self.thread_id);
            let mut p_prev = rlu_dereference(self.rlu_ptr, self.thread_id, self.head);
            let mut p_next =
                rlu_dereference(self.rlu_ptr, self.thread_id, unsafe { (*p_prev).next });
            let mut exact_match = false;
            loop {
                if p_next.is_null() {
                    break;
                }
                let v = unsafe { (*p_next).data };
                if v >= value {
                    if v == value {
                        exact_match = true;
                    }
                    break;
                }
                p_prev = p_next;
                p_next = rlu_dereference(self.rlu_ptr, self.thread_id, unsafe { (*p_next).next });
            }
            if exact_match {
                break; //dont insert if already in list
            }
            if !rlu_try_lock(self.rlu_ptr, self.thread_id, &mut p_prev) {
                rlu_abort(self.rlu_ptr, self.thread_id);
                continue; //retry
            }

            if !p_next.is_null() {
                if !rlu_try_lock(self.rlu_ptr, self.thread_id, &mut p_next) {
                    //maybe can remove this? see gradescope
                    rlu_abort(self.rlu_ptr, self.thread_id);
                    continue; //retry
                }
            }

            let p_new_node = rlu_new_node(value);
            // make the new node point to the current head of the list
            rlu_assign_ptr(unsafe { &mut (*p_new_node).next }, p_next);
            rlu_assign_ptr(unsafe { &mut (*p_prev).next }, p_new_node);
            break;
        }
        rlu_reader_unlock(self.rlu_ptr, self.thread_id);
        true
    }

    fn delete(&self, value: T) -> bool {
        let mut ret = false;
        loop {
            let mut continue_outer = false;
            //outer loop for restarting on failed lock
            rlu_reader_lock(self.rlu_ptr, self.thread_id);
            let mut p_prev = rlu_dereference(self.rlu_ptr, self.thread_id, self.head); //points to dummy head node
            let mut p_next =
                rlu_dereference(self.rlu_ptr, self.thread_id, unsafe { (*p_prev).next });
            loop {
                if p_next.is_null() {
                    break;
                } else {
                    let v = unsafe { (*p_next).data };
                    if v > value {
                        break;
                    }
                    if v == value {
                        if !rlu_try_lock(self.rlu_ptr, self.thread_id, &mut p_prev) {
                            rlu_abort(self.rlu_ptr, self.thread_id);
                            continue_outer = true;
                            break;
                        }
                        if !rlu_try_lock(self.rlu_ptr, self.thread_id, &mut p_next) {
                            rlu_abort(self.rlu_ptr, self.thread_id);
                            continue_outer = true;
                            break;
                        }
                        ret = true;
                        unsafe {
                            (*p_prev).next = (*p_next).next;
                        }
                        unsafe {
                            rlu_free(self.rlu_ptr, self.thread_id, p_next);
                        }
                        break;
                    }
                    p_prev = p_next;
                    p_next =
                        rlu_dereference(self.rlu_ptr, self.thread_id, unsafe { (*p_next).next });
                }
            }
            if continue_outer {
                continue;
            } else {
                rlu_reader_unlock(self.rlu_ptr, self.thread_id);
                break;
            }
        }
        ret
    }

    fn clone_ref(&self) -> Self {
        let thread_id = rlu_thread_init(self.rlu_ptr);
        RluSet {
            rlu_ptr: self.rlu_ptr,
            head: self.head,
            thread_id: thread_id,
        }
    }
}
