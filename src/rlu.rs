use std::marker::{PhantomData, Unpin};
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

const RLU_MAX_LOG_SIZE: usize = 128;
const RLU_MAX_THREADS: usize = 32;
const RLU_MAX_FREE_NODES: usize = 100;
pub const PTR_ID_OBJ_COPY: usize = 0x12341234;

pub struct WsHdr<T: RluObj> {
    pub p_obj_actual: *mut T,
    pub run_counter: u64,
    pub thread_id: usize,
}

pub struct ObjList<T: RluObj> {
    num_of_objs: usize,
    cur_pos: usize,
    buffer: [Option<T>; RLU_MAX_LOG_SIZE],
}

impl<T> ObjList<T>
where
    T: RluObj,
{
    pub fn new() -> ObjList<T> {
        ObjList {
            num_of_objs: 0,
            cur_pos: 0,
            // Rust array initialization is so stupid. It's mind boggling there isn't a clean way to
            // initialize large arrays where the internal type is not copy.
            buffer: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None,
            ],
        }
    }
}

#[derive(Copy, Clone)]
pub struct WaitEntry {
    is_wait: bool,
    run_counter: u64,
}

pub struct RluThread<T: RluObj> {
    uniq_id: usize,
    is_writer: bool,
    wlog: ObjList<T>,
    run_counter: AtomicU64, //odd = active, even = inactive
    local_clock: AtomicU64,
    write_clock: AtomicU64,
    is_write_detected: bool,
    is_check_locks: bool,                    // no idea what u are
    q_threads: [WaitEntry; RLU_MAX_THREADS], //pre-allocated storage for checking thread status
    free_nodes: [*mut T; RLU_MAX_FREE_NODES],
    free_nodes_size: usize,
}

impl<T> RluThread<T>
where
    T: RluObj,
{
    pub fn new(id: usize) -> RluThread<T> {
        RluThread {
            uniq_id: id,
            is_writer: false,
            wlog: ObjList::new(),
            run_counter: AtomicU64::new(0),
            local_clock: AtomicU64::new(0),
            write_clock: AtomicU64::new(std::u64::MAX),
            is_write_detected: false,
            is_check_locks: false,
            q_threads: [WaitEntry {
                is_wait: false,
                run_counter: 0,
            }; RLU_MAX_THREADS],
            free_nodes: [
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            ],
            free_nodes_size: 0,
        }
    }
}

pub trait RluObj {
    fn get_p_obj_copy(&self) -> *mut Self;
    fn is_locked(&self) -> bool;
    fn is_copy(&self) -> bool;
    fn has_ws_hdr(&self) -> bool;
    fn get_p_original(&self) -> *mut Self;
    fn get_locking_thread_from_ws_obj(&self) -> usize;
    fn get_ws_run_counter(&self) -> u64;
    fn get_copy_with_ws_hdr(&self, run_counter: u64, thread_id: usize) -> Self;
    fn cas(&self, new_obj: *mut Self) -> bool;
    fn copy_back_to_original(&self);
    fn unlock_original(&self);
    fn unlock(&self);
}

pub struct RluObjHdr<T: RluObj> {
    pub p_obj_copy: AtomicPtr<T>,
    pub ws_hdr: Option<WsHdr<T>>, //only Some() if we are a copy, None at start
}

// Begin Internal functions

// End Internal Funcations

// Begin Rlu init / teardown functions

// This struct makes it possible to have multiple concurrent RLU data structures
pub struct GlobalRlu<T: RluObj> {
    threads: [Option<Box<RluThread<T>>>; RLU_MAX_THREADS],
    global_clock: AtomicU64,
    num_threads_created: AtomicUsize,
}

impl<T> GlobalRlu<T>
where
    T: RluObj,
{
    pub fn new() -> GlobalRlu<T> {
        GlobalRlu {
            threads: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None,
            ],
            global_clock: AtomicU64::new(0),
            num_threads_created: AtomicUsize::new(0),
        }
    }
    pub fn init_rlu() -> *mut GlobalRlu<T> {
        let mut boxed = Box::new(GlobalRlu::new());
        Box::into_raw(boxed)
    }
}

// End Rlu init/teardown functions

// Begin internal RLU functions
fn rlu_process_free<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                for i in 0..box_thread.free_nodes_size {
                    let box_node = Box::from_raw(box_thread.free_nodes[i]);
                    drop(box_node);
                    box_thread.free_nodes[i] = ptr::null_mut();
                }
                box_thread.free_nodes_size = 0;
            },
        );
    }
}

fn rlu_synchronize<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    //basing mostly off paper pseudocode for now
    unsafe {
        for i in 0..RLU_MAX_THREADS {
            if i == id {
                continue; //dont wait for myself
            }
            if (*rlu).threads[i].is_none() {
                continue; //dont wait for uninitialized/finished threads
            }
            (*rlu).threads[id].as_mut().map(|mut box_thread| {
                box_thread.q_threads[i].run_counter = (*rlu).threads[i]
                    .as_ref()
                    .unwrap()
                    .run_counter
                    .load(Ordering::SeqCst);
                if (box_thread.q_threads[i].run_counter & 0x1 == 0x1) {
                    //if run_counter odd, wait on it
                    box_thread.q_threads[i].is_wait = true;
                } else {
                    box_thread.q_threads[i].is_wait = false;
                }
            });
        }
        for i in 0..RLU_MAX_THREADS {
            loop {
                let done = (*rlu).threads[id]
                    .as_mut()
                    .map(|mut box_thread| {
                        if !box_thread.q_threads[i].is_wait {
                            return true; //already confirmed I dont need to wait
                        }
                        return (*rlu).threads[i]
                            .as_ref()
                            .map(|other_thread| {
                                if box_thread.q_threads[i].run_counter
                                    != other_thread.run_counter.load(Ordering::SeqCst)
                                {
                                    return true; //other thread has progressed
                                }
                                if box_thread.write_clock.load(Ordering::SeqCst)
                                    <= other_thread.local_clock.load(Ordering::SeqCst)
                                {
                                    return true; //other thread started after me so dont wait on it
                                }
                                false
                            })
                            .unwrap();
                    })
                    .unwrap();
                if done {
                    break;
                }
                std::sync::atomic::spin_loop_hint();
            }
        }
    }
}

fn rlu_commit_write_log<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                box_thread.write_clock.store(
                    (*rlu).global_clock.load(Ordering::SeqCst) + 1,
                    Ordering::SeqCst,
                );
                (*rlu).global_clock.fetch_add(1, Ordering::SeqCst);
            },
        );
    }
    rlu_synchronize(rlu, id); //spin loop while readers finish up
    rlu_writeback_write_log(rlu, id);
    // now set write clock back to inf
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                box_thread
                    .write_clock
                    .store(std::u64::MAX, Ordering::SeqCst);
            },
        );
    }
    rlu_swap_write_logs(rlu, id);
    rlu_process_free(rlu, id);
}

fn rlu_writeback_write_log<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                let cur_pos = box_thread.wlog.cur_pos;
                for i in (cur_pos - box_thread.wlog.num_of_objs)..cur_pos {
                    assert!(box_thread.wlog.buffer[i].is_some());
                    box_thread.wlog.buffer[i]
                        .as_ref()
                        .map(|obj_copy| obj_copy.copy_back_to_original());
                }
                box_thread.wlog.num_of_objs = 0; //these objects still exist but only until next sync()
            },
        );
    }
}

fn rlu_swap_write_logs<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                let cur_pos = box_thread.wlog.cur_pos;
                //Now, clear other half of write log bc this is second synchronize() since (see end
                //of 3.5 in paper)
                if cur_pos < (RLU_MAX_LOG_SIZE / 2) {
                    for i in (RLU_MAX_LOG_SIZE / 2)..RLU_MAX_LOG_SIZE {
                        if box_thread.wlog.buffer[i].is_some() {
                            box_thread.wlog.buffer[i] = None; //no readers can remain for these entries
                        }
                    }
                    box_thread.wlog.cur_pos = (RLU_MAX_LOG_SIZE / 2); //swaps write logs
                } else {
                    for i in 0..(RLU_MAX_LOG_SIZE / 2) {
                        if box_thread.wlog.buffer[i].is_some() {
                            box_thread.wlog.buffer[i] = None; //no readers can remain for these entries
                        }
                    }
                    box_thread.wlog.cur_pos = 0; //swaps write logs
                }
            },
        );
    }
}

fn rlu_unlock_objs<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                for i in
                    (box_thread.wlog.cur_pos - box_thread.wlog.num_of_objs)..box_thread.wlog.cur_pos
                {
                    assert!(box_thread.wlog.buffer[i].is_some());
                    box_thread.wlog.buffer[i]
                        .as_ref()
                        .map(|ws_copy| (*ws_copy).unlock_original());
                }
                box_thread.wlog.cur_pos -= box_thread.wlog.num_of_objs;
                box_thread.wlog.num_of_objs = 0;
            },
        )
    }
}
// End internal RLU functions

// Begin main externally exposed RLU functions

pub fn rlu_thread_init<T: RluObj>(rlu: *mut GlobalRlu<T>) -> usize {
    unsafe {
        let id = (*rlu).num_threads_created.fetch_add(1, Ordering::SeqCst);
        if id >= RLU_MAX_THREADS {
            panic!("invalid thread id");
        }
        //this is safe because no 2 threads will ever access the same index
        assert!((*rlu).threads[id].is_none());
        (*rlu).threads[id] = Some(Box::new(RluThread::new(id)));
        id
    }
}

pub fn rlu_reader_lock<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                assert!((box_thread.run_counter.load(Ordering::SeqCst) & 0x1) == 0);
                box_thread.run_counter.fetch_add(1, Ordering::SeqCst);
                box_thread.is_writer = false;
                box_thread
                    .local_clock
                    .store((*rlu).global_clock.load(Ordering::SeqCst), Ordering::SeqCst);
            },
        )
    }
}

pub fn rlu_reader_unlock<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                assert!((box_thread.run_counter.load(Ordering::SeqCst) & 0x1) != 0);
                box_thread.run_counter.fetch_add(1, Ordering::SeqCst);
                if box_thread.is_writer {
                    box_thread.is_writer = false;
                    rlu_commit_write_log(rlu, id);
                }
            },
        )
    }
}

pub fn rlu_dereference<T: RluObj>(
    rlu: *mut GlobalRlu<T>,
    id: usize,
    p_obj: *mut T, /* ptr to any object */
) -> *mut T {
    //check if object is unlocked:
    unsafe {
        if p_obj.is_null() {
            return p_obj;
        }
        let p_obj_copy = (*p_obj).get_p_obj_copy();

        if p_obj_copy.is_null() {
            //unlocked!
            return p_obj;
        }
        if p_obj_copy == mem::transmute(PTR_ID_OBJ_COPY) {
            // this is already a copy, it has already been referenced
            return p_obj;
        }

        let locking_thread = (*p_obj_copy).get_locking_thread_from_ws_obj();
        if locking_thread == id {
            //locked by us!
            return p_obj_copy;
        }
        let other_write_clock = (*rlu).threads[locking_thread].as_ref().map_or_else(
            || unreachable!(),
            |other_thread| other_thread.write_clock.load(Ordering::SeqCst),
        );
        let my_local_clock = (*rlu).threads[id].as_ref().map_or_else(
            || unreachable!(),
            |box_thread| box_thread.local_clock.load(Ordering::SeqCst),
        );
        if other_write_clock <= my_local_clock {
            p_obj_copy //steal!
        } else {
            p_obj //no stealing
        }
    }
}

pub fn rlu_try_lock<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize, p_p_obj: *mut *mut T) -> bool {
    unsafe {
        let mut p_obj = (*p_p_obj);

        assert!(!p_obj.is_null()); // cant lock null pointer!

        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                box_thread.is_writer = true;
            },
        );
        let mut p_obj_copy = (*p_obj).get_p_obj_copy();
        if p_obj_copy == mem::transmute(PTR_ID_OBJ_COPY) {
            //tried to lock a copy!
            //get original
            assert!((*p_obj).has_ws_hdr());
            p_obj = (*p_obj).get_p_original();
            p_obj_copy = (*p_obj).get_p_obj_copy();
        }

        if !p_obj_copy.is_null() {
            // object already locked!
            let th_id = (*p_obj_copy).get_locking_thread_from_ws_obj();
            if th_id == id {
                //check run counter to see if locked by current execution of this thread
                if (*p_obj_copy).get_ws_run_counter()
                    == (*rlu).threads[id]
                        .as_ref()
                        .map(|thread| thread.run_counter.load(Ordering::SeqCst))
                        .unwrap()
                {
                    // already locked by current execution of this thread
                    *p_p_obj = p_obj_copy;
                    return true;
                }
                //locked by other execution of this thread
                return false;
            }
            // locked by another thread
            return false;
        }
        //unlocked!
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                if !box_thread.is_write_detected {
                    box_thread.is_write_detected = true;
                    box_thread.is_check_locks = true;
                }
            },
        );
        let obj_copy = (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                box_thread.wlog.buffer[box_thread.wlog.cur_pos] = Some(
                    (*p_obj)
                        .get_copy_with_ws_hdr(box_thread.run_counter.load(Ordering::SeqCst), id),
                );
                box_thread.wlog.buffer[box_thread.wlog.cur_pos]
                    .as_mut()
                    .unwrap()
            },
        );
        // My design here differs slightly from the C implementation, in that it puts the entire
        // copy in the write log before trying to compare-and-swap the pointer in the original.

        if !(*p_obj).cas(obj_copy) {
            return false;
        }

        //now, update ws_hdr state
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                box_thread.wlog.cur_pos += 1;
                box_thread.wlog.num_of_objs += 1;
            },
        );

        *p_p_obj = obj_copy; //new

        return true;
    }
}

pub fn rlu_abort<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize) {
    unsafe {
        (*rlu).threads[id].as_mut().map_or_else(
            || unreachable!(),
            |mut box_thread| {
                let prev = box_thread.run_counter.fetch_add(1, Ordering::SeqCst);
                assert!((prev & 0x1) != 0);
                if box_thread.is_writer {
                    box_thread.is_writer = false;
                    rlu_unlock_objs(rlu, id);
                }
            },
        )
    }
}

pub unsafe fn rlu_free<T: RluObj>(rlu: *mut GlobalRlu<T>, id: usize, p_obj: *mut T) {
    assert!((*p_obj).is_copy()); //cant free node you havent locked!

    (*rlu).threads[id].as_mut().map_or_else(
        || unreachable!(),
        |mut box_thread| {
            assert!(box_thread.free_nodes_size < RLU_MAX_FREE_NODES);
            box_thread.free_nodes[box_thread.free_nodes_size] = (*p_obj).get_p_original();
            box_thread.free_nodes_size += 1;
        },
    );
}

pub fn rlu_assign_ptr<T: RluObj>(p_ptr: *mut *mut T, p_obj: *mut T) {
    unsafe {
        if p_obj.is_null() {
            (*p_ptr) = p_obj; //assign null
            return;
        }
        if (*p_obj).is_copy() {
            (*p_ptr) = (*p_obj).get_p_original();
        } else {
            //already original
            (*p_ptr) = p_obj;
        }
    }
}

// End main externally exposed RLU functions
