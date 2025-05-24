//! Growable array.

use core::fmt::Debug;
use core::mem::{self, ManuallyDrop};
use core::sync::atomic::Ordering::*;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other child segments**. In
/// other words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// height 2 segment and arrives at its `0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```text
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example, in `SplitOrderedList` the destruction of elements are handled by the inner `List`.
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment<T>>,
}

const SEGMENT_LOGSIZE: usize = 10;

/// A fixed size array of atomic pointers to other `Segment<T>` or `T`.
///
/// Each segment is either an inner segment with pointers to other, children `Segment<T>` or an
/// element segment with pointers to `T`. This is determined by the height of this segment in the
/// main array, which one needs to track separately. For example, use the main array root's tag.
///
/// Since destructing segments requires its height information, it is not recommended to implement
/// [`Drop`]. Rather, implement and use the custom [`Segment::deallocate`] method that accounts for
/// the height of the segment.
union Segment<T> {
    children: ManuallyDrop<[Atomic<Segment<T>>; 1 << SEGMENT_LOGSIZE]>,
    elements: ManuallyDrop<[Atomic<T>; 1 << SEGMENT_LOGSIZE]>,
}

impl<T> Segment<T> {
    /// Create a new segment filled with null pointers. It is up to the callee to whether to use
    /// this as an intermediate or an element segment.
    fn new() -> Owned<Self> {
        Owned::new(
            // SAFETY: An array of null pointers can be interperted as either an intermediate
            // segment or an element segment.
            unsafe { mem::zeroed() },
        )
    }

    /// Deallocates a segment of `height`.
    ///
    /// # Safety
    ///
    /// - `self` must actually have height `height`.
    /// - There should be no other references to possible children segments.
    unsafe fn deallocate(mut self, height: usize) {
        if height == 1 {
            // SAFETY: This is an element segment, so we can safely drop the elements.
            unsafe { ManuallyDrop::drop(&mut self.elements) };
        } else {
            // SAFETY: This is an intermediate segment, so we can safely drop the children segments.
            let guard = unsafe { crossbeam_epoch::unprotected() };
            for child in unsafe { &self.children }.iter() {
                unsafe {
                    let child_seg = child.load(Relaxed, guard).into_owned();
                    child_seg.into_box().deallocate(height - 1);
                }
            }
            unsafe {
                ManuallyDrop::drop(&mut self.children);
            }
        }
    }
}

impl<T> Debug for Segment<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        let guard = unsafe { crossbeam_epoch::unprotected() };
        let root = self.root.load(Relaxed, guard);
        let height = root.tag() as usize;
        unsafe {
            root.into_owned().into_box().deallocate(height);
        }
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

fn get_idx_seg_vec(index: usize) -> Vec<usize> {
    let mut index_seg_vec = Vec::new();
    let mut index = index;
    while index > 0 {
        index_seg_vec.push(index & ((1 << SEGMENT_LOGSIZE) - 1));
        index >>= SEGMENT_LOGSIZE;
    }
    index_seg_vec
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get<'g>(&self, index: usize, guard: &'g Guard) -> &'g Atomic<T> {
        let mask = (1 << SEGMENT_LOGSIZE) - 1;
        let index_seg_vec = get_idx_seg_vec(index);
        let h_required = index_seg_vec.len();

        let mut root_seg = self.root.load(SeqCst, guard);
        while root_seg.tag() < h_required {
            // Allocate a new segment and set it as the root.
            let new_seg = Segment::<T>::new().with_tag(root_seg.tag() + 1);
            if self
                .root
                .compare_exchange(root_seg, new_seg, SeqCst, Relaxed, guard)
                .is_ok()
            {
                // updated root
                root_seg = self.root.load(SeqCst, guard);
            } else {
                root_seg = self.root.load(SeqCst, guard);
            }
        }

        let mut seg = root_seg;
        for (i, index_seg) in index_seg_vec.iter().rev().enumerate() {
            if i == h_required - 1 {
                // This is the last segment, so we return the element.
                let elements = unsafe { &seg.as_ref().unwrap().elements };
                let element = &elements[*index_seg];
                return element;
            }
            // This is an intermediate segment, so we traverse to the next segment.
            let children = unsafe { &seg.as_ref().unwrap().children };
            let child_seg = children[*index_seg].load(SeqCst, guard);
            if child_seg.is_null() {
                // Allocate a new segment and set it as the child.
                let new_child_seg = Segment::<T>::new().with_tag(seg.tag() - 1);
                if children[*index_seg]
                    .compare_exchange(child_seg, new_child_seg, SeqCst, Relaxed, guard)
                    .is_ok()
                {
                    seg = children[*index_seg].load(SeqCst, guard);
                } else {
                    // updated child
                    seg = children[*index_seg].load(SeqCst, guard);
                }
            } else {
                seg = child_seg;
            }
        }

        panic!(
            "GrowableArray::get: index {} is out of bounds for height {}",
            index, h_required
        );
    }
}
