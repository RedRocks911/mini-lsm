// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use nom::lib::std::collections::binary_heap::PeekMut;
use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;
use serde_json::map::Iter;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap: BinaryHeap<_> = BinaryHeap::new();

        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(index, iter));
            }
        }
        let inner = heap.pop();
        Self {
            iters: heap,
            current: inner,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().map_or(false, |c| c.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        //let next = self.iters.pop();
        //self.current = next;
        //let mut next = self.iters.pop();
        /*println!(
            "next :key {:?}, value {:?} , current : key {:?}, value {:?}",
            next.as_ref().unwrap().1.key(),
            str::from_utf8(next.as_ref().unwrap().1.value()),
            self.key(),
            str::from_utf8(self.value())
        );*/
        /*if let Some(current) = self.current.as_mut() {
            let current_key = current.1.key().to_owned(); // 保存键的副本

            while let Some(inner_iter) = self.iters.peek_mut() {
                // 检查键是否相同
                if inner_iter.1.key() == current_key {
                    // 移除并比较
                    let inner = PeekMut::pop(inner_iter);
                    if inner.1.value() < current.1.value() {
                        *current = inner;
                    }
                    // 继续检查下一个元素
                } else {
                    // 键不同，退出循环
                    break;
                }
            }
        }
        //let mut iter = heap.iter();
        let mut iter = self.iters.iter();
        while let Some(item) = iter.next() {

        }
        Ok(())*/
        let current: &mut HeapWrapper<I> = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }
}
