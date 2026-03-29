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

//! 按 key 的增量状态缓存：LRU + TTL（idle），供 [`super::incremental_aggregate`] 等使用。

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct Key(pub Arc<Vec<u8>>);

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

struct Node<T> {
    key: Key,
    data: Option<T>,
    generation: u64,
    updated: Instant,
    prev: Option<usize>,
    next: Option<usize>,
}

/// 基于数组槽位 + 双向链表（LRU）的 UpdatingCache，支持按代更新与 TTL 逐出。
pub struct UpdatingCache<T: Send + Sync> {
    map: HashMap<Key, usize>,
    nodes: Vec<Node<T>>,
    free_list: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    ttl: Duration,
}

struct TTLIter<'a, T: Send + Sync> {
    now: Instant,
    cache: &'a mut UpdatingCache<T>,
}

impl<T: Send + Sync> Iterator for TTLIter<'_, T> {
    type Item = (Arc<Vec<u8>>, T);

    fn next(&mut self) -> Option<Self::Item> {
        let head_idx = self.cache.head?;
        let node = &self.cache.nodes[head_idx];

        if self.now.saturating_duration_since(node.updated) < self.cache.ttl {
            return None;
        }

        let (k, v) = self.cache.pop_front()?;
        Some((k.0, v))
    }
}

impl<T: Send + Sync> UpdatingCache<T> {
    pub fn with_time_to_idle(ttl: Duration) -> Self {
        Self {
            map: HashMap::new(),
            nodes: Vec::new(),
            free_list: Vec::new(),
            head: None,
            tail: None,
            ttl,
        }
    }

    pub fn insert(&mut self, key: Arc<Vec<u8>>, now: Instant, generation: u64, value: T) {
        let key_obj = Key(key);

        if let Some(&idx) = self.map.get(&key_obj) {
            if self.nodes[idx].generation >= generation {
                return;
            }
            self.nodes[idx].data = Some(value);
            self.nodes[idx].generation = generation;
            self.nodes[idx].updated = now;
            self.move_to_tail(idx);
            return;
        }

        let idx = self.allocate_node(key_obj.clone(), value, generation, now);
        self.map.insert(key_obj, idx);
        self.push_back(idx);
    }

    pub fn time_out(&mut self, now: Instant) -> impl Iterator<Item = (Arc<Vec<u8>>, T)> + '_ {
        TTLIter { now, cache: self }
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&Key, &mut T)> {
        self.nodes.iter_mut().filter_map(|n| {
            if let Some(data) = &mut n.data {
                Some((&n.key, data))
            } else {
                None
            }
        })
    }

    pub fn modify_and_update<E, F: Fn(&mut T) -> Result<(), E>>(
        &mut self,
        key: &[u8],
        now: Instant,
        f: F,
    ) -> Option<Result<(), E>> {
        let &idx = self.map.get(key)?;
        let node = &mut self.nodes[idx];

        if let Err(e) = f(node.data.as_mut().unwrap()) {
            return Some(Err(e));
        }

        node.generation += 1;
        node.updated = now;
        self.move_to_tail(idx);

        Some(Ok(()))
    }

    pub fn modify<E, F: Fn(&mut T) -> Result<(), E>>(
        &mut self,
        key: &[u8],
        f: F,
    ) -> Option<Result<(), E>> {
        let &idx = self.map.get(key)?;
        let node = &mut self.nodes[idx];

        node.generation += 1;

        if let Err(e) = f(node.data.as_mut().unwrap()) {
            return Some(Err(e));
        }

        Some(Ok(()))
    }

    pub fn contains_key(&self, k: &[u8]) -> bool {
        self.map.contains_key(k)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        let &idx = self.map.get(key)?;
        self.nodes[idx].data.as_mut()
    }

    pub fn get_mut_generation(&mut self, key: &[u8]) -> Option<(&mut T, u64)> {
        let &idx = self.map.get(key)?;
        let node = &mut self.nodes[idx];
        Some((node.data.as_mut().unwrap(), node.generation))
    }

    pub fn get_mut_key_value(&mut self, key: &[u8]) -> Option<(Key, &mut T)> {
        let &idx = self.map.get(key)?;
        let node = &mut self.nodes[idx];
        Some((node.key.clone(), node.data.as_mut().unwrap()))
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<T> {
        let &idx = self.map.get(key)?;
        self.map.remove(key);
        self.remove_node(idx);

        let data = self.nodes[idx].data.take().unwrap();
        self.free_list.push(idx);

        Some(data)
    }

    fn pop_front(&mut self) -> Option<(Key, T)> {
        let head_idx = self.head?;
        self.remove_node(head_idx);

        let node = &mut self.nodes[head_idx];
        self.map.remove(&node.key);

        let key = node.key.clone();
        let data = node.data.take().unwrap();
        self.free_list.push(head_idx);

        Some((key, data))
    }

    fn allocate_node(&mut self, key: Key, data: T, generation: u64, updated: Instant) -> usize {
        let new_node = Node {
            key,
            data: Some(data),
            generation,
            updated,
            prev: None,
            next: None,
        };

        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = new_node;
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(new_node);
            idx
        }
    }

    fn push_back(&mut self, index: usize) {
        self.nodes[index].prev = self.tail;
        self.nodes[index].next = None;

        if let Some(tail_idx) = self.tail {
            self.nodes[tail_idx].next = Some(index);
        } else {
            self.head = Some(index);
        }
        self.tail = Some(index);
    }

    fn remove_node(&mut self, index: usize) {
        let prev = self.nodes[index].prev;
        let next = self.nodes[index].next;

        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            self.head = next;
        }

        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            self.tail = prev;
        }

        self.nodes[index].prev = None;
        self.nodes[index].next = None;
    }

    fn move_to_tail(&mut self, index: usize) {
        if self.tail == Some(index) {
            return;
        }
        self.remove_node(index);
        self.push_back(index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_modify() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_secs(60));

        let key = Arc::new(vec![1, 2, 3]);
        let now = Instant::now();
        cache.insert(key.clone(), now, 1, 42);

        assert!(
            cache
                .modify(key.as_ref(), |x| {
                    *x = 43;
                    Ok::<(), ()>(())
                })
                .unwrap()
                .is_ok()
        );

        assert_eq!(*cache.get_mut(key.as_ref()).unwrap(), 43);
    }

    #[test]
    fn test_timeout() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_millis(10));

        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);

        let start = Instant::now();
        cache.insert(key1.clone(), start, 1, "value1");
        cache.insert(key2.clone(), start + Duration::from_millis(5), 2, "value2");

        let check_time = start + Duration::from_millis(11);
        let timed_out: Vec<_> = cache.time_out(check_time).collect();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(&*timed_out[0].0, &*key1);

        assert!(cache.contains_key(key2.as_ref()));
        assert!(!cache.contains_key(key1.as_ref()));
    }

    #[test]
    fn test_update_keeps_alive() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_millis(10));

        let key = Arc::new(vec![1]);
        let start = Instant::now();
        cache.insert(key.clone(), start, 1, "value");

        let update_time = start + Duration::from_millis(5);
        cache
            .modify_and_update(key.as_ref(), update_time, |_| Ok::<(), ()>(()))
            .unwrap()
            .unwrap();

        let check_time = start + Duration::from_millis(11);
        let timed_out: Vec<_> = cache.time_out(check_time).collect();
        assert!(timed_out.is_empty());
        assert!(cache.contains_key(key.as_ref()));
    }

    #[test]
    fn test_lru_eviction_order_matches_insertion() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_secs(60));
        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        let key3 = Arc::new(vec![3]);
        let now = Instant::now();
        cache.insert(key1.clone(), now, 1, 1);
        cache.insert(key2.clone(), now, 2, 2);
        cache.insert(key3.clone(), now, 3, 3);

        let evicted: Vec<_> = cache.time_out(now + Duration::from_secs(61)).collect();
        assert_eq!(evicted.len(), 3);
        assert_eq!(evicted[0].0.as_ref(), &*key1);
        assert_eq!(evicted[1].0.as_ref(), &*key2);
        assert_eq!(evicted[2].0.as_ref(), &*key3);
    }

    #[test]
    fn test_remove_middle_key() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_secs(60));
        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        let key3 = Arc::new(vec![3]);
        let now = Instant::now();
        cache.insert(key1.clone(), now, 1, 1);
        cache.insert(key2.clone(), now, 2, 2);
        cache.insert(key3.clone(), now, 3, 3);

        assert_eq!(cache.remove(&[2]).unwrap(), 2);
        assert!(cache.contains_key(&[1]));
        assert!(!cache.contains_key(&[2]));
        assert!(cache.contains_key(&[3]));

        let evicted: Vec<_> = cache.time_out(now + Duration::from_secs(61)).collect();
        assert_eq!(evicted.len(), 2);
        assert_eq!(evicted[0].0.as_ref(), &*key1);
        assert_eq!(evicted[1].0.as_ref(), &*key3);
    }

    #[test]
    fn reorder_with_update() {
        let mut cache = UpdatingCache::<i32>::with_time_to_idle(Duration::from_secs(10));
        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        let now = Instant::now();

        cache.insert(key1.clone(), now, 1, 100);
        cache.insert(key2.clone(), now, 2, 200);

        cache
            .modify_and_update(&[1], now + Duration::from_secs(1), |v| {
                *v += 1;
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let _ = cache.modify_and_update(&[1], now + Duration::from_secs(2), |v| {
            *v += 1;
            Ok::<(), ()>(())
        });
    }

    #[test]
    fn test_ttl_eviction() {
        let ttl = Duration::from_millis(100);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let now = Instant::now();
        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        cache.insert(key1.clone(), now, 1, 10);
        cache.insert(key2.clone(), now, 2, 20);

        cache
            .modify_and_update(&[2], now + Duration::from_millis(50), |v| {
                *v += 1;
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let now2 = now + Duration::from_millis(150);
        let evicted: Vec<_> = cache.time_out(now2).collect();
        assert_eq!(evicted.len(), 2);
        assert_eq!(evicted[0].0.as_ref(), &[1]);
        assert_eq!(evicted[1].0.as_ref(), &[2]);
    }

    #[test]
    fn test_remove_key() {
        let ttl = Duration::from_millis(100);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let now = Instant::now();
        let key = Arc::new(vec![1]);
        cache.insert(key.clone(), now, 1, 42);
        let value = cache.remove(&[1]).unwrap();
        assert_eq!(value, 42);
        assert!(!cache.contains_key(&[1]));
        let evicted: Vec<_> = cache.time_out(now + Duration::from_millis(200)).collect();
        assert!(evicted.is_empty());
    }

    #[test]
    fn test_update_order() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key_a = Arc::new(vec![b'A']);
        let key_b = Arc::new(vec![b'B']);
        let key_c = Arc::new(vec![b'C']);
        cache.insert(key_a.clone(), base, 1, 1);
        cache.insert(key_b.clone(), base, 2, 2);
        cache.insert(key_c.clone(), base, 3, 3);

        let t_update = base + Duration::from_millis(500);
        cache
            .modify_and_update(b"B", t_update, |v| {
                *v += 10;
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let t_eviction = base + Duration::from_secs(2);
        let evicted: Vec<_> = cache.time_out(t_eviction).collect();
        assert_eq!(evicted.len(), 3);
        assert_eq!(evicted[0].0.as_ref(), b"A");
        assert_eq!(evicted[1].0.as_ref(), b"C");
        assert_eq!(evicted[2].0.as_ref(), b"B");
    }

    #[test]
    fn test_get_mut_key_value() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key = Arc::new(vec![1, 2, 3]);
        cache.insert(key.clone(), base, 1, 42);
        if let Some((k, v)) = cache.get_mut_key_value(&[1, 2, 3]) {
            *v += 1;
            assert_eq!(*v, 43);
            assert_eq!(k.0.as_ref(), &[1, 2, 3]);
        } else {
            panic!("Key not found");
        }
    }

    #[test]
    fn test_modify_error() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key = Arc::new(vec![1]);
        cache.insert(key.clone(), base, 1, 42);
        let res = cache.modify(&[1], |_v| Err("error"));
        assert!(res.unwrap().is_err());
    }

    #[test]
    fn test_drop_cleanup() {
        let ttl = Duration::from_secs(1);
        {
            let mut cache = UpdatingCache::with_time_to_idle(ttl);
            let base = Instant::now();
            for i in 0..10 {
                cache.insert(Arc::new(vec![i as u8]), base, i as u64, i);
            }
        }
    }

    #[test]
    fn test_generational_replacement() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key = Arc::new(vec![1]);

        cache.insert(key.clone(), base, 1, "first");
        assert_eq!(cache.get_mut(&[1]), Some(&mut "first"));

        cache.insert(key.clone(), base, 2, "second");
        assert_eq!(cache.get_mut(&[1]), Some(&mut "second"));

        cache.insert(key.clone(), base, 1, "third");
        assert_eq!(cache.get_mut(&[1]), Some(&mut "second"));
    }
}
