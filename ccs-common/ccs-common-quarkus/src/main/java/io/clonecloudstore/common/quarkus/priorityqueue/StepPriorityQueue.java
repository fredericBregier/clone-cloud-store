/*
 * Copyright (c) 2022-2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed
 *  under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 *  OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.clonecloudstore.common.quarkus.priorityqueue;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.UnaryOperator;

/**
 * Same as PriorityBlockingQueue except it allows to prioritize at step by updating priority of all
 * elements at once (using stepResetPriority method and its associated reprioritize function)
 *
 * @param <E>
 */
class StepPriorityQueue<E> extends PriorityBlockingQueue<E> {
  /**
   * Creates a {@code PriorityBlockingQueue} with the specified initial
   * capacity that orders its elements according to the specified
   * comparator.
   *
   * @param initialCapacity the initial capacity for this priority queue
   * @param comparator      the comparator that will be used to order this
   *                        priority queue.  If {@code null}, the {@linkplain Comparable
   *                        natural ordering} of the elements will be used.
   * @throws IllegalArgumentException if {@code initialCapacity} is less
   *                                  than 1
   */
  public StepPriorityQueue(final int initialCapacity, final Comparator<? super E> comparator) {
    super(initialCapacity, comparator);
  }

  /**
   * Update all items and reset priority accordingly using comparator (O(n.log(n)) operation)
   */
  public synchronized void stepResetPriority(final UnaryOperator<E> reprioritize) {
    final var list = stream().map(reprioritize).toList();
    clear();
    addAll(list);
  }

  @Override
  public synchronized boolean add(final E e) {
    return super.add(e);
  }

  @Override
  public synchronized boolean offer(final E e) {
    return super.offer(e);
  }

  @Override
  public synchronized E poll() {
    return super.poll();
  }

  @Override
  public synchronized int size() {
    return super.size();
  }

  @Override
  public synchronized boolean remove(final Object o) {
    return super.remove(o);
  }

  @Override
  public synchronized boolean addAll(final Collection<? extends E> c) {
    return super.addAll(c);
  }

  @Override
  public synchronized boolean isEmpty() {
    return super.isEmpty();
  }

  @Override
  public synchronized void clear() {
    super.clear();
  }
}

