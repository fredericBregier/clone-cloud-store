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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.UnaryOperator;

/**
 * Global logic is the following:<br>
 * <ul>
 *   <li>A Priority queue managed by a comparator function</li>
 *   <li>A list of runnable tasks (as much as maxRunnable) ready to run in a round robin
 *   fashion</li>
 *   <li>When poll is called, if not enough elements are in the runnable tasks, the queue is
 *   reprioritized
 *   through reprioritize function (O(n.log(n) at max, O(1) at min) and
 *   then missing elements are taken out of the priority queue to the runnable task list</li>
 *   <li>If the task must be resubmitted (whatever the logic), addContinue shall be called and
 *   the task
 *   will be add at the end of the runnable list</li>
 *   <li>If e task must be removed from this structure, the remove will use the findEquals
 *   function</li>
 * </ul>
 * Note that if elements were polled before while an element is polled, the runnable list could
 * be larger
 * than maxRunnable.
 *
 * @param <E>
 */
public class ManagedRunnable<E> {
  private final ConcurrentLinkedQueue<E> realRunnables;
  private final StepPriorityQueue<E> stepPriorityQueue;
  private final UnaryOperator<E> reprioritize;
  private final Comparator<? super E> findEquals;
  private final int maxRunnable;


  /**
   * @param maxRunnable  the maximum runnable items, while extra ones will be added to priority
   *                     Queue
   * @param comparator   to compare 2 items in priority queue (during addition)
   * @param findEquals   to find 2 equaled items (during removal or existence check)
   * @param reprioritize to reprioritize when necessary all items (should be self defined),
   *                     getting one
   *                     item as entry, returning the very same updated before comparator is engaged
   */
  public ManagedRunnable(final int maxRunnable, final Comparator<? super E> comparator,
                         final Comparator<? super E> findEquals, final UnaryOperator<E> reprioritize) {
    this.maxRunnable = maxRunnable;
    realRunnables = new ConcurrentLinkedQueue<>();
    stepPriorityQueue = new StepPriorityQueue<>(11, comparator);
    this.findEquals = findEquals;
    this.reprioritize = reprioritize;
  }

  /**
   * @param e element to add at tail
   * @return True if added
   */
  public synchronized boolean add(final E e) {
    return stepPriorityQueue.add(e);
  }

  /**
   * Retrieves the head of this queue, or returns null if this queue is empty.
   * During retrieval, if the managed runnable items is not full, resort existing candidates
   * first then
   * fill the runnable list in order to get the maximum allowed items.
   */
  public synchronized E poll() {
    E e = null;
    if (realRunnables.size() < maxRunnable) {
      stepPriorityQueue.stepResetPriority(reprioritize);
      do {
        e = stepPriorityQueue.poll();
        if (e != null) {
          realRunnables.add(e);
        }
      } while (e != null && realRunnables.size() < maxRunnable);
    }
    return realRunnables.poll();
  }

  /**
   * @return the current size of managed runnable items
   */
  public int size() {
    return realRunnables.size() + stepPriorityQueue.size();
  }

  /**
   * @param e item to continue to act on, as real runnable ones
   */
  public synchronized void addContinue(final E e) {
    if (e != null) {
      realRunnables.add(e);
    }
  }

  /**
   * @param o the element to remove (using findEquals function)
   * @return True if removed
   */
  public synchronized boolean remove(final E o) {
    var found = searchReal(o);
    if (found != null) {
      return realRunnables.remove(found);
    }
    found = searchQueue(o);
    if (found != null) {
      return stepPriorityQueue.remove(found);
    }
    return false;
  }

  private E searchReal(final E o) {
    for (final var e : realRunnables) {
      if (findEquals.compare(o, e) == 0) {
        return e;
      }
    }
    return null;
  }

  private E searchQueue(final E o) {
    for (final var e : stepPriorityQueue) {
      if (findEquals.compare(o, e) == 0) {
        return e;
      }
    }
    return null;
  }

  /**
   * @param o the element to find (using findEquals function)
   * @return True if found
   */
  public synchronized boolean contains(final E o) {
    final var found = searchReal(o);
    if (found != null) {
      return true;
    }
    return searchQueue(o) != null;
  }

  /**
   * @return true if all collection is added correctly
   */
  public boolean addAll(final Collection<? extends E> c) {
    return stepPriorityQueue.addAll(c);
  }

  /**
   * @return True if this is Empty
   */
  public boolean isEmpty() {
    return realRunnables.isEmpty() && stepPriorityQueue.isEmpty();
  }

  /**
   * @return True if the active runner set is full (maxRunnable)
   */
  public boolean isRunnerFull() {
    return realRunnables.size() >= maxRunnable;
  }

  int runnerSize() {
    return realRunnables.size();
  }

  /**
   * Clear all the managed Runnable items
   */
  public synchronized void clear() {
    realRunnables.clear();
    stepPriorityQueue.clear();
  }
}
