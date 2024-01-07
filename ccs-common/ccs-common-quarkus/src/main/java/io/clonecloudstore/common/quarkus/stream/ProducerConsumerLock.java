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
package io.clonecloudstore.common.quarkus.stream;

import java.io.Closeable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Producer-Consumer lock implementation : (https://en.wikipedia.org/wiki/Producer%E2%80%93consumer_problem)
 * Inspired from https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/locks/Condition.html implementation
 * Implements {@link Closeable} - When closed, all locks are unlocked.
 */
public class ProducerConsumerLock implements Closeable {

  private final Lock lock = new ReentrantLock();
  private final Condition canWrite = lock.newCondition();
  private final Condition canRead = lock.newCondition();

  private int writeCapacity;
  private int readCapacity;
  private volatile boolean closed = false;

  public ProducerConsumerLock(final int bufferCapacity) {
    writeCapacity = bufferCapacity;
    readCapacity = 0;
  }

  /**
   * Waits until enough units are available for write, or lock closed.
   *
   * @return true if enough write units reserved. false if closed.
   */
  public boolean tryBeginProduce(final int units) throws InterruptedException {
    lock.lock();
    try {

      while (!closed && writeCapacity < units) {
        canWrite.await();
      }
      writeCapacity -= units;

      return !closed;

    } finally {
      lock.unlock();
    }
  }

  /**
   * Notifies consumer of available units to consume.
   */
  public void endProduce(final int units) {
    lock.lock();
    try {
      readCapacity += units;
      canRead.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Best effort to get available bytes
   */
  public int possibleAvailable() {
    lock.lock();
    try {
      if (closed) {
        return 0;
      }
      return readCapacity;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Waits until 1..units are available for read, or lock closed.
   *
   * @return The number of available units to read (1 .. units). 0 if closed.
   */
  public int tryBeginConsume(final int units) throws InterruptedException {
    lock.lock();
    try {

      while (!closed && readCapacity == 0) {
        canRead.await();
      }

      final var immediatelyAvailable = Math.min(units, readCapacity);
      readCapacity -= immediatelyAvailable;

      return immediatelyAvailable;

    } finally {
      lock.unlock();
    }
  }

  /**
   * Notifies writer of available units to write.
   */
  public void endConsume(final int units) {
    lock.lock();
    try {
      writeCapacity += units;
      canWrite.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Closes locks and notifies reader &amp; producer
   */
  public void close() {
    lock.lock();
    try {
      closed = true;
      canRead.signal();
      canWrite.signal();
    } finally {
      lock.unlock();
    }
  }
}
