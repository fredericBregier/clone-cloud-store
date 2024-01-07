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

package io.clonecloudstore.test.stream;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class FakeIterator<E> implements Iterator<E>, Closeable {
  private final long size;
  private final Function<Long, E> createObject;
  private long current = 0;

  public FakeIterator(long size, Function<Long, E> createObject) {
    this.size = size;
    this.createObject = createObject;
  }

  @Override
  public boolean hasNext() {
    return current < size;
  }

  @Override
  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final var e = createObject.apply(current);
    current++;
    return e;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not compatible");
  }

  @Override
  public void close() {
    current = size;
  }

  public void reset() {
    current = 0;
  }
}
