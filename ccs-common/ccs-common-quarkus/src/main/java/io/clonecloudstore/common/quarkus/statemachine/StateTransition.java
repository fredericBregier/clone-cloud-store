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

package io.clonecloudstore.common.quarkus.statemachine;

import java.util.EnumSet;
import java.util.Set;

/**
 * Transition object that joins one state and one set of acceptable following
 * states.
 *
 * @param <E> the enum class
 */
public class StateTransition<E extends Enum<E>> {
  private final E state;
  private final E defaultNextValidState;
  private final EnumSet<E> nextStates;

  /**
   * Final State with no next States
   */
  public StateTransition(final E stateFinal) {
    this(stateFinal, null, null);
  }

  /**
   * State with next States and one default Valid next step
   */
  public StateTransition(final E state, final E defaultNextValidState, final Set<E> nextStates) {
    if (state == null) {
      throw new IllegalStateException("Argument cannot be null");
    }
    this.state = state;
    this.defaultNextValidState = defaultNextValidState;
    if (defaultNextValidState == null) {
      this.nextStates = EnumSet.noneOf(state.getClass());
    } else if (nextStates == null) {
      this.nextStates = EnumSet.of(defaultNextValidState);
    } else if (nextStates.contains(defaultNextValidState)) {
      this.nextStates = (EnumSet<E>) nextStates;
    } else {
      throw new IllegalStateException("Set must contain default next step");
    }
  }

  /**
   * State with only one next State
   */
  public StateTransition(final E state, final E defaultNextValidState) {
    this(state, defaultNextValidState, null);
  }

  /**
   * @return the state
   */
  public E state() {
    return state;
  }

  /**
   * @return the next valid state or null if final
   */
  public E getDefaultNextValidState() {
    return defaultNextValidState;
  }

  /**
   * @return the set of next states
   */
  public Set<E> nextStates() {
    return nextStates;
  }

  /**
   * @return the clone set of next states
   */
  public Set<E> cloneNextStates() {
    return nextStates.clone();
  }
}
