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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.logging.Logger;

/**
 * This is the base class for the basic support of Finite State Machine.
 * One need to implement an Enum class to use with it.
 *
 * @param <E> the enum class
 */
public class StateMachine<E extends Enum<E>> {
  private static final Logger LOGGER = Logger.getLogger(StateMachine.class);
  private ConcurrentMap<E, StateTransition<E>> stateTransitionMap;
  private E currentState;

  /**
   * Initialize with an initial State and StateTransitions
   *
   * @param initialState initial State
   * @param collection   the collection of StateTransition
   */
  public StateMachine(final E initialState, final Collection<StateTransition<E>> collection) {
    if (initialState == null || collection == null) {
      throw new IllegalStateException("Arguments cannot be null");
    }
    stateTransitionMap = new ConcurrentHashMap<>();
    for (final var stateTransition : collection) {
      stateTransitionMap.put(stateTransition.state(), stateTransition);
    }
    currentState = initialState;
  }

  /**
   * Initialize with an initial State and StateTransitions
   *
   * @param initialState     initial State
   * @param stateTransitions the StateTransitions
   */
  @SafeVarargs
  public StateMachine(final E initialState, final StateTransition<E>... stateTransitions) {
    if (initialState == null || stateTransitions == null) {
      throw new IllegalStateException("Arguments cannot be null");
    }
    stateTransitionMap = new ConcurrentHashMap<>();
    for (final var stateTransition : stateTransitions) {
      stateTransitionMap.put(stateTransition.state(), stateTransition);
    }
    currentState = initialState;
  }

  /**
   * Initialize with an initialState but no StateTransition (Machine State is
   * empty)
   *
   * @param initialState initial State
   */
  public StateMachine(final E initialState) {
    if (initialState == null) {
      throw new IllegalStateException("Argument cannot be null");
    }
    stateTransitionMap = new ConcurrentHashMap<>();
    currentState = initialState;
  }

  /**
   * Add a new association from one state to a stateTransition of acceptable following
   * states (can replace an existing association)
   *
   * @param stateTransition the new association as reachable targets
   * @return the previous association if any
   */
  public final StateTransition<E> addTransition(final StateTransition<E> stateTransition) {
    if (stateTransition == null) {
      throw new IllegalStateException("Argument cannot be null");
    }
    return stateTransitionMap.put(stateTransition.state(), stateTransition);
  }

  /**
   * Remove an association from one state to any acceptable following states
   *
   * @param state the state to remove any acceptable following states
   * @return the previous association if any
   */
  public final StateTransition<E> removeTransition(final E state) {
    return stateTransitionMap.remove(state);
  }

  /**
   * Return the current application state.
   *
   * @return the current State
   */
  public final E getState() {
    return currentState;
  }

  /**
   * Sets the current application state.
   *
   * @param desiredState the desired state
   * @return the requested state, if it was reachable
   * @throws IllegalStateException if the state is not allowed
   */
  public final E setState(final E desiredState) throws IllegalStateException {
    if (!isReachable(desiredState)) {
      LOGGER.debugf("State %s not reachable from: %s", desiredState, currentState);
      throw new IllegalStateException(desiredState + " not allowed from " + currentState);
    }
    return setAsFinal(desiredState);
  }

  /**
   * Determine if the given state is reachable.
   *
   * @param desiredState the desired state
   * @return True if the desiredState is reachable from currentState
   */
  public boolean isReachable(final E desiredState) {
    final var set = stateTransitionMap.get(currentState);
    if (set != null) {
      return set.nextStates().contains(desiredState);
    }
    return false;
  }

  /**
   * Finalizes the new requested state
   *
   * @param desiredState the desired state
   * @return the requested state
   */
  private E setAsFinal(final E desiredState) {
    LOGGER.debugf("New State: %s from %s", desiredState, currentState);
    currentState = desiredState;
    return currentState;
  }

  /**
   * Sets the current application state if reachable, and if not reachable
   * keeps the current state;
   *
   * @param desiredState the desired state
   * @return the current state, possibly unchanged
   */
  public final E setDryState(final E desiredState) {
    if (isReachable(desiredState)) {
      return setAsFinal(desiredState);
    }
    return currentState;
  }

  /**
   * @return True if the current state is terminal
   */
  public boolean isTerminal() {
    return stateTransitionMap.get(currentState).getDefaultNextValidState() == null;
  }

  /**
   * @return the reachable States from the current one
   */
  public Set<E> getReachableState() {
    return stateTransitionMap.get(currentState).cloneNextStates();
  }

  /**
   * @return the reachable States from the current one
   */
  public E getDefaultNextReachableState() {
    return stateTransitionMap.get(currentState).getDefaultNextValidState();
  }

  /**
   * Release the Machine State, keeping the current state but no more the
   * allowed steps
   */
  public final void release() {
    stateTransitionMap.clear();
    stateTransitionMap = null;
  }
}
