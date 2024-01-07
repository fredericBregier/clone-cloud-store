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
import java.util.EnumSet;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class StateMachineTest {
  @Test
  void checkStateMachineVoidAsTerminalWithoutReachable() {
    final var stateMachine = ExampleEnumState.newStateMachine();
    assertEquals(ExampleEnumState.NONE, stateMachine.getState());
    assertTrue(stateMachine.isReachable(ExampleEnumState.START));
    assertEquals(ExampleEnumState.START, stateMachine.setState(ExampleEnumState.START));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.ERROR, stateMachine.setState(ExampleEnumState.ERROR));
    assertTrue(stateMachine.isReachable(ExampleEnumState.VOID));
    assertEquals(ExampleEnumState.VOID, stateMachine.setState(ExampleEnumState.VOID));
    assertFalse(stateMachine.isReachable(ExampleEnumState.END));
  }

  @Test
  void checkStateMachine() {
    final var stateMachine = ExampleEnumState.newStateMachine();
    assertEquals(ExampleEnumState.NONE, stateMachine.getState());
    assertFalse(stateMachine.isReachable(ExampleEnumState.END));
    assertFalse(stateMachine.isReachable(ExampleEnumState.VOID));
    assertEquals(ExampleEnumState.NONE, stateMachine.setDryState(ExampleEnumState.END));
    assertTrue(stateMachine.isReachable(ExampleEnumState.START));
    assertEquals(ExampleEnumState.START, stateMachine.setState(ExampleEnumState.START));
    assertTrue(stateMachine.isReachable(ExampleEnumState.RUNNING));
    assertTrue(stateMachine.isReachable(ExampleEnumState.PAUSE));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.setDryState(ExampleEnumState.RUNNING));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.getState());
    assertFalse(stateMachine.isTerminal());
    assertEquals(3, stateMachine.getReachableState().size());
    assertTrue(stateMachine.isReachable(ExampleEnumState.END));
    assertTrue(stateMachine.isReachable(ExampleEnumState.PAUSE));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.PAUSE, stateMachine.setState(ExampleEnumState.PAUSE));
    assertEquals(1, stateMachine.getReachableState().size());
    assertThrows(IllegalStateException.class, () -> stateMachine.setState(ExampleEnumState.END));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.setState(ExampleEnumState.RUNNING));
    assertEquals(ExampleEnumState.END, stateMachine.setState(ExampleEnumState.END));
    assertTrue(stateMachine.isTerminal());
    assertFalse(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.END, stateMachine.setDryState(ExampleEnumState.ERROR));
    assertThrows(IllegalStateException.class, () -> stateMachine.setState(ExampleEnumState.ERROR));
    stateMachine.release();
  }

  @Test
  void checkStateMachineBuildFromTransition() {
    final var stateMachine = new StateMachine<ExampleEnumState>(ExampleEnumState.NONE);
    for (final var transitionState : ExampleEnumState.ExampleTransitionState.values()) {
      assertNull(stateMachine.addTransition(transitionState.elt));
    }
    assertEquals(ExampleEnumState.NONE, stateMachine.getState());
    assertFalse(stateMachine.isReachable(ExampleEnumState.END));
    assertEquals(ExampleEnumState.NONE, stateMachine.setDryState(ExampleEnumState.END));
    assertTrue(stateMachine.isReachable(ExampleEnumState.START));
    assertEquals(ExampleEnumState.START, stateMachine.setState(ExampleEnumState.START));
    assertTrue(stateMachine.isReachable(ExampleEnumState.RUNNING));
    assertTrue(stateMachine.isReachable(ExampleEnumState.PAUSE));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.setDryState(ExampleEnumState.RUNNING));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.getState());
    assertFalse(stateMachine.isTerminal());
    assertTrue(stateMachine.isReachable(ExampleEnumState.END));
    assertTrue(stateMachine.isReachable(ExampleEnumState.PAUSE));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.PAUSE, stateMachine.setState(ExampleEnumState.PAUSE));
    assertThrows(IllegalStateException.class, () -> stateMachine.setState(ExampleEnumState.END));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.setState(ExampleEnumState.RUNNING));
    assertEquals(ExampleEnumState.END, stateMachine.setState(ExampleEnumState.END));
    assertTrue(stateMachine.isTerminal());
    assertFalse(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.END, stateMachine.setDryState(ExampleEnumState.ERROR));
    assertThrows(IllegalStateException.class, () -> stateMachine.setState(ExampleEnumState.ERROR));
    assertNotNull(stateMachine.removeTransition(ExampleEnumState.END));
    final var transition = new StateTransition<ExampleEnumState>(ExampleEnumState.END, ExampleEnumState.ERROR);
    assertNull(stateMachine.addTransition(transition));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.ERROR, stateMachine.getDefaultNextReachableState());
    assertEquals(ExampleEnumState.ERROR, stateMachine.setState(ExampleEnumState.ERROR));
    stateMachine.release();
  }

  @Test
  void checkStateMachineBuildFromArray() {
    final StateTransition<ExampleEnumState>[] stateMachines =
        ExampleEnumState.configuration.toArray(new StateTransition[0]);
    final var stateMachine = new StateMachine<ExampleEnumState>(ExampleEnumState.NONE, stateMachines);
    assertEquals(ExampleEnumState.NONE, stateMachine.getState());
    assertFalse(stateMachine.isReachable(ExampleEnumState.END));
    assertEquals(ExampleEnumState.NONE, stateMachine.setDryState(ExampleEnumState.END));
    assertTrue(stateMachine.isReachable(ExampleEnumState.START));
    assertEquals(ExampleEnumState.START, stateMachine.setState(ExampleEnumState.START));
    assertTrue(stateMachine.isReachable(ExampleEnumState.RUNNING));
    assertTrue(stateMachine.isReachable(ExampleEnumState.PAUSE));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.setDryState(ExampleEnumState.RUNNING));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.getState());
    assertFalse(stateMachine.isTerminal());
    assertTrue(stateMachine.isReachable(ExampleEnumState.END));
    assertTrue(stateMachine.isReachable(ExampleEnumState.PAUSE));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.PAUSE, stateMachine.setState(ExampleEnumState.PAUSE));
    assertThrows(IllegalStateException.class, () -> stateMachine.setState(ExampleEnumState.END));
    assertEquals(ExampleEnumState.RUNNING, stateMachine.setState(ExampleEnumState.RUNNING));
    assertEquals(ExampleEnumState.END, stateMachine.setState(ExampleEnumState.END));
    assertTrue(stateMachine.isTerminal());
    assertFalse(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.END, stateMachine.setDryState(ExampleEnumState.ERROR));
    assertThrows(IllegalStateException.class, () -> stateMachine.setState(ExampleEnumState.ERROR));
    assertNotNull(stateMachine.removeTransition(ExampleEnumState.END));
    final var transition = new StateTransition<ExampleEnumState>(ExampleEnumState.END, ExampleEnumState.ERROR);
    assertNull(stateMachine.addTransition(transition));
    assertTrue(stateMachine.isReachable(ExampleEnumState.ERROR));
    assertEquals(ExampleEnumState.ERROR, stateMachine.getDefaultNextReachableState());
    assertEquals(ExampleEnumState.ERROR, stateMachine.setState(ExampleEnumState.ERROR));
    stateMachine.release();
  }

  @Test
  void checkStateMachineWrongBuild() {
    assertThrows(IllegalStateException.class, () -> new StateMachine<>(null));
    assertThrows(IllegalStateException.class,
        () -> new StateMachine<>(ExampleEnumState.NONE, (StateTransition<ExampleEnumState>[]) null));
    assertThrows(IllegalStateException.class,
        () -> new StateMachine<>(null, (StateTransition<ExampleEnumState>[]) null));
    assertThrows(IllegalStateException.class,
        () -> new StateMachine<>(ExampleEnumState.NONE, (Collection<StateTransition<ExampleEnumState>>) null));
    assertThrows(NullPointerException.class,
        () -> new StateMachine<>(ExampleEnumState.NONE, (StateTransition<ExampleEnumState>) null));
    assertThrows(IllegalStateException.class,
        () -> new StateMachine<>(ExampleEnumState.NONE, (StateTransition<ExampleEnumState>[]) null));
    assertThrows(IllegalStateException.class, () -> new StateMachine<>(null, ExampleEnumState.configuration));
    assertThrows(IllegalStateException.class,
        () -> new StateMachine<>(ExampleEnumState.START, (StateTransition<ExampleEnumState>[]) null));
    assertThrows(IllegalStateException.class, () -> new StateTransition<>(null));
    new StateTransition<>(ExampleEnumState.NONE, null);
    new StateTransition<>(ExampleEnumState.NONE, null, null);
    new StateTransition<>(ExampleEnumState.NONE, ExampleEnumState.ERROR, null);
    assertThrows(IllegalStateException.class,
        () -> new StateTransition<>(ExampleEnumState.NONE, ExampleEnumState.ERROR, EnumSet.of(ExampleEnumState.END)));
    assertThrows(IllegalStateException.class, () -> new StateTransition<>(ExampleEnumState.NONE, ExampleEnumState.ERROR,
        EnumSet.noneOf(ExampleEnumState.class)));
    final var stateMachine = new StateMachine<ExampleEnumState>(ExampleEnumState.NONE);
    assertThrows(IllegalStateException.class, () -> stateMachine.addTransition(null));
  }
}
