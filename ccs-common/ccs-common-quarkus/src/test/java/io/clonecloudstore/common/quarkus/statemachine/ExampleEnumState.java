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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public enum ExampleEnumState {
  NONE,
  START,
  RUNNING,
  PAUSE,
  END,
  ERROR,
  VOID;

  static final List<StateTransition<ExampleEnumState>> configuration;

  static {
    final var transitionStates = ExampleTransitionState.values();
    configuration = new ArrayList<>(transitionStates.length);
    for (final var transitionState : transitionStates) {
      configuration.add(transitionState.elt);
    }
  }

  public static StateMachine<ExampleEnumState> newStateMachine() {
    return new StateMachine<>(NONE, configuration);
  }

  public enum ExampleTransitionState {
    tNONE(NONE, START, EnumSet.of(START)),
    tSTART(START, RUNNING, EnumSet.of(RUNNING, PAUSE, ERROR)),
    tRUNNING(RUNNING, END, EnumSet.of(PAUSE, END, ERROR)),
    tPAUSE(PAUSE, RUNNING),
    tEND(END),
    tERROR(ERROR, VOID);

    public final StateTransition<ExampleEnumState> elt;

    ExampleTransitionState(final ExampleEnumState state) {
      elt = new StateTransition<>(state);
    }

    ExampleTransitionState(final ExampleEnumState state, final ExampleEnumState stateNext) {
      elt = new StateTransition<>(state, stateNext);
    }

    ExampleTransitionState(final ExampleEnumState state, final ExampleEnumState stateNext,
                           final EnumSet<ExampleEnumState> set) {
      elt = new StateTransition<>(state, stateNext, set);
    }
  }
}
