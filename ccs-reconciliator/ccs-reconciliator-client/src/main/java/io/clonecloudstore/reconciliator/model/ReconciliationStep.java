/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.reconciliator.model;

public enum ReconciliationStep {
  CREATE,
  CREATE_ERROR,
  LOCAL,
  LOCAL_ERROR,
  CENTRALIZE,
  CENTRALIZE_ERROR,
  COMPUTE,
  COMPUTE_ERROR,
  DRYRUN,
  DISPATCH,
  DISPATCH_ERROR,
  ACTION,
  ACTION_ERROR,
  DONE
}