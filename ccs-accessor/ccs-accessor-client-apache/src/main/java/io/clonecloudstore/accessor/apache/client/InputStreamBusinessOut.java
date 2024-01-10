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

package io.clonecloudstore.accessor.apache.client;

import java.io.InputStream;

/**
 * InputStream dnd BusinessOut for Getting both information on Read Object
 *
 * @param dtoOut      the Business Out or null if none
 * @param inputStream the InputStream or null if none
 * @param <O>         the type for Business Output request (in GET or POST)
 */
public record InputStreamBusinessOut<O>(O dtoOut, InputStream inputStream) {
}
