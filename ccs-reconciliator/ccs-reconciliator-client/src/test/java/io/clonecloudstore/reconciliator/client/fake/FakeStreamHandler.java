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

package io.clonecloudstore.reconciliator.client.fake;

import java.util.Map;

import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.clonecloudstore.test.server.service.FakeStreamHandlerAbstract;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class FakeStreamHandler extends FakeStreamHandlerAbstract<ReconciliationRequest, ReconciliationRequest> {
  @Override
  protected ReconciliationRequest getBusinessOutForPushAnswer(final ReconciliationRequest apiBusinessIn,
                                                              final String finalHash, final long size) {
    return null;
  }

  @Override
  protected long getLengthFromBusinessIn(final ReconciliationRequest businessIn) {
    return 0;
  }

  @Override
  protected Map<String, String> getHeaderPushInputStream(final ReconciliationRequest businessIn, final String finalHash,
                                                         final long size, final ReconciliationRequest businessOut)
      throws CcsClientGenericException, CcsServerGenericException {
    return Map.of();
  }

  @Override
  protected boolean checkPullAble(final ReconciliationRequest businessIn, final MultiMap headers)
      throws CcsClientGenericException, CcsServerGenericException {
    return true;
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final ReconciliationRequest businessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    return Map.of();
  }

  @Override
  protected Map<String, String> getHeaderError(final ReconciliationRequest businessIn, final int status) {
    return Map.of();
  }
}
