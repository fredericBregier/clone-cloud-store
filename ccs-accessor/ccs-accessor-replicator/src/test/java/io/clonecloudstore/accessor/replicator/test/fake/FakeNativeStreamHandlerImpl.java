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

package io.clonecloudstore.accessor.replicator.test.fake;

import java.io.InputStream;
import java.util.Map;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.test.accessor.common.FakeNativeStreamHandlerAbstract;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class FakeNativeStreamHandlerImpl extends FakeNativeStreamHandlerAbstract {
  public static InputStream fakeInputStream = null;
  public static Map<String, String> fakeAnswer = null;

  @Override
  protected boolean isPublic() {
    return false;
  }

  @Override
  protected boolean checkPullAble(final AccessorObject object, final MultiMap headers) {
    if (fakeInputStream != null) {
      return true;
    }
    return super.checkPullAble(object, headers);
  }

  @Override
  protected InputStream getPullInputStream(final AccessorObject object) {
    if (fakeInputStream != null) {
      return fakeInputStream;
    }
    return super.getPullInputStream(object);
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final AccessorObject objectIn) {
    if (fakeAnswer != null) {
      return fakeAnswer;
    }
    return super.getHeaderPullInputStream(objectIn);
  }
}
