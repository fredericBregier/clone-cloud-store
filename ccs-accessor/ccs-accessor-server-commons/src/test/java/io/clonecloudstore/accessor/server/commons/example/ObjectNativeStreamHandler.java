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

package io.clonecloudstore.accessor.server.commons.example;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.commons.AbstractObjectNativeStreamHandler;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;

@RequestScoped
public class ObjectNativeStreamHandler extends AbstractObjectNativeStreamHandler {
  private static final Logger LOGGER = Logger.getLogger(ObjectNativeStreamHandler.class);

  protected ObjectNativeStreamHandler() {
    super(CDI.current().select(AccessorObjectService.class).get());
  }

  protected ObjectNativeStreamHandler(final AccessorObjectService service) {
    super(service);
  }

  @Override
  protected boolean checkPullAble(final AccessorObject object, final MultiMap headers) {
    try {
      if (isListing) {
        checked.set(null);
        LOGGER.debug("Filter able: " + object.getBucket() + " = " + filter);
        return true;
      }
      final var response = service.getObjectInfo(object.getBucket(), object.getName());
      checked.set(response);
      LOGGER.debug("Pull able: " + object.getBucket() + "/" + object.getName() + " = " + object);
      return true;
    } catch (final CcsNotExistException e) {
      LOGGER.debug("Not Pull able: " + object.getBucket() + "/" + object.getName());
      checked.set(null);
      return false;
    }
  }

  @Override
  protected InputStream getPullInputStream(final AccessorObject object) {
    try {
      if (isListing) {
        return ((AccessorObjectService) service).filterObjects(object.getBucket(), filter, driverApi);
      }
      LOGGER.debugf("Debug Log Read: %s %s", object.getBucket(), object.getName());
      return driverApi.objectGetInputStreamInBucket(object.getBucket(), object.getName());
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderError(final AccessorObject object, final int status) {
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(object, map);
    return map;
  }
}
