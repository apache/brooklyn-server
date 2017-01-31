/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.camp.brooklyn;

import static org.apache.brooklyn.test.Asserts.assertFalse;
import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.effector.http.HttpCommandEffector;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.guava.Maybe;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test
public class HttpCommandEffectorYamlRebindTest extends AbstractYamlRebindTest {

   private final static String appId = "my-app-with-http-effector";
   private final static String appVersion = "1.0.0-SNAPSHOT";
   static final String appVersionedId = appId + ":" + appVersion;

   static final String catalogYamlSimple = Joiner.on("\n").join(
           "brooklyn.catalog:",
           "  id: " + appId,
           "  version: " + appVersion,
           "  itemType: entity",
           "  item:",
           "    type: " + TestEntity.class.getName(),
           "    name: targetEntity",
           "    brooklyn.initializers:",
           "      - type: " + HttpCommandEffector.class.getName(),
           "        brooklyn.config:",
           "          name: myEffector",
           "          description: myDescription",
           "          uri: https://httpbin.org/get?id=myId",
           "          httpVerb: GET",
           "          jsonPath: $.args.id",
           "          publishSensor: results");

   @Test
   public void testRebindWhenHealthy() throws Exception {
      runRebindWhenIsUp(catalogYamlSimple, appVersionedId);
   }

   protected void runRebindWhenIsUp(String catalogYaml, String appId) throws Exception {
      addCatalogItems(catalogYaml);

      String appYaml = Joiner.on("\n").join(
              "services: ",
              "- type: " + appId);
      createStartWaitAndLogApplication(appYaml);

      // Rebind
      StartableApplication newApp = rebind();
      TestEntity testEntity = (TestEntity) Iterables.find(newApp.getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
      Effector effector = assertHasInitializers(testEntity, "myEffector");

      // Confirm HttpCommandEffector still functions
      Object result = testEntity.invoke(effector, ImmutableMap.<String, Object>of()).get();
      assertEquals(((String)result).trim(), "myId");
   }


   protected static Effector<?> assertHasInitializers(Entity entity, String effectorName) {
      Maybe<Effector<?>> effectorMaybe = entity.getEntityType().getEffectorByName(effectorName);
      assertFalse(effectorMaybe.isAbsent());
      return effectorMaybe.get();
   }

}
