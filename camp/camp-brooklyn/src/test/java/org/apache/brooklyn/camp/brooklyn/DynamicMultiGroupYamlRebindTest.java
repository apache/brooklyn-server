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

import com.google.common.io.Files;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ha.MementoCopyMode;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.persist.BrooklynPersistenceUtils;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.entity.group.BasicGroup;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DynamicMultiGroupYamlRebindTest extends AbstractYamlRebindTest {

   @Test(invocationCount = 100)
   public void testDynamicMultiGroupWithCluster_DeleteBeforeRebind() throws Exception {
      try {
         // Create test app first.
         Entity app = createDynamicMultiGroupWithCluster();
         waitForApplicationTasks(app);

         // Expect 10 entities in persistence store.
         BrooklynMementoRawData state = BrooklynPersistenceUtils.newStateMemento(mgmt(), MementoCopyMode.LOCAL);
         Assert.assertEquals(state.getEntities().size(), 10);

//         Dumper.dumpInfo(app);

         // Destroy application before first rebind.
         Entities.destroy(app);

         // Rebind, expect no apps.
         Entity appRebind = rebind(RebindOptions.create().terminateOrigManagementContext(true));
         Assert.assertNull(appRebind);
         switchOriginalToNewManagementContext();

         // Expect no resources in persistence store.
         state = BrooklynPersistenceUtils.newStateMemento(mgmt(), MementoCopyMode.LOCAL);
         Assert.assertEquals(state.getEntities().size(), 0);
         Files.fileTraverser().breadthFirst(mementoDir).forEach(f -> {
            if (!f.isDirectory()) {
               if (MutableSet.of("planeId").contains(f.getName())) {
                  // expect these
               } else {
                  Assert.fail("At least one unexpected file exists after app stopped: " + f);
               }
            }
         });
      } catch (Throwable t) {
         throw Exceptions.propagate(t);
      }
   }

   @Test
   public void testDynamicMultiGroupWithCluster_DeleteAfterRebind() throws Exception {
      Entity app = createDynamicMultiGroupWithCluster();
      waitForApplicationTasks(app);

      // Expect 10 entities in persistence store.
      BrooklynMementoRawData state = BrooklynPersistenceUtils.newStateMemento(mgmt(), MementoCopyMode.LOCAL);
      Assert.assertEquals(state.getEntities().size(), 10);

      Entity appRebind = rebind(RebindOptions.create().terminateOrigManagementContext(true));
      switchOriginalToNewManagementContext();

      // Expect 10 entities in persistence store after rebind.
      state = BrooklynPersistenceUtils.newStateMemento(mgmt(), MementoCopyMode.LOCAL);
      Assert.assertEquals(state.getEntities().size(), 10);

      // Destroy application after first rebind.
      Entities.destroy(appRebind);

      // Rebind, expect no apps.
      rebind(RebindOptions.create().terminateOrigManagementContext(true));
      switchOriginalToNewManagementContext();

      // Expect no resources in persistence store.
      state = BrooklynPersistenceUtils.newStateMemento(mgmt(), MementoCopyMode.LOCAL);
      Assert.assertEquals(state.getEntities().size(), 0);
      Files.fileTraverser().breadthFirst(mementoDir).forEach(f -> {
         if (!f.isDirectory()) {
            if (MutableSet.of("planeId").contains(f.getName())) {
               // expect these
            } else {
               Assert.fail("At least one unexpected file exists after app stopped: " + f);
            }
         }
      });
   }

   /**
    * Creates an app from a specific blueprint combination that was discovered to detect resource leak around
    * {@link BasicGroup}.
    *
    * @return {@link Entity} application of a specific blueprint for test.
    * @throws Exception
    */
   private Entity createDynamicMultiGroupWithCluster() throws Exception {
      String yaml = "name: My Application\n" +
              "services:\n" +
              "  - type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
              "    brooklyn.config:\n" +
              "      marker: Entity Marker\n" +
              "    brooklyn.children:\n" +
              "      - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
              "      - type: org.apache.brooklyn.entity.group.DynamicCluster\n" +
              "        name: My Cluster\n" +
              "        brooklyn.config:\n" +
              "          dynamiccluster.memberspec:\n" +
              "            '$brooklyn:entitySpec':\n" +
              "               type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
              "               brooklyn.children:\n" +
              "                 - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
              "                 - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
              "  - type: org.apache.brooklyn.entity.group.DynamicMultiGroup\n" +
              "    brooklyn.config:\n" +
              "      entityFilter:\n" +
              "        $brooklyn:object:\n" +
              "          type: org.apache.brooklyn.core.entity.EntityPredicates\n" +
              "          factoryMethod.name: displayNameEqualTo\n" +
              "          factoryMethod.args:\n" +
              "            - My Cluster\n" +
              "      bucketSpec:\n" +
              "        $brooklyn:entitySpec:\n" +
              "          type: org.apache.brooklyn.entity.group.BasicGroup\n" +
              "      bucketFunction:\n" +
              "        $brooklyn:object:\n" +
              "          type: com.google.common.base.Functions\n" +
              "          factoryMethod.name: compose\n" +
              "          factoryMethod.args:\n" +
              "            - $brooklyn:object:\n" +
              "                type: org.apache.brooklyn.util.text.StringFunctions\n" +
              "                factoryMethod.name: formatter\n" +
              "                factoryMethod.args: [ My %s ]\n" +
              "            - $brooklyn:object:\n" +
              "                type: org.apache.brooklyn.core.entity.EntityFunctions\n" +
              "                factoryMethod.name: config\n" +
              "                factoryMethod.args: [ marker ]";
      return createAndStartApplication(yaml);
   }
}
