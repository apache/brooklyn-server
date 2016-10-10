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
package org.apache.brooklyn.core.mgmt.rebind;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.os.Os;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

public class RebindWithDeserializingClassRenamesTest extends RebindTestFixtureWithApp {

    @Test
    @SuppressWarnings("deprecation")
    public void testRebindWillRenameLegacyConfigInheritance() throws Exception {
        Map<String, String> expectedTransforms = ImmutableMap.<String, String>builder()
                .put("org.apache.brooklyn.config.ConfigInheritance$None", "org.apache.brooklyn.config.ConfigInheritance$Legacy$None")
                .put("org.apache.brooklyn.config.ConfigInheritance$Always", "org.apache.brooklyn.config.ConfigInheritance$Legacy$Always")
                .put("org.apache.brooklyn.config.ConfigInheritance$Merged", "org.apache.brooklyn.config.ConfigInheritance$Legacy$Merged")
                .build();
        
        String entityId = "toruf2wxg4";
        String entityResource = "org/apache/brooklyn/core/test/rebind/deserializing-class-names/"+entityId;
        String persistedEntity = ResourceUtils.create(RebindWithDeserializingClassRenamesTest.class).getResourceAsString(entityResource);

        // Orig state contains the old names (and not the new names)
        for (Map.Entry<String, String> entry : expectedTransforms.entrySet()) {
            assertTrue(persistedEntity.contains(entry.getKey()));
            assertFalse(persistedEntity.contains(entry.getValue()));
        }
        
        File persistedEntityFile = new File(mementoDir, Os.mergePaths("entities", entityId));
        Files.write(persistedEntity.getBytes(), persistedEntityFile);
        
        rebind();
        
        // After rebind, we've re-written the persisted state so it contains the new names (and not old names)
        RebindTestUtils.waitForPersisted(mgmt());
        String newPersistedEntity = Joiner.on("\n").join(Files.readLines(persistedEntityFile, Charsets.UTF_8));
        for (Map.Entry<String, String> entry : expectedTransforms.entrySet()) {
            assertFalse(newPersistedEntity.contains(entry.getKey()));
            assertTrue(newPersistedEntity.contains(entry.getValue()));
        }

        // Check the config keys are as expected 
        EntityInternal entity = (EntityInternal) mgmt().getEntityManager().getEntity(entityId);
        Map<ConfigKey<?>, Object> config = entity.config().getAllLocalRaw();
        ConfigKey<?> keyWithInheritanceNone = Iterables.find(config.keySet(), ConfigPredicates.nameEqualTo("my.config.inheritanceNone"));
        ConfigKey<?> keyWithInheritanceAlways = Iterables.find(config.keySet(), ConfigPredicates.nameEqualTo("my.config.inheritanceAlways"));
        ConfigKey<?> keyWithInheritanceMerged = Iterables.find(config.keySet(), ConfigPredicates.nameEqualTo("my.config.inheritanceMerged"));
        
        assertLegacyConfigRuntimInheritanceMode(keyWithInheritanceNone, ConfigInheritance.InheritanceMode.NONE);
        assertLegacyConfigRuntimInheritanceMode(keyWithInheritanceAlways, ConfigInheritance.InheritanceMode.IF_NO_EXPLICIT_VALUE);
        assertLegacyConfigRuntimInheritanceMode(keyWithInheritanceMerged, ConfigInheritance.InheritanceMode.DEEP_MERGE);
    }

    @SuppressWarnings("deprecation")
    private void assertLegacyConfigRuntimInheritanceMode(ConfigKey<?> key, ConfigInheritance.InheritanceMode expected) throws Exception {
        ConfigInheritance val = key.getInheritanceByContext().get(InheritanceContext.RUNTIME_MANAGEMENT);
        Method method = val.getClass().getDeclaredMethod("getMode");
        method.setAccessible(true);
        ConfigInheritance.InheritanceMode mode = (ConfigInheritance.InheritanceMode) method.invoke(val);
        assertEquals(mode, expected);
    }
}
