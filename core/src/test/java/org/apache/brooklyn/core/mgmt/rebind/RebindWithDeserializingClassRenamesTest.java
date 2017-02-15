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

import java.io.File;
import java.util.Map;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.os.Os;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

public class RebindWithDeserializingClassRenamesTest extends RebindTestFixtureWithApp {

    /** also see {@link RebindConfigInheritanceTest} */
    @Test
    public void testRebindWillRenameLegacyConfigInheritance() throws Exception {
        Map<String, String> expectedTransforms = ImmutableMap.<String, String>builder()
                .put("org.apache.brooklyn.config.ConfigInheritance$None", BasicConfigInheritance.NOT_REINHERITED.getClass().getName())
                .put("org.apache.brooklyn.config.ConfigInheritance$Always", BasicConfigInheritance.OVERWRITE.getClass().getName())
                .put("org.apache.brooklyn.config.ConfigInheritance$Merged", BasicConfigInheritance.DEEP_MERGE.getClass().getName())
                .build();
        
        String entityId = "toruf2wxg4";
        String entityResource = "org/apache/brooklyn/core/test/rebind/deserializing-class-names/"+entityId;
        String persistedEntity = ResourceUtils.create(RebindWithDeserializingClassRenamesTest.class).getResourceAsString(entityResource);

        // Orig state contains the old names (and not the new names)
        for (Map.Entry<String, String> entry : expectedTransforms.entrySet()) {
            Asserts.assertStringContains(persistedEntity, entry.getKey());
            Asserts.assertStringDoesNotContain(persistedEntity, entry.getValue());
        }
        
        File persistedEntityFile = new File(mementoDir, Os.mergePaths("entities", entityId));
        Files.write(persistedEntity.getBytes(), persistedEntityFile);
        
        rebind();
        
        // After rebind, we've re-written the persisted state so it contains the new names (and not old names)
        RebindTestUtils.waitForPersisted(mgmt());
        String newPersistedEntity = Joiner.on("\n").join(Files.readLines(persistedEntityFile, Charsets.UTF_8));
        for (Map.Entry<String, String> entry : expectedTransforms.entrySet()) {
            Asserts.assertStringDoesNotContain(newPersistedEntity, entry.getKey());
            Asserts.assertStringContains(newPersistedEntity, entry.getValue());
        }

        // Check the config keys are as expected 
        EntityInternal entity = (EntityInternal) mgmt().getEntityManager().getEntity(entityId);
        Map<ConfigKey<?>, Object> config = entity.config().getAllLocalRaw();
        ConfigKey<?> keyWithInheritanceNone = Iterables.find(config.keySet(), ConfigPredicates.nameEqualTo("my.config.inheritanceNone"));
        ConfigKey<?> keyWithInheritanceAlways = Iterables.find(config.keySet(), ConfigPredicates.nameEqualTo("my.config.inheritanceAlways"));
        ConfigKey<?> keyWithInheritanceMerged = Iterables.find(config.keySet(), ConfigPredicates.nameEqualTo("my.config.inheritanceMerged"));
        
        assertConfigRuntimeInheritanceMode(keyWithInheritanceNone, BasicConfigInheritance.NOT_REINHERITED);
        assertConfigRuntimeInheritanceMode(keyWithInheritanceAlways, BasicConfigInheritance.OVERWRITE);
        assertConfigRuntimeInheritanceMode(keyWithInheritanceMerged, BasicConfigInheritance.DEEP_MERGE);
    }

    private void assertConfigRuntimeInheritanceMode(ConfigKey<?> key, ConfigInheritance expected) throws Exception {
        ConfigInheritance val = key.getInheritanceByContext().get(InheritanceContext.RUNTIME_MANAGEMENT);
        assertEquals(val, expected);
    }
}
