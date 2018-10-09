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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.os.Os;
import org.assertj.core.api.WithAssertions;
import org.testng.annotations.Test;

public class RebindWithDeserializingClassRenamesTest extends RebindTestFixtureWithApp implements WithAssertions {

    private static final String LEGACY_INHERITANCE_NONE = "org.apache.brooklyn.config.ConfigInheritance$None";
    private static final String LEGACY_INHERITANCE_ALWAYS = "org.apache.brooklyn.config.ConfigInheritance$Always";
    private static final String LEGACY_INHERITANCE_MERGED = "org.apache.brooklyn.config.ConfigInheritance$Merged";
    private static final String RESOURCE_PATH = "org/apache/brooklyn/core/test/rebind/deserializing-class-names/";
    private static final String ENTITY_ID = "toruf2wxg4";

    /** also see {@link RebindConfigInheritanceTest} */
    @Test
    public void testRebindWillRenameLegacyConfigInheritance() throws Exception {
        //given
        final List<Transform> expectedTransforms = Collections.unmodifiableList(Arrays.asList(
                Transform.of(LEGACY_INHERITANCE_NONE, BasicConfigInheritance.NOT_REINHERITED, "my.config.inheritanceNone"),
                Transform.of(LEGACY_INHERITANCE_ALWAYS, BasicConfigInheritance.OVERWRITE, "my.config.inheritanceAlways"),
                Transform.of(LEGACY_INHERITANCE_MERGED, BasicConfigInheritance.DEEP_MERGE, "my.config.inheritanceMerged")
        ));
        // a legacy brooklyn 0.10.0 entity
        final File persistedEntityFile = givenLegacyEntityFile(ENTITY_ID);
        // verify that the entity has not been updated and is still using the old names
        final String persistedEntity = readFile(persistedEntityFile);
        assertSoftly(s -> expectedTransforms
                .forEach(transform -> {
                    s.assertThat(persistedEntity)
                            .as("existing persisted entity uses legacy classnames")
                            .contains(transform.legacyClassName);
                    s.assertThat(persistedEntity)
                            .as("existing persisted entity does not use current classnames")
                            .doesNotContain(transform.currentClassName);
                }));
        //when
        // reload and rewrite the configuration
        rebind();
        //then
        // After rebind, we've re-written the persisted state so it contains the new names (and not old names)
        RebindTestUtils.waitForPersisted(mgmt());
        final String reboundPersistedEntity = readFile(persistedEntityFile);
        final Map<ConfigKey<?>, Object> config = getEntityConfig(ENTITY_ID);
        // verify that the new config file has been updated and is using the new classnames
        assertSoftly(s -> expectedTransforms
                .forEach(transform -> {
                    s.assertThat(reboundPersistedEntity)
                            .as("rebound persisted entity does not use legacy classnames")
                            .doesNotContain(transform.legacyClassName);
                    s.assertThat(reboundPersistedEntity)
                            .as("rebound persisted entity uses current classnames")
                            .contains(transform.currentClassName);
                    s.assertThat(inheritanceModeByKeyName(config, transform.configKeyName))
                            .as("config keys are as expected")
                            .isEqualTo(transform.inheritanceMode);
                }));
    }

    private File givenLegacyEntityFile(final String entityId) throws IOException {
        // load template entity config file
        final String persistedEntity = ResourceUtils.create(RebindWithDeserializingClassRenamesTest.class)
                .getResourceAsString(RESOURCE_PATH + entityId);
        // create a private/writable copy of the legacy config file
        final File persistedEntityFile = new File(mementoDir, Os.mergePaths("entities", entityId));
        Files.write(persistedEntityFile.toPath(), persistedEntity.getBytes());
        return persistedEntityFile;
    }

    private String readFile(final File legacyEntityFile) throws IOException {
        return Files.readAllLines(legacyEntityFile.toPath(), UTF_8)
                .stream().collect(Collectors.joining(System.lineSeparator()));
    }

    private Map<ConfigKey<?>, Object> getEntityConfig(final String entityId) {
        final EntityInternal entity = (EntityInternal) mgmt().getEntityManager().getEntity(entityId);
        assertThat(entity).isNotNull();
        return entity.config().getAllLocalRaw();
    }

    private ConfigInheritance inheritanceModeByKeyName(final Map<ConfigKey<?>, Object> config, final String keyName) {
        return inheritanceMode(configKeyByName(config, keyName));
    }

    private ConfigInheritance inheritanceMode(ConfigKey<?> key) {
        return key.getInheritanceByContext().get(InheritanceContext.RUNTIME_MANAGEMENT);
    }

    private ConfigKey<?> configKeyByName(final Map<ConfigKey<?>, Object> config, final String keyName) {
        return config.keySet().stream().filter(ConfigPredicates.nameEqualTo(keyName)::apply).findFirst().get();
    }

    private static class Transform {

        private String legacyClassName;
        private String currentClassName;
        private ConfigInheritance inheritanceMode;
        private String configKeyName;

        private Transform(final String legacyClassName,
                          final String currentClassName,
                          final ConfigInheritance inheritanceMode,
                          final String configKeyName) {
            this.legacyClassName = legacyClassName;
            this.currentClassName = currentClassName;
            this.inheritanceMode = inheritanceMode;
            this.configKeyName = configKeyName;
        }

        static Transform of(final String legacyClassName,
                            final ConfigInheritance configInheritance,
                            final String configKey) {
            final Class<? extends ConfigInheritance> configInheritanceClass = configInheritance.getClass();
            return new Transform(legacyClassName, configInheritanceClass.getName(), configInheritance, configKey);
        }
    }
}
