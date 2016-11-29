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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.core.sensor.SensorEventPredicates;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.policy.ha.HASensors;
import org.apache.brooklyn.policy.ha.ServiceFailureDetector;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test
public class ServiceFailureDetectorYamlTest extends AbstractYamlTest {
    
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ServiceFailureDetectorYamlTest.class);
    
    static final String INDICATOR_KEY_1 = "test-indicator-1";

    static final String appId = "my-app";
    static final String appVersion = "1.2.3";
    static final String appVersionedId = appId + ":" + appVersion;
    
    static final String catalogYamlSimple = Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  id: " + appId,
            "  version: " + appVersion,
            "  itemType: entity",
            "  item:",
            "    type: " + TestEntity.class.getName(),
            "    name: targetEntity",
            "    brooklyn.enrichers:",
            "    - type: " + ServiceFailureDetector.class.getName());

    static final String catalogYamlWithDsl = Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  id: my-app",
            "  version: 1.2.3",
            "  itemType: entity",
            "  item:",
            "    services:",
            "    - type: " + TestEntity.class.getName(),//FailingEntity.class.getName(),
            "      name: targetEntity",
            "      brooklyn.parameters:",
            "      - name: custom.stabilizationDelay",
            "        type: " + Duration.class.getName(),
            "        default: 1ms",
            "      - name: custom.republishTime",
            "        type: " + Duration.class.getName(),
            "        default: 1m",
            "      brooklyn.enrichers:",
            "      - type: " + ServiceFailureDetector.class.getName(),
            "        brooklyn.config:",
            "          serviceOnFire.stabilizationDelay: $brooklyn:config(\"custom.stabilizationDelay\")",
            "          entityFailed.stabilizationDelay: $brooklyn:config(\"custom.stabilizationDelay\")",
            "          entityRecovered.stabilizationDelay: $brooklyn:config(\"custom.stabilizationDelay\")",
            "          entityFailed.republishTime: $brooklyn:config(\"custom.republishTime\")");

    /*
     * TODO Currently have to use `scopeRoot`. The brooklyn.parameter defined on the parent entity 
     * isn't inherited, so when the child does `$brooklyn:config("custom.stabilizationDelay")` it 
     * doesn't find a default value - we get null.
     */
    static final String catalogYamlWithDslReferenceParentDefault = Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  id: my-app",
            "  version: 1.2.3",
            "  itemType: entity",
            "  item:",
            "    brooklyn.parameters:",
            "    - name: custom.stabilizationDelay",
            "      type: " + Duration.class.getName(),
            "      default: 1ms",
            "    - name: custom.republishTime",
            "      type: " + Duration.class.getName(),
            "      default: 1m",
            "    services:",
            "    - type: " + TestEntity.class.getName(),
            "      name: ignored",
            "    - type: " + TestEntity.class.getName(),
            "      name: targetEntity",
            "      brooklyn.enrichers:",
            "      - type: " + ServiceFailureDetector.class.getName(),
            "        brooklyn.config:",
            "          serviceOnFire.stabilizationDelay: $brooklyn:scopeRoot().config(\"custom.stabilizationDelay\")",
            "          entityFailed.stabilizationDelay: $brooklyn:scopeRoot().config(\"custom.stabilizationDelay\")",
            "          entityRecovered.stabilizationDelay: $brooklyn:scopeRoot().config(\"custom.stabilizationDelay\")",
            "          entityFailed.republishTime: $brooklyn:scopeRoot().config(\"custom.republishTime\")");

    @Test
    public void testDefaults() throws Exception {
        runTest(catalogYamlSimple, appVersionedId);
    }
    
    @Test
    public void testWithDslConfig() throws Exception {
        Entity app = runTest(catalogYamlWithDsl, appVersionedId);
        
        TestEntity newEntity = (TestEntity) Iterables.find(app.getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        ServiceFailureDetector newEnricher = assertHasEnricher(newEntity, ServiceFailureDetector.class);
        assertEnricherConfigMatchesDsl(newEnricher);
    }

    @Test
    public void testWithDslConfigReferenceParentDefault() throws Exception {
        Entity app = runTest(catalogYamlWithDslReferenceParentDefault, appVersionedId);
        
        TestEntity newEntity = (TestEntity) Iterables.find(app.getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        ServiceFailureDetector newEnricher = assertHasEnricher(newEntity, ServiceFailureDetector.class);
        assertEnricherConfigMatchesDsl(newEnricher);
    }
    
    protected Entity runTest(String catalogYaml, String appId) throws Exception {
        addCatalogItems(catalogYaml);

        String appYaml = Joiner.on("\n").join(
                "services:",
                "- type: " + appId);
        Entity app = createStartWaitAndLogApplication(appYaml);
        TestEntity entity = (TestEntity) Iterables.find(app.getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        assertHasEnricher(entity, ServiceFailureDetector.class);
        
        // Confirm ServiceFailureDetector triggers event
        RecordingSensorEventListener<Object> listener = subscribeToHaSensors(entity);
        
        ServiceNotUpLogic.updateNotUpIndicator(entity, INDICATOR_KEY_1, "Simulate a problem");
        listener.assertHasEventEventually(SensorEventPredicates.sensorEqualTo(HASensors.ENTITY_FAILED));
        listener.assertEventCount(1);
        listener.clearEvents();
        
        ServiceNotUpLogic.clearNotUpIndicator(entity, INDICATOR_KEY_1);
        listener.assertHasEventEventually(SensorEventPredicates.sensorEqualTo(HASensors.ENTITY_RECOVERED));
        listener.assertEventCount(1);
        
        return app;
    }
    
    protected static <T extends Enricher> T assertHasEnricher(Entity entity, Class<T> enricherClazz) {
        Enricher enricher = Iterables.find(entity.enrichers(), Predicates.instanceOf(enricherClazz));
        assertNotNull(enricher);
        return enricherClazz.cast(enricher);
    }
    
    protected static void assertEnricherConfig(Enricher enricher, Map<? extends ConfigKey<?>, ?> expectedVals) {
        for (Map.Entry<? extends ConfigKey<?>, ?> entry : expectedVals.entrySet()) {
            ConfigKey<?> key = entry.getKey();
            Object actual = enricher.config().get(key);
            assertEquals(actual, entry.getValue(), "key="+key+"; expected="+entry.getValue()+"; actual="+actual);
        }
    }
    
    protected static void assertEnricherConfigMatchesDsl(Enricher enricher) {
        assertEnricherConfig(enricher, ImmutableMap.<ConfigKey<?>, Object>of(
                ServiceFailureDetector.ENTITY_FAILED_STABILIZATION_DELAY, Duration.ONE_MILLISECOND,
                ServiceFailureDetector.SERVICE_ON_FIRE_STABILIZATION_DELAY, Duration.ONE_MILLISECOND,
                ServiceFailureDetector.ENTITY_RECOVERED_STABILIZATION_DELAY, Duration.ONE_MILLISECOND,
                ServiceFailureDetector.ENTITY_FAILED_REPUBLISH_TIME, Duration.ONE_MINUTE));
    }

    protected static RecordingSensorEventListener<Object> subscribeToHaSensors(Entity entity) {
        RecordingSensorEventListener<Object> listener = new RecordingSensorEventListener<>();
        entity.subscriptions().subscribe(entity, HASensors.ENTITY_RECOVERED, listener);
        entity.subscriptions().subscribe(entity, HASensors.ENTITY_FAILED, listener);
        return listener;
    }
}
