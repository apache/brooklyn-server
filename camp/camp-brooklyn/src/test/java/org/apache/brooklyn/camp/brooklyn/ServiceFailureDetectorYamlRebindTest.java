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

import static org.apache.brooklyn.camp.brooklyn.ServiceFailureDetectorYamlTest.appVersionedId;
import static org.apache.brooklyn.camp.brooklyn.ServiceFailureDetectorYamlTest.assertEnricherConfigMatchesDsl;
import static org.apache.brooklyn.camp.brooklyn.ServiceFailureDetectorYamlTest.assertHasEnricher;
import static org.apache.brooklyn.camp.brooklyn.ServiceFailureDetectorYamlTest.catalogYamlSimple;
import static org.apache.brooklyn.camp.brooklyn.ServiceFailureDetectorYamlTest.catalogYamlWithDsl;
import static org.apache.brooklyn.camp.brooklyn.ServiceFailureDetectorYamlTest.catalogYamlWithDslReferenceParentDefault;
import static org.apache.brooklyn.camp.brooklyn.ServiceFailureDetectorYamlTest.subscribeToHaSensors;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager.RebindFailureMode;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.core.mgmt.rebind.RebindExceptionHandlerImpl;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RecordingRebindExceptionHandler;
import org.apache.brooklyn.core.sensor.SensorEventPredicates;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.policy.ha.HASensors;
import org.apache.brooklyn.policy.ha.ServiceFailureDetector;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

@Test
public class ServiceFailureDetectorYamlRebindTest extends AbstractYamlRebindTest {
    
    final static String INDICATOR_KEY_1 = "test-indicator-1";

    @Test
    public void testRebindWhenHealthy() throws Exception {
        runRebindWhenHealthy(catalogYamlSimple, appVersionedId);
    }
    
    @Test
    public void testRebindWhenHealthyWithDslConfig() throws Exception {
        runRebindWhenHealthy(catalogYamlWithDsl, appVersionedId);
        
        TestEntity newEntity = (TestEntity) Iterables.find(app().getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        ServiceFailureDetector newEnricher = assertHasEnricher(newEntity, ServiceFailureDetector.class);
        assertEnricherConfigMatchesDsl(newEnricher);
    }

    @Test
    public void testRebindWhenHealthyWithDslConfigReferenceParentDefault() throws Exception {
        runRebindWhenHealthy(catalogYamlWithDslReferenceParentDefault, appVersionedId);
        
        TestEntity newEntity = (TestEntity) Iterables.find(app().getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        ServiceFailureDetector newEnricher = assertHasEnricher(newEntity, ServiceFailureDetector.class);
        assertEnricherConfigMatchesDsl(newEnricher);
    }
    
    @Test
    public void testRebindWhenHasNotUpIndicator() throws Exception {
        runRebindWhenNotUp(catalogYamlSimple, appVersionedId);
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-345
    @Test
    public void testRebinWhenHasNotUpIndicatorWithDslConfig() throws Exception {
        runRebindWhenNotUp(catalogYamlWithDsl, appVersionedId);
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-345
    @Test
    public void testRebindWhenHasNotUpIndicatorWithDslConfigReferenceParentDefault() throws Exception {
        runRebindWhenNotUp(catalogYamlWithDslReferenceParentDefault, appVersionedId);
    }
    
    protected void runRebindWhenHealthy(String catalogYaml, String appId) throws Exception {
        addCatalogItems(catalogYaml);

        String appYaml = Joiner.on("\n").join(
                "services:",
                "- type: " + appId);
        createStartWaitAndLogApplication(appYaml);

        // Rebind
        StartableApplication newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.find(newApp.getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        assertHasEnricher(newEntity, ServiceFailureDetector.class);
        
        // Confirm ServiceFailureDetector still functions
        RecordingSensorEventListener<Object> listener = subscribeToHaSensors(newEntity);
        
        ServiceNotUpLogic.updateNotUpIndicator(newEntity, INDICATOR_KEY_1, "Simulate a problem");
        listener.assertHasEventEventually(SensorEventPredicates.sensorEqualTo(HASensors.ENTITY_FAILED));
        listener.assertEventCount(1);
    }

    protected void runRebindWhenNotUp(String catalogYaml, String appId) throws Exception {
        addCatalogItems(catalogYaml);

        String appYaml = Joiner.on("\n").join(
                "services:",
                "- type: " + appId);
        Entity app = createStartWaitAndLogApplication(appYaml);
        
        // Make entity go on-fire
        TestEntity entity = (TestEntity) Iterables.find(app.getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        RecordingSensorEventListener<Object> listener = subscribeToHaSensors(entity);
        ServiceNotUpLogic.updateNotUpIndicator(entity, INDICATOR_KEY_1, "Simulating a problem");
        listener.assertHasEventEventually(SensorEventPredicates.sensorEqualTo(HASensors.ENTITY_FAILED));

        // Rebind
        StartableApplication newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.find(newApp.getChildren(), EntityPredicates.displayNameEqualTo("targetEntity"));
        assertHasEnricher(newEntity, ServiceFailureDetector.class);
        
        // Confirm ServiceFailureDetector still functions
        RecordingSensorEventListener<Object> newListener = subscribeToHaSensors(newEntity);
        
        ServiceNotUpLogic.clearNotUpIndicator(newEntity, INDICATOR_KEY_1);
        newListener.assertHasEventEventually(SensorEventPredicates.sensorEqualTo(HASensors.ENTITY_RECOVERED));
        newListener.assertEventCount(1);
    }
    
    @Override
    protected StartableApplication rebind() throws Exception {
        RecordingRebindExceptionHandler exceptionHandler = new RecordingRebindExceptionHandler(RebindExceptionHandlerImpl.builder()
                .addPolicyFailureMode(RebindFailureMode.FAIL_AT_END)
                .loadPolicyFailureMode(RebindFailureMode.FAIL_AT_END)
                .danglingRefFailureMode(RebindFailureMode.FAIL_AT_END));
        return rebind(RebindOptions.create().exceptionHandler(exceptionHandler));
    }
}
