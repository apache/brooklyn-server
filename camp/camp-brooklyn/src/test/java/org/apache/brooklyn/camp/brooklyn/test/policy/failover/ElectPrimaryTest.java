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
package org.apache.brooklyn.camp.brooklyn.test.policy.failover;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.policy.failover.ElectPrimaryConfig;
import org.apache.brooklyn.policy.failover.ElectPrimaryConfig.PrimaryDefaultSensorsAndEffectors;
import org.apache.brooklyn.policy.failover.ElectPrimaryConfig.SelectionMode;
import org.apache.brooklyn.policy.failover.ElectPrimaryEffector;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.LoggingVerboseReporter;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestNGListener;
import org.testng.TestNG;
import org.testng.annotations.Test;
import org.testng.reporters.FailedReporter;

import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;

// TODO this test is in the CAMP package because YAML not available in policies package
public class ElectPrimaryTest extends AbstractYamlTest {

    private static final Logger log = LoggerFactory.getLogger(ElectPrimaryTest.class);
    
    public static AttributeSensor<Entity> PRIMARY = PrimaryDefaultSensorsAndEffectors.PRIMARY;
    public static ConfigKey<Double> WEIGHT_CONFIG = PrimaryDefaultSensorsAndEffectors.PRIMARY_WEIGHT_CONFIG;
    public static AttributeSensor<Double> WEIGHT_SENSOR = PrimaryDefaultSensorsAndEffectors.PRIMARY_WEIGHT_SENSOR;

    protected void setItemFromTestAsSimple() {
        addCatalogItems(Strings.lines(
            "brooklyn.catalog:",
            "  items:",
            "  - id: item-from-test",
            "    item:",
            "      type: "+TestEntity.class.getName()));
    }
    
    @Test
    public void testTwoChildren() throws Exception {
        setItemFromTestAsSimple();
        
        Entity app = createAndStartApplication(loadYaml("classpath://org/apache/brooklyn/policy/failover/elect-primary-simple-test.yaml"));
        EntityAsserts.assertAttributeEventually(app, PRIMARY, Predicates.notNull());
        log.info("Primary sensor is: "+app.sensors().get(PRIMARY));
    }

    @Test
    public void testSetPreferredViaWeightConfigOnB() throws Exception {
        runSetPreferredViaWeightConfigOnB();
    }
    
    public Entity runSetPreferredViaWeightConfigOnB() throws Exception {
        setItemFromTestAsSimple();
        
        Entity app = createAndStartApplication(loadYaml("classpath://org/apache/brooklyn/policy/failover/elect-primary-simple-test.yaml",
            "  brooklyn.config:",
            "    "+WEIGHT_CONFIG.getName()+": 1"));
        
        EntityAsserts.assertAttributeEventually(app, PRIMARY, Predicates.notNull());
        Assert.assertEquals(app.sensors().get(PRIMARY).getDisplayName(), "b");
        EntityAsserts.assertEntityHealthyEventually(app);
        return app;
    }

    @Test
    public void testSetDisallowedViaWeightConfigOnB() throws Exception {
        setItemFromTestAsSimple();
        
        Entity app = createAndStartApplication(loadYaml("classpath://org/apache/brooklyn/policy/failover/elect-primary-simple-test.yaml",
            "  brooklyn.config:",
            "    "+WEIGHT_CONFIG.getName()+": -1"));
        
        EntityAsserts.assertAttributeEventually(app, PRIMARY, Predicates.notNull());
        Assert.assertEquals(app.sensors().get(PRIMARY).getDisplayName(), "a");
    }

    @Test
    public void testFailover() throws Exception {
        Entity app = runSetPreferredViaWeightConfigOnB();
        Entity b = (Entity)mgmt().<Entity>lookup(EntityPredicates.displayNameEqualTo("b"));
        
        Entities.unmanage(b);
        EntityAsserts.assertAttributeEventually(app, PRIMARY, EntityPredicates.displayNameEqualTo("a"));
    }

    @Test
    public void testPropagateSensorsWithFailover() throws Exception {
        setItemFromTestAsSimple();
        
        Entity app = createAndStartApplication(loadYaml("classpath://org/apache/brooklyn/policy/failover/elect-primary-propagate-test.yaml"));
        
        AttributeSensor<String> S1 = Sensors.newStringSensor("sens1");
        AttributeSensor<String> S2 = Sensors.newStringSensor("sens2");
        AttributeSensor<String> S3 = Sensors.newStringSensor("sens3");
        
        Entity a = (Entity)mgmt().<Entity>lookup(EntityPredicates.displayNameEqualTo("a"));
        Entity b = (Entity)mgmt().<Entity>lookup(EntityPredicates.displayNameEqualTo("b"));
        
        EntityAsserts.assertAttributeEqualsEventually(b, S1, "hi1");
        
        EntityAsserts.assertAttributeEventually(app, PRIMARY, Predicates.notNull());
        Assert.assertEquals(app.sensors().get(PRIMARY), b);
        EntityAsserts.assertEntityHealthyEventually(app);
        EntityAsserts.assertAttributeEqualsEventually(app, S1, "hi1");

        b.sensors().set(S3, "hi-3-1");
        b.sensors().set(S1, "hi2");
        b.sensors().set(S2, "hi-2-1");
        EntityAsserts.assertAttributeEqualsEventually(app, S1, "hi2");
        EntityAsserts.assertAttributeEqualsEventually(app, S2, "hi-2-1");
        // S3 not propagated
        Asserts.assertNull(app.sensors().get(S3));
        
        a.sensors().set(S1, "hi-a-1");
        Entities.unmanage(b);
        EntityAsserts.assertAttributeEqualsEventually(app, PRIMARY, a);
        // s1 changes
        EntityAsserts.assertAttributeEqualsEventually(app, S1, "hi-a-1");
        // and s2 is cleared
        EntityAsserts.assertAttributeEqualsEventually(app, S2, null);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testFireCausesPromoteDemote() throws Exception {
        Entity app = runSetPreferredViaWeightConfigOnB();
        
        Collection<Entity> testEntities = mgmt().<Entity>lookupAll(EntityPredicates.hasInterfaceMatching(".*TestEntity"));
        Asserts.assertSize(testEntities, 2);
        promoteDemoteEffectorMessages.clear();
        testEntities.forEach((entity) -> {
            new MockPromoteDemoteEffector(ElectPrimaryConfig.PrimaryDefaultSensorsAndEffectors.PROMOTE).apply((EntityLocal) entity);
            new MockPromoteDemoteEffector(ElectPrimaryConfig.PrimaryDefaultSensorsAndEffectors.DEMOTE).apply((EntityLocal) entity);
        });
        
        Entity a = (Entity)mgmt().<Entity>lookup(EntityPredicates.displayNameEqualTo("a"));
        Entity b = (Entity)mgmt().<Entity>lookup(EntityPredicates.displayNameEqualTo("b"));
        
        a.sensors().set(WEIGHT_SENSOR, 2.0d);
        ((Startable)b).stop();
        // or can do this; but it causes app to be on fire because it is requiring children healthy
        // TODO what's the best way for a node to say it's allowed to have failed children?
        // run a variant of this test using that, to ensure policy kicks in on down or on fire
        //ServiceProblemsLogic.updateProblemsIndicator(b, "test", "force fire");
        
        EntityAsserts.assertAttributeEventually(app, PRIMARY, EntityPredicates.displayNameEqualTo("a"));
        // promotion and demotion should finish by the time the app is running again
        EntityAsserts.assertEntityHealthyEventually(app);
        
        // shouldn't be necessary as app is set starting before primary set to a, so
        // above method checks not pass until promotion/demotion invocations have also completed
        //Asserts.succeedsEventually(() -> Asserts.assertSize(promoteDemoteEffectorMessages, 2));
        
        Asserts.assertTrue(promoteDemoteEffectorMessages.stream().anyMatch((s) -> s.matches("promote .*"+a.getId()+".* args=.*")),
            "Missing/bad promotion message in: "+promoteDemoteEffectorMessages);
        Asserts.assertTrue(promoteDemoteEffectorMessages.stream().anyMatch((s) -> s.matches("demote .*"+b.getId()+".* args=.*")),
            "Missing/bad demotion message in: "+promoteDemoteEffectorMessages);
        
        promoteDemoteEffectorMessages.clear();
    }
    
    List<String> promoteDemoteEffectorMessages = Collections.synchronizedList(MutableList.of());
    private class MockPromoteDemoteEffector extends AddEffector {
        public MockPromoteDemoteEffector(Effector<String> base) {
            super(Effectors.effector(base).impl(new MockPromoteDemoteEffectorBody(base.getName())).build());
        }
    }
    private class MockPromoteDemoteEffectorBody extends EffectorBody<String> {
        private String message;
        private MockPromoteDemoteEffectorBody(String message) {
            this.message = message;
        }
        @Override
        public String call(ConfigBag args) {
            String result = message+" "+entity()+" args="+args.getAllConfigRaw();
            promoteDemoteEffectorMessages.add(result);
            return result;
        }
    }

    // deferred:
    // TODO support configurable parallelisation of promote/demote (in the code)
    // TODO tests for timeout
    // TODO tests for effector propagation in addition to sensor propagation
    
    @Test
    public void testSelectionModeStrictReelectWithPreference() throws Exception {
        runSelectionModeTest(SelectionMode.STRICT, false);
    }

    @Test
    public void testSelectionModeBestReelectWithPreference() throws Exception {
        runSelectionModeTest(SelectionMode.BEST, false);
    }

    @Test
    public void testSelectionModeFailoverReelectWithPreference() throws Exception {
        runSelectionModeTest(SelectionMode.FAILOVER, false);
    }
    
    @Test(groups="Integration")
    public void testSelectionModeStrictReelectWithPreferenceIntegration() throws Exception {
        runSelectionModeTest(SelectionMode.STRICT, true);
    }

    @Test(groups="Integration")
    public void testSelectionModeBestReelectWithPreferenceIntegration() throws Exception {
        runSelectionModeTest(SelectionMode.BEST, true);
    }

    @Test(groups="Integration")
    public void testSelectionModeFailoverReelectWithPreferenceIntegration() throws Exception {
        runSelectionModeTest(SelectionMode.FAILOVER, true);
    }

    private void runSelectionModeTest(SelectionMode mode, boolean integration) throws Exception {
        Entity app = null;
        try {
            setItemFromTestAsSimple();
            
            app = createAndStartApplication(Strings.replaceAllNonRegex(
                loadYaml("classpath://org/apache/brooklyn/policy/failover/elect-primary-selection-mode-test.yaml"), 
                "$$REPLACE$$", mode.name().toLowerCase()) );
            EntityAsserts.assertEntityHealthyEventually(app);
    
            Entity a = (Entity)mgmt().<Entity>lookup(EntityPredicates.displayNameEqualTo("a"));
            Entity b = (Entity)mgmt().<Entity>lookup(EntityPredicates.displayNameEqualTo("b"));
            // guarantee not running until has primary - via the enricher
            EntityAsserts.assertAttributeEquals(app, PRIMARY, a);
            
            b.sensors().set(WEIGHT_SENSOR, 2.0d);
            if (mode==SelectionMode.FAILOVER) {
                // primary won't change in failover mode
                assertPrimaryUnchanged(app, a, integration);
                
                // force election of best, overriding config mode
                app.invoke(ElectPrimaryEffector.EFFECTOR, ConfigBag.newInstance().configure(
                    ElectPrimaryConfig.SELECTION_MODE, SelectionMode.BEST).getAllConfigRaw()).get();
            }
            EntityAsserts.assertAttributeEventually(app, PRIMARY, Predicates.equalTo(b));
            
            if (mode==SelectionMode.FAILOVER) {
                // other tests don't apply to faliover
                return;
            }
            
            b.sensors().set(WEIGHT_SENSOR, -1.0d);
            // disabling b causes revert to a
            EntityAsserts.assertAttributeEventually(app, PRIMARY, Predicates.equalTo(a));
            
            b.sensors().set(WEIGHT_SENSOR, 1.0d);
            // now have a tie, should fail in strict, preserve a in best
            if (mode==SelectionMode.STRICT) {
                EntityAsserts.assertAttributeEqualsEventually(app, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
            } else if (mode==SelectionMode.BEST) {
                assertPrimaryUnchanged(app, a, integration);
                EntityAsserts.assertEntityHealthyEventually(app);
            }
            try {
                Object result = getLastElectionTask(app).get();
                // one error was observed in above call outside of strict mode but couldn't tell what;
                // refactored to say what the error was -- possibly task TC'd or maybe a CME;
                // was running in a tight loop so likely we'll never see it again
                if (mode==SelectionMode.STRICT) {
                    Asserts.shouldHaveFailedPreviously("Instead got: "+result);
                }
            } catch (Exception e) {
                if (mode!=SelectionMode.STRICT) {
                    // tie should only fail in strict mode
                    Exceptions.propagate(e);
                }
                Asserts.expectedFailureContainsIgnoreCase(e, 
                    "Cannot select primary", b.toString(), a.toString());
            }
    
            a.sensors().set(WEIGHT_SENSOR, -1d);
            // disabling a reverts to b and makes it healthy
            EntityAsserts.assertAttributeEventually(app, PRIMARY, Predicates.equalTo(b));
            EntityAsserts.assertAttributeEqualsEventually(app, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
            
            // now disabling b causes failure
            b.sensors().set(WEIGHT_SENSOR, -1d);
            log.info("Waiting for no primary");
            EntityAsserts.assertAttributeEqualsEventually(app, PRIMARY, null);
            Object result = getLastElectionTask(app).get();
            Asserts.assertStringContainsIgnoreCase(result.toString(), ElectPrimaryEffector.ResultCode.NO_PRIMARY_AVAILABLE.toString());
            
        } catch (Throwable t) {
            log.error("Failed: "+t, t);
            Dumper.dumpInfo(app);
            t.printStackTrace();
            Exceptions.propagateIfFatal(t);
        }
    }

    protected Task<?> getLastElectionTask(Entity app) {
        // look up last task as noted in highlight and confirm it gave the right output
        return mgmt().getExecutionManager().getTask(app.policies().iterator().next().getHighlights().get("lastScan").getTaskId());
    }

    private void assertPrimaryUnchanged(Entity app, Entity a, boolean integration) {
        if (integration) {
            EntityAsserts.assertAttributeEqualsContinually(app, PRIMARY, a);
        } else {
            EntityAsserts.assertAttributeEquals(app, PRIMARY, a);
        }
    }

    public static void main(String[] args) throws Exception {
        int count = -1;
        Stopwatch sw = Stopwatch.createStarted();
        while (++count<100) {
            log.info("new test run\n\n\nTEST RUN "+count+"\n");
            
//            ElectPrimaryTest t = new ElectPrimaryTest();
//            t.setUp();
//            t.testFireCausesPromoteDemote();
//            t.tearDown();
            
            TestNG testNG = new TestNG();
            testNG.setTestClasses(new Class[] { ElectPrimaryTest.class });
            testNG.addListener((ITestNGListener)new LoggingVerboseReporter());
            FailedReporter failedReporter = new FailedReporter();
            testNG.addListener((ITestNGListener)failedReporter);
            testNG.run();
            if (!failedReporter.getFailedTests().isEmpty()) {
                log.error("Failures: "+failedReporter.getFailedTests());
                System.exit(1);
            }
        }
        log.info("\n\nCompleted "+count+" runs in "+Duration.of(sw));
    }
}
