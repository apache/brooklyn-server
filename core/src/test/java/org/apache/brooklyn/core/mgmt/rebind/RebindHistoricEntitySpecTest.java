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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.rebind.RebindExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.mgmt.rebind.mementos.EntityMemento;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestApplicationNoEnrichersImpl;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class RebindHistoricEntitySpecTest extends AbstractRebindHistoricTest {
    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * In https://github.com/apache/brooklyn-server/pull/859, we deleted EntitySpec.policies 
     * and EntitySpec.enrichers fields. Unfortunately that broke backwards compatibility:
     * persisted state that included these fields would no longer rebind. 
     *
     * This included for an empty list of policies, i.e. {@code <policies/>}.
     * 
     * An alternative fix would have been to for XmlMementoSerializer.SpecConverter to have
     * omitted those fields. However, it would then have been very hard to warn if we come
     * across a non-empty list of policies or enrichers.
     */
    @Test
    public void testEntitySpecWithEmptyPoliciesList() throws Exception {
        addMemento(BrooklynObjectType.ENTITY, "entity-with-entityspec-containing-empty-policies", "wj5s8u9h73");
        rebind();
    }

    /**
     * EntitySpec includes a hard-coded reference to a policy and to an enricher.
     * 
     * Ensure we log.warn if there are policies/enrichers. We discard them, as this is 
     * no longer supported. It would have been dangerous to add them: the entity spec would
     * have had a hard-coded policy id, so if the spec was used to create more than one entity
     * (e.g. in a cluster's memberSpec) then they would have shared the same policy instance,
     * which would have gone badly wrong (e.g. calling policy.setEntity multiple times).
     * 
     * Also this causes a rebind problem - dangling reference to the policy - because rebind does 
     * {@link RebindIteration#instantiateMementos()} (to instantiate the {@link EntityMemento} etc)
     * before it does {@link RebindIteration#instantiateAdjuncts(org.apache.brooklyn.core.mgmt.rebind.RebindIteration.BrooklynObjectInstantiator)}.
     * Therefore the rebindContext does not yet know about that policy instance.
     * Note that the policies of an entity are referenced via {@link EntityMemento#getPolicies()},
     * so we did not try to resolve them during instantiation of the EntityMemento.
     * In contrast, the EntitySpec's reference is for a field of type {@code List<Policy>} so needs to 
     * be resolved immediately (and because we don't use DynamicProxies for policies, we haven't 
     * created a place-holder).
     * 
     * An alternative impl would be to fail to instantiate the EntitySpec (thus failing rebind).
     * But given rebind would have failed previously (with the dangling-policy-ref described
     * above), then I think it is fine to just discard these!
    */
    @Test
    public void testEntitySpecWithPolicyInstance() throws Exception {
        // Need to ignore dangling-policy-ref (see javadoc above) 
        RebindExceptionHandler exceptionHandler = RebindExceptionHandlerImpl.builder()
                .danglingRefFailureMode(RebindManager.RebindFailureMode.CONTINUE)
                .rebindFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .addConfigFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .addPolicyFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .loadPolicyFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .build();

        addMemento(BrooklynObjectType.ENRICHER, "enricher", "abcdefghij");
        addMemento(BrooklynObjectType.POLICY, "policy", "awmsgjxp8i");
        addMemento(BrooklynObjectType.ENTITY, "entity-with-entityspec-containing-policies", "aeifj99fjd");
        
        Predicate<ILoggingEvent> filter = EventPredicates.containsMessage("NOT SUPPORTED"); 
        try (LogWatcher watcher = new LogWatcher(EntitySpec.class.getName(), ch.qos.logback.classic.Level.WARN, filter)) {
            rebind(RebindOptions.create().exceptionHandler(exceptionHandler));
            watcher.assertHasEventEventually();
            
            Optional<ILoggingEvent> policiesWarning = Iterables.tryFind(watcher.getEvents(), EventPredicates.containsMessage("policies will be ignored"));
            Optional<ILoggingEvent> enrichersWarning = Iterables.tryFind(watcher.getEvents(), EventPredicates.containsMessage("enrichers will be ignored"));
            assertTrue(policiesWarning.isPresent());
            assertTrue(enrichersWarning.isPresent());
        }
    }

    // Check that a normal EntitySpec does not include "policies" or "enrichers" in its persisted state
    @Test
    public void testVanillaEntitySpec() throws Exception {
        TestApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class).impl(TestApplicationNoEnrichersImpl.class)
                .configure("myEntitySpec", EntitySpec.create(BasicEntity.class)));
        
        RebindTestUtils.waitForPersisted(app);
        String memento = readMemento(BrooklynObjectType.ENTITY, app.getId());
        assertFalse(memento.contains("<policies"));
        assertFalse(memento.contains("<enrichers"));
        
        rebind();
    }
}
