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
package org.apache.brooklyn.rest.resources;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.core.mgmt.internal.TestEntityWithEffectors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.rest.domain.EffectorSummary;
import org.apache.brooklyn.rest.domain.SummaryComparators;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.transform.EffectorTransformer;
import org.apache.brooklyn.rest.transform.TaskTransformer;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class EffectorUtilsRestTest extends BrooklynAppUnitTestSupport {
    private static final Logger log = LoggerFactory.getLogger(EffectorUtils.class);

    private TestEntityWithEffectors entity;
    private UriBuilder uriBuilder = new org.apache.cxf.jaxrs.impl.UriBuilderImpl(URI.create("http://localhost"));

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.createAndManageChild(EntitySpec.create(TestEntityWithEffectors.class));
    }

    /**
     * Invoking an effector with sensitive parameters.
     * Invocation imitates {@link EffectorResource#invoke(String, String, String, String, Map)}
     */
    @Test
    public void testInvokingEffector() {
        Maybe<Effector<?>> effector = EffectorUtils.findEffectorDeclared(entity, "resetPassword");

        final String sensitiveField1 = "newPassword";
        final String sensitiveField2 = "secretPin";
        Task<?> t = entity.invoke(effector.get(), ImmutableMap.of(sensitiveField1, "#$%'332985$23$#\"sd'", "secretPin", 1234));

        TaskSummary summary = TaskTransformer.taskSummary(t, uriBuilder);

        try {
            t.get();
        } catch (InterruptedException|ExecutionException e) {
            throw Exceptions.propagate(e);
        }
        log.debug("Result description: "+summary.getDescription());
        assertEquals(
                summary.getDescription(),
                "Invoking effector resetPassword on "+TestEntityWithEffectors.class.getSimpleName()+":"+entity.getId().substring(0,4)
                    +" with parameters {"+sensitiveField1+"=xxxxxxxx, "+sensitiveField2+"=xxxxxxxx}",
                "Task summary must hide sensitive parameters");
    }

    /**
     * Invoking an effector with sensitive parameters.
     * Invocation imitates {@link EffectorResource#list}
     */
    @Test
    public void testListingEffectorsParameterSummary() {
        List<EffectorSummary> effectorSummaries = FluentIterable
                .from(entity.getEntityType().getEffectors())
                .filter(new Predicate<Effector<?>>() {
                    @Override
                    public boolean apply(@Nullable Effector<?> input) {
                        return ImmutableList.of("resetPassword", "invokeUserAndPassword").contains(input.getName());
                    }
                })
                .transform(new Function<Effector<?>, EffectorSummary>() {
                    @Override
                    public EffectorSummary apply(Effector<?> effector) {
                        return EffectorTransformer.effectorSummary(entity, effector, uriBuilder);
                    }
                })
                .toSortedList(SummaryComparators.nameComparator());
        EffectorSummary.ParameterSummary<?> passwordFieldInSimpleEffector = findEffectorSummary(effectorSummaries, "resetPassword").getParameters().iterator().next();
        assertTrue(passwordFieldInSimpleEffector.shouldSanitize());

        EffectorSummary longEffector = findEffectorSummary(effectorSummaries, "invokeUserAndPassword");
        EffectorSummary.ParameterSummary<?> resetUsersPassword_user = Iterables.getFirst(longEffector.getParameters(), null);
        assertFalse(resetUsersPassword_user.shouldSanitize());
        EffectorSummary.ParameterSummary<?> resetUsersPassword_password = longEffector.getParameters().toArray(new EffectorSummary.ParameterSummary<?>[0])[1];
        assertTrue(resetUsersPassword_password.shouldSanitize());
    }

    private EffectorSummary findEffectorSummary(List<EffectorSummary> effectorSummaries, final String effectorName) {
        return Iterables.find(effectorSummaries, new Predicate<EffectorSummary>() {
            @Override
            public boolean apply(@Nullable EffectorSummary input) {
                return input.getName().equals(effectorName);
            }
        });
    }
}
