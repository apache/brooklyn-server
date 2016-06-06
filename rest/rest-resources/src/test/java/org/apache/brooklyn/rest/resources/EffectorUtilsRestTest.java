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

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.core.mgmt.internal.TestEntityWithEffectors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.transform.TaskTransformer;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EffectorUtilsRestTest extends BrooklynAppUnitTestSupport {
    private static final Logger log = LoggerFactory.getLogger(EffectorUtils.class);

    private TestEntityWithEffectors entity;

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
        Maybe<Effector<?>> effector = EffectorUtils.findEffectorDeclared(entity, "resetUserPassword");

        final String sensitiveField = "newPassword";
        Task<?> t = entity.invoke(effector.get(), ImmutableMap.of(sensitiveField, "#$%'332985$23$#\"sd'"));

        UriBuilder uriBuilder = new org.apache.cxf.jaxrs.impl.UriBuilderImpl(URI.create("http://localhost"));
        TaskSummary summary = TaskTransformer.taskSummary(t, uriBuilder);

        try {
            t.get();
        } catch (InterruptedException|ExecutionException e) {
            throw Exceptions.propagate(e);
        }
        Assert.assertEquals(
                summary.getDescription(),
                "Invoking effector resetUserPassword on "+TestEntityWithEffectors.class.getSimpleName()+":"+entity.getId().substring(0,4)
                    +" with parameters {"+sensitiveField+"=xxxxxxxx}",
                "Task summary must hide sensitive parameters");
    }
}
