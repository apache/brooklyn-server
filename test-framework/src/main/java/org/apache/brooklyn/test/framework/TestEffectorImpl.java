/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.getAssertions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.test.framework.TestFrameworkAssertions.AssertionOptions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TestEffectorImpl extends TargetableTestComponentImpl implements TestEffector {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(TestEffectorImpl.class);


    /**
     * {@inheritDoc}
     */
    public void start(Collection<? extends Location> locations) {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
        try {
            final Entity targetEntity = resolveTarget();
            final String effectorName = getRequiredConfig(EFFECTOR_NAME);
            final Map<String, ?> effectorParams = getConfig(EFFECTOR_PARAMS);
            final Duration timeout = getConfig(TIMEOUT);
            if (!getChildren().isEmpty()) {
                throw new RuntimeException(String.format("The entity [%s] cannot have child entities", getClass().getName()));
            }
            
            Maybe<Effector<?>> effector = EffectorUtils.findEffectorDeclared(targetEntity, effectorName);
            if (effector.isAbsentOrNull()) {
                throw new AssertionError(String.format("No effector with name [%s]", effectorName));
            }
            final Task<?> effectorTask;
            if (effectorParams == null || effectorParams.isEmpty()) {
                effectorTask = Entities.invokeEffector(this, targetEntity, effector.get());
            } else {
                effectorTask = Entities.invokeEffector(this, targetEntity, effector.get(), effectorParams);
            }
            
            final Object effectorResult;
            try {
                effectorResult = effectorTask.get(timeout);
            } catch (TimeoutException e) {
                effectorTask.cancel(true);
                throw new AssertionError("Effector "+effectorName+" timed out after "+timeout, e);
            }

            final List<Map<String, Object>> assertions = getAssertions(this, ASSERTIONS);
            if(assertions != null && !assertions.isEmpty()){
                Supplier<?> supplier = Suppliers.ofInstance(effectorResult);
                TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions(effectorName, supplier)
                        .timeout(timeout).assertions(assertions));
            }

            //Add result of effector to sensor
            sensors().set(EFFECTOR_RESULT, effectorResult);
            setUpAndRunState(true, Lifecycle.RUNNING);
        } catch (Throwable t) {
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void stop() {
        setUpAndRunState(false, Lifecycle.STOPPED);
    }

    /**
     * {@inheritDoc}
     */
    public void restart() {
        final Collection<Location> locations = Lists.newArrayList(getLocations());
        stop();
        start(locations);
    }
}
