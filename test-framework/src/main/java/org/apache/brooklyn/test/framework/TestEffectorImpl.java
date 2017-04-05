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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.framework.TestFrameworkAssertions.AssertionOptions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestEffectorImpl extends TargetableTestComponentImpl implements TestEffector {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(TestEffectorImpl.class);


    /**
     * {@inheritDoc}
     */
    @Override
    public void start(Collection<? extends Location> locations) {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
        try {
            final Entity targetEntity = resolveTarget();
            final String effectorName = getRequiredConfig(EFFECTOR_NAME);
            final Map<String, ?> effectorParams = getConfig(EFFECTOR_PARAMS);
            final Duration timeout = getConfig(TIMEOUT);
            final Integer maxAttempts = getConfig(MAX_ATTEMPTS);
            final Duration backoffToPeriod = getConfig(BACKOFF_TO_PERIOD);
            if (!getChildren().isEmpty()) {
                throw new RuntimeException(String.format("The entity [%s] cannot have child entities", getClass().getName()));
            }
            
            Maybe<Effector<?>> effector = EffectorUtils.findEffectorDeclared(targetEntity, effectorName);
            if (effector.isAbsentOrNull()) {
                throw new AssertionError(String.format("No effector with name [%s]", effectorName));
            }
            
            Object effectorResult = invokeEffector(targetEntity, effector.get(), effectorParams, maxAttempts, timeout, backoffToPeriod);

            final List<Map<String, Object>> assertions = getAssertions(this, ASSERTIONS);
            if(assertions != null && !assertions.isEmpty()){
                Supplier<?> supplier = Suppliers.ofInstance(effectorResult);
                TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions(effectorName, supplier)
                        .maxAttempts(1)
                        .timeout(timeout)
                        .assertions(assertions));
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
    @Override
    public void stop() {
        setUpAndRunState(false, Lifecycle.STOPPED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restart() {
        final Collection<Location> locations = Lists.newArrayList(getLocations());
        stop();
        start(locations);
    }
    
    /**
     * Invokes the effector. On failure, it repeats the invocation up to a maximum of 
     * {@code maxAttempts} times (defaulting to one attempt if that parameter is null).
     * 
     * The total invocation time is capped at {@code timeout} (if multiple attempts are
     * permitted, this is the total time for all attempts).
     * 
     * @throws AssertionError if the invocation times out
     * @throws ExecutionException if the last invocation attempt fails
     */
    protected Object invokeEffector(final Entity targetEntity, final Effector<?> effector, final Map<String, ?> effectorParams, 
            Integer maxAttempts, final Duration timeout, Duration backoffToPeriod) {
        
        Duration timeLimit = (timeout != null) ? timeout : (maxAttempts == null ? Asserts.DEFAULT_LONG_TIMEOUT : Duration.PRACTICALLY_FOREVER);
        int iterationLimit = (maxAttempts != null) ? maxAttempts : 1;
        final Long startTime = System.currentTimeMillis();
        final AtomicReference<Object> effectorResult = new AtomicReference<Object>();
        
        Repeater.create()
                .until(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws ExecutionException {
                        try {
                            long timeRemaining = Time.timeRemaining(startTime, timeout.toMilliseconds());
                            Object result = invokeEffector(targetEntity, effector, effectorParams, Duration.millis(timeRemaining));
                            effectorResult.set(result);
                            return true;
                        } catch (TimeoutException e) {
                            throw new AssertionError("Effector "+effector.getName()+" timed out after "+timeout, e);
                        }
                    }})
                .limitIterationsTo(iterationLimit)
                .limitTimeTo(timeLimit)
                .backoffTo(backoffToPeriod)
                .rethrowExceptionImmediately(Predicates.or(ImmutableList.of(
                        Predicates.instanceOf(TimeoutException.class), 
                        Predicates.instanceOf(AbortError.class), 
                        Predicates.instanceOf(InterruptedException.class), 
                        Predicates.instanceOf(RuntimeInterruptedException.class))))
                .runRequiringTrue();

        return effectorResult.get();
    }
    
    protected Object invokeEffector(Entity targetEntity, Effector<?> effector, Map<String, ?> effectorParams, Duration timeout) throws ExecutionException, TimeoutException {
        Task<?> task;
        if (effectorParams == null || effectorParams.isEmpty()) {
            task = Entities.invokeEffector(TestEffectorImpl.this, targetEntity, effector);
        } else {
            task = Entities.invokeEffector(TestEffectorImpl.this, targetEntity, effector, effectorParams);
        }
        
        try {
            return task.get(timeout);
        } catch (InterruptedException e) {
            task.cancel(true);
            throw Exceptions.propagate(e);
        } catch (TimeoutException e) {
            task.cancel(true);
            throw e;
        }
    }
}
