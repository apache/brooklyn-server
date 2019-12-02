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

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * {@inheritDoc}
 */
public class TestCaseImpl extends TargetableTestComponentImpl implements TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestCaseImpl.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(Collection<? extends Location> locations) {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
        try {
            boolean continueOnFailure = Boolean.TRUE.equals(config().get(CONTINUE_ON_FAILURE));
            List<Throwable> childErrors = Lists.newArrayList();
            for (Entity child : getChildren()) {
                Boolean serviceUp = child.sensors().get(Attributes.SERVICE_UP);
                if (child instanceof Startable && !Boolean.TRUE.equals(serviceUp)){
                    try {
                        ((Startable) child).start(locations);
                    } catch (Throwable t) {
                        Exceptions.propagateIfFatal(t);
                        if (continueOnFailure) {
                            LOG.warn("Problem starting child "+child+" (continuing, and will throw at end)", t);
                            childErrors.add(t);
                        } else {
                            throw t;
                        }
                    }
                }
            }
            
            if (childErrors.size() > 0) {
                throw Exceptions.propagate(childErrors);
            }
            
            sensors().set(Attributes.SERVICE_UP, true);
            ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
            
        } catch (Throwable t) {
            Exceptions.propagateIfInterrupt(t);
            try {
                execOnSpec(ON_ERROR_SPEC);
            } catch (Throwable t2) {
                LOG.error("Problem executing on-error for "+this, t2);
                Exceptions.propagateIfInterrupt(t2);
            }
            sensors().set(Attributes.SERVICE_UP, false);
            ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        } finally {
            execOnSpec(ON_FINALLY_SPEC);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPING);
        sensors().set(Attributes.SERVICE_UP, false);
        try {
            boolean continueOnFailure = Boolean.TRUE.equals(config().get(CONTINUE_ON_FAILURE));
            List<Throwable> childErrors = Lists.newArrayList();
            for (Entity child : getChildren()) {
                try {
                    if (child instanceof Startable) ((Startable) child).stop();
                } catch (Throwable t) {
                    Exceptions.propagateIfFatal(t);
                    if (continueOnFailure) {
                        LOG.warn("Problem stopping child "+child+" (continuing, and will throw at end)", t);
                        childErrors.add(t);
                    } else {
                        throw t;
                    }
                }
            }
            
            if (childErrors.size() > 0) {
                throw Exceptions.propagate(childErrors);
            }

            ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPED);
            
        } catch (Exception e) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(e);
        }
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

    protected void execOnSpec(ConfigKey<EntitySpec<?>> configKey) {
        EntitySpec<?> spec = config().get(configKey);
        if (spec != null) {
            LOG.info("Creating and starting {} child entity {} for {}", configKey.getName(), spec.getType().getSimpleName(), this);
            Entity onEntity = addChild(spec);
            if (onEntity instanceof Startable){
                ((Startable) onEntity).start(getLocations());
            }
        } else {
            LOG.debug("No {} for {}", configKey.getName(), this);
        }
    }
}
