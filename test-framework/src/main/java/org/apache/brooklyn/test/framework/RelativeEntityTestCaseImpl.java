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

package org.apache.brooklyn.test.framework;

import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class RelativeEntityTestCaseImpl extends TargetableTestComponentImpl implements RelativeEntityTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(RelativeEntityTestCaseImpl.class);

    @Override
    public Entity resolveTarget() {
        Entity anchor = config().get(ANCHOR);
        if (anchor == null) {
            Maybe<Entity> resolvedTarget = tryResolveTarget();
            if (resolvedTarget.isPresent()) {
                anchor = resolvedTarget.get();
            } else {
                throw new IllegalArgumentException("No anchor entity found for " + this);
            }
        }
        sensors().set(ANCHOR, anchor);
        
        Maybe<Object> component = config().getRaw(COMPONENT);
        if (component.isAbsentOrNull()) {
            throw new IllegalArgumentException("No component found for " + this);
        } else if (!(component.get() instanceof DslComponent)) {
            throw new IllegalArgumentException("Expected DslComponent value for component, found " + component.get());
        }
        DslComponent<Entity> finder = DslComponent.class.cast(component.get());
        Task<Entity> task = Entities.submit(anchor, finder);
        return task.getUnchecked();
    }

    @Override
    public void start(Collection<? extends Location> locations) {
        sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);

        Entity target = resolveTarget();
        if (target == null) {
            LOG.debug("Tasks NOT successfully run. RelativeEntityTestCaseImpl target unset");
            setServiceState(false, Lifecycle.ON_FIRE);
            return;
        }
        config().set(BaseTest.TARGET_ENTITY, target);

        boolean success = true;
        try {
            for (Entity child : getChildren()) {
                if (child instanceof Startable) {
                    Startable test = Startable.class.cast(child);
                    test.start(locations);
                    if (Lifecycle.RUNNING.equals(child.sensors().get(Attributes.SERVICE_STATE_ACTUAL))) {
                        LOG.debug("Task of {} successfully run, targeting {}", this, target);
                    } else {
                        LOG.warn("Problem in child test-case of {}, targeting {}", this, target);
                        success = false;
                    }
                } else {
                    LOG.info("Ignored child of {} that is not Startable: {}", this, child);
                }
                if (!success) {
                    break;
                }
            }
        } catch (Throwable t) {
            Exceptions.propagateIfFatal(t);
            LOG.warn("Problem in child test-case of " + this + ", targeting " + target, t);
            success = false;
        }

        if (success) {
            LOG.debug("Tasks successfully run. Update state of {} to RUNNING.", this);
            setServiceState(true, Lifecycle.RUNNING);
        } else {
            LOG.debug("Tasks NOT successfully run. Update state of {} to ON_FIRE.", this);
            setServiceState(false, Lifecycle.ON_FIRE);
        }
    }

    @Override
    public void stop() {
        sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
        try {
            for (Entity child : this.getChildren()) {
                if (child instanceof Startable) ((Startable) child).stop();
            }
            LOG.debug("Tasks successfully run. Update state of {} to STOPPED.", this);
            setServiceState(false, Lifecycle.STOPPED);
        } catch (Throwable t) {
            LOG.debug("Tasks NOT successfully run. Update state of {} to ON_FIRE.", this);
            setServiceState(false, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }

    @Override
    public void restart() {
        final Collection<Location> locations = Lists.newArrayList(getLocations());
        stop();
        start(locations);
    }

    /**
     * Sets the state of the Entity. Useful so that the GUI shows the correct icon.
     *
     * @param serviceUpState     Whether or not the entity is up.
     * @param serviceStateActual The actual state of the entity.
     */
    private void setServiceState(final boolean serviceUpState, final Lifecycle serviceStateActual) {
        sensors().set(SERVICE_UP, serviceUpState);
        sensors().set(Attributes.SERVICE_STATE_ACTUAL, serviceStateActual);
    }

}
