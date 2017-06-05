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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.annotation.EffectorParam;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LoopOverGroupMembersTestCaseImpl extends TargetableTestComponentImpl implements LoopOverGroupMembersTestCase {

    private static final Logger logger = LoggerFactory.getLogger(LoopOverGroupMembersTestCaseImpl.class);

    @Override
    public void start(@EffectorParam(name = "locations") Collection<? extends Location> locations) {
        // Let everyone know we're starting up (so that the GUI shows the correct icon).
        sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);

        Maybe<Entity> target = tryResolveTarget();
        if (!target.isPresent()) {
            logger.debug("Tasks NOT successfully run. LoopOverGroupMembersTestCaseImpl group not set");
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            return;
        }

        if (!(target.get() instanceof Group)) {
            logger.debug("Tasks NOT successfully run. LoopOverGroupMembersTestCaseImpl target is not a group");
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            return;
        }

        EntitySpec<? extends TargetableTestComponent> testSpec = config().get(TEST_SPEC);
        if (testSpec == null) {
            logger.debug("Tasks NOT successfully run. LoopOverGroupMembersTestCaseImpl test spec not set");
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            return;
        }

        // Create the child-assertions (one per group-member)
        Group group = (Group) target.get();
        Collection<Entity> members = group.getMembers();
        List<Throwable> exceptions = new ArrayList<>();
        boolean allSuccesful = true;
        for (Entity member : members) {
            EntitySpec<? extends TargetableTestComponent> testSpecCopy = EntitySpec.create(testSpec)
                    .configure(TestCase.TARGET_ENTITY, member);

            try {
                TargetableTestComponent targetableTestComponent = this.addChild(testSpecCopy);
                targetableTestComponent.start(locations);
                if (Lifecycle.RUNNING.equals(targetableTestComponent.sensors().get(Attributes.SERVICE_STATE_ACTUAL))) {
                    logger.debug("Task of {} successfully run, targetting {}", this, member);
                } else {
                    logger.warn("Problem in child test-case of {}, targetting {}", this, member);
                    allSuccesful = false;
                }
            } catch (Throwable t) {
                Exceptions.propagateIfFatal(t);
                logger.warn("Problem in child test-case of "+this+", targetting "+member, t);
                exceptions.add(t);
                allSuccesful = false;
            }
        }

        if (allSuccesful) {
            // Let everyone know we've started up successfully (changes the icon in the GUI).
            logger.debug("Tasks successfully run. Update state of {} to RUNNING.", this);
            setUpAndRunState(true, Lifecycle.RUNNING);
        } else {
            // Let everyone know we've not started up successfully (changes the icon in the GUI).
            // Important to fail the start() method, so that parent TestCase entity also reports failure.
            logger.debug("Tasks NOT successfully run. Update state of {} to ON_FIRE.", this);
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            throw Exceptions.propagate("Test failed on group member(s)", exceptions);
        }

    }

    @Override
    public void stop() {
        // Let everyone know we're stopping (so that the GUI shows the correct icon).
        sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);

        try {
            for (Entity child : this.getChildren()) {
                if (child instanceof Startable) ((Startable) child).stop();
            }

            // Let everyone know we've stopped successfully (changes the icon in the GUI).
            logger.debug("Tasks successfully run. Update state of {} to STOPPED.", this);
            setUpAndRunState(false, Lifecycle.STOPPED);
        } catch (Throwable t) {
            logger.debug("Tasks NOT successfully run. Update state of {} to ON_FIRE.", this);
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }

    @Override
    public void restart() {
        final Collection<Location> locations = Lists.newArrayList(getLocations());
        stop();
        start(locations);
    }
}
