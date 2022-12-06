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
package org.apache.brooklyn.entity.stock;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.entity.trait.StartableMethods;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractStartableImpl extends AbstractEntity implements BasicStartable {

    private static final Logger log = LoggerFactory.getLogger(AbstractStartableImpl.class);

    @Override
    public void start(Collection<? extends Location> locations) {
        try {
            ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);

            // Opportunity to block startup until other dependent components are available
            Object val = config().get(START_LATCH);
            if (val != null) log.debug("{} finished waiting for start-latch; continuing...", this);

            addLocations(locations);
            locations = Locations.getLocationsCheckingAncestors(locations, this);
            log.info("Starting entity "+this+" at "+locations);

            doStart(locations);
            sensors().set(Attributes.SERVICE_UP, true);
        } finally {
            ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
        }
    }

    protected abstract void doStart(Collection<? extends Location> locations);

    @Override
    public void stop() {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPING);
        sensors().set(SERVICE_UP, false);
        try {
            doStop();
            ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPED);
        } catch (Exception e) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(e);
        }
    }

    protected abstract void doStop();

    @Override
    public void restart() {
        StartableMethods.restart(this);
    }

    @Override
    protected void initEnrichers() {
        super.initEnrichers();
        enrichers().add(ServiceStateLogic.newEnricherFromChildrenUp()
                .checkChildrenOnly()
                .requireUpChildren(QuorumCheck.QuorumChecks.all())
                .suppressDuplicates(true));
    }

}
