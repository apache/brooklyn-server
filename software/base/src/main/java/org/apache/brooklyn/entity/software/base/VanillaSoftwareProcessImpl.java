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
package org.apache.brooklyn.entity.software.base;

import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.util.guava.Maybe;

public class VanillaSoftwareProcessImpl extends SoftwareProcessImpl implements VanillaSoftwareProcess {
    
    @Override
    public Class<?> getDriverInterface() {
        return VanillaSoftwareProcessDriver.class;
    }

    @Override
    protected void connectSensors() {
        super.connectSensors();
        if (isSshMonitoringEnabled()) {
            connectServiceUpIsRunning();
        } else {
            // See SoftwareProcessImpl.waitForEntityStart(). We will already have waited for driver.isRunning.
            // We will not poll for that again.
            // 
            // Also disable the associated enricher - otherwise that would reset the not-up-indicator 
            // if serviceUp=false temporarily (e.g. if restart effector is called).
            // See https://issues.apache.org/jira/browse/BROOKLYN-547
            Maybe<Enricher> enricher = EntityAdjuncts.tryFindWithUniqueTag(enrichers(), "service-process-is-running-updating-not-up");
            if (enricher.isPresent()) {
                enrichers().remove(enricher.get());
            }
            ServiceNotUpLogic.clearNotUpIndicator(this, SERVICE_PROCESS_IS_RUNNING);
        }
    }
    
    @Override
    protected void disconnectSensors() {
        disconnectServiceUpIsRunning();
        super.disconnectSensors();
    }
    
    protected boolean isSshMonitoringEnabled() {
        return Boolean.TRUE.equals(getConfig(USE_SSH_MONITORING));
    }
}