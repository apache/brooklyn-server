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
package org.apache.brooklyn.policy.ha;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceProblemsLogic;
import org.apache.brooklyn.core.sensor.Sensors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Records if the elected primary child/member is running, updating service state of this entity 
 * if it isn't running when it should be via the service problems map which in the event of
 * an issue will contain more information about primary status. */ 
@SuppressWarnings("rawtypes")
public class PrimaryRunningEnricher extends AbstractEnricher implements SensorEventListener {

    private static final Logger log = LoggerFactory.getLogger(PrimaryRunningEnricher.class);
    
    public static final ConfigKey<String> PRIMARY_SENSOR_NAME = ElectPrimaryConfig.PRIMARY_SENSOR_NAME;
    
    @SuppressWarnings("unchecked")
    public void setEntity(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity) {
        super.setEntity(entity);
        subscriptions().subscribe(entity, Sensors.newSensor(Entity.class, config().get(PRIMARY_SENSOR_NAME)), this);
        subscriptions().subscribeToChildren(entity, Attributes.SERVICE_UP, this);
        subscriptions().subscribeToChildren(entity, Attributes.SERVICE_STATE_ACTUAL, this);
        highlightTriggers("Listening for "+config().get(PRIMARY_SENSOR_NAME)+" locally and service up and state at children");
        onEvent(null);
    }

    @Override
    public void onEvent(SensorEvent event) {
        Entity primary = entity.getAttribute( Sensors.newSensor(Entity.class, config().get(PRIMARY_SENSOR_NAME)) );
        if (primary==null) {
            ServiceNotUpLogic.updateNotUpIndicator(entity, "primary.enricher", "no primary found");
            ServiceProblemsLogic.updateProblemsIndicator(entity, "primary.enricher", "no primary found");
        } else if (Lifecycle.RUNNING.equals(primary.getAttribute(Attributes.SERVICE_STATE_ACTUAL)) &&
                Boolean.TRUE.equals(primary.getAttribute(Attributes.SERVICE_UP))) {
            if (ServiceStateLogic.getMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, "primary.enricher")!=null) {
                log.info("Primary "+primary+" at "+entity+" detected as healthy");
                ServiceProblemsLogic.clearProblemsIndicator(entity, "primary.enricher");
                ServiceNotUpLogic.clearNotUpIndicator(entity, "primary.enricher");
            }
        } else {
            log.warn("Primary "+primary+" at "+entity+" detected as down or otherwise unhealthy");
            ServiceProblemsLogic.updateProblemsIndicator(entity, "primary.enricher", "Primary "+primary+" not in healthy state");
        }
    }

}
