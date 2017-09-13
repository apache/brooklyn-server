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
package org.apache.brooklyn.core.policy;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;

import groovy.lang.Closure;

@SuppressWarnings({"rawtypes","unchecked"})
public class Policies {

    private static final Logger LOG = LoggerFactory.getLogger(Policies.class);

    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static SensorEventListener listenerFromValueClosure(final Closure code) {
        LOG.warn("Use of groovy.lang.Closure is deprecated in Policies.listenerFromValueClosure()");
        return new SensorEventListener() {
            @Override
            public void onEvent(SensorEvent event) {
                code.call(event.getValue());
            }
        };
    }
    
    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <T> Policy newSingleSensorValuePolicy(final Sensor<T> sensor, final Closure code) {
        LOG.warn("Use of groovy.lang.Closure is deprecated in Policies.newSingleSensorValuePolicy()");
        return new AbstractPolicy() {
            @Override
            public void setEntity(EntityLocal entity) {
                super.setEntity(entity);
                entity.subscriptions().subscribe(entity, sensor, listenerFromValueClosure(code));
            }
        };
    }
    
    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <S,T> Policy newSingleSensorValuePolicy(final Entity remoteEntity, final Sensor<T> remoteSensor, 
            final Closure code) {
        LOG.warn("Use of groovy.lang.Closure is deprecated in Policies.newSingleSensorValuePolicy()");
        return new AbstractPolicy() {
            @Override
            public void setEntity(EntityLocal entity) {
                super.setEntity(entity);
                entity.subscriptions().subscribe(remoteEntity, remoteSensor, listenerFromValueClosure(code));
            }
        };
    }

    public static Lifecycle getPolicyStatus(Policy p) {
        return inferAdjunctStatus(p);
    }
    
    @Beta
    public static Lifecycle inferAdjunctStatus(EntityAdjunct a) {
        if (a.isRunning()) return Lifecycle.RUNNING;
        if (a.isDestroyed()) return Lifecycle.DESTROYED;
        
        // adjuncts don't currently support an "error" state; though that would be useful!
        
        if (a instanceof Policy) {
            if (((Policy)a).isSuspended()) return Lifecycle.STOPPED;
            return Lifecycle.CREATED;
        }
        if (a instanceof Feed) {
            if (((Feed)a).isSuspended()) return Lifecycle.STOPPED;
            if (((Feed)a).isActivated()) return Lifecycle.STARTING;
            return Lifecycle.CREATED;
        }
        
        return Lifecycle.STOPPED;
    }
    
}
