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
package org.apache.brooklyn.core.feed;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle.Transition;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 * Handler for when polling an entity's attribute. On each poll result the entity's attribute is set.
 * 
 * Calls to onSuccess and onError will happen sequentially, but may be called from different threads 
 * each time. Note that no guarantees of a synchronized block exist, so additional synchronization 
 * would be required for the Java memory model's "happens before" relationship.
 * 
 * @author aled
 */
public class AttributePollHandler<V> implements PollHandler<V> {

    public static final Logger log = LoggerFactory.getLogger(AttributePollHandler.class);

    private final FeedConfig<V,?,?> config;
    private final Entity entity;
    @SuppressWarnings("rawtypes")
    private final AttributeSensor sensor;
    private final AbstractFeed feed;
    private final boolean suppressDuplicates;
    
    // internal state to look after whether to log warnings
    private volatile Long lastSuccessTime = null;
    private volatile Long currentProblemStartTime = null;
    private volatile boolean currentProblemLoggedAsWarning = false;
    private volatile boolean lastWasProblem = false;

    
    public AttributePollHandler(FeedConfig<V,?,?> config, Entity entity, AbstractFeed feed) {
        this.config = checkNotNull(config, "config");
        this.entity = checkNotNull(entity, "entity");
        this.sensor = checkNotNull(config.getSensor(), "sensor");
        this.feed = checkNotNull(feed, "feed");
        this.suppressDuplicates = config.getSupressDuplicates();
    }

    @Override
    public boolean checkSuccess(V val) {
        // Always true if no checkSuccess predicate was configured.
        return !config.hasCheckSuccessHandler() || config.getCheckSuccess().apply(val);
    }

    @Override
    public void onSuccess(V val) {
        if (log.isTraceEnabled()) log.trace("poll for "+getBriefDescription()+" got: "+val);
        setSensor(transformValueOnSuccess(val));
        
        if (lastWasProblem) {
            if (currentProblemLoggedAsWarning) { 
                log.info("Success (following previous problem) reading "+getBriefDescription());
            } else {
                log.debug("Success (following previous problem) reading "+getBriefDescription());
            }
            lastWasProblem = false;
            currentProblemStartTime = null;
            currentProblemLoggedAsWarning = false;
        }
        lastSuccessTime = System.currentTimeMillis();
    }

    /** allows post-processing, such as applying a success handler; 
     * default applies the onSuccess handler (which is recommended) */
    protected Object transformValueOnSuccess(V val) {
        return config.hasSuccessHandler() ? config.getOnSuccess().apply(val) : val;
    }

    @Override
    public void onFailure(V val) {
        if (!config.hasFailureHandler()) {
            onException(new Exception("checkSuccess of "+this+" for "+getBriefDescription()+" was false but poller has no failure handler"));
        } else {
            logProblem("failure", val);

            try {
                setSensor(config.hasFailureHandler() ? config.getOnFailure().apply(val) : val);
            } catch (Exception e) {
                if (feed.isConnected()) {
                    log.warn("Error computing " + getBriefDescription() + "; val=" + val+": "+ e, e);
                } else {
                    if (log.isDebugEnabled())
                        log.debug("Error computing " + getBriefDescription() + "; val=" + val + " (when inactive)", e);
                }
            }
        }
    }

    @Override
    public void onException(Exception exception) {
        if (!feed.isConnected()) {
            if (log.isTraceEnabled()) log.trace("Read of "+this+" in "+getBriefDescription()+" gave exception (while not connected or not yet connected): "+ exception);
        } else {
            logProblem("exception", exception);
        }

        if (config.hasExceptionHandler()) {
            try {
                setSensor( config.getOnException().apply(exception) );
            } catch (Exception e) {
                if (feed.isConnected()) {
                    if (!e.equals(exception)) {
                        log.warn("Exception handler for "+getBriefDescription()+" returned a different error than the original computation; original error was "+exception, exception);
                    }
                    log.debug("Exception handler failed computing "+getBriefDescription()+" (rethrowing): "+e);
                    throw Exceptions.propagate(e);
                } else {
                    if (log.isDebugEnabled()) log.debug("Unable to compute "+getBriefDescription()+"; exception="+exception+" (when inactive)", e);
                }
            }
        }
    }

    protected void logProblem(String type, Object val) {
        if (lastWasProblem && currentProblemLoggedAsWarning) {
            if (log.isTraceEnabled())
                log.trace("Recurring "+type+" reading "+this+" in "+getBriefDescription()+": "+val);
        } else {
            long nowTime = System.currentTimeMillis();
            // get a non-volatile value
            Long currentProblemStartTimeCache = currentProblemStartTime;
            long expiryTime = 
                    (lastSuccessTime!=null && !isTransitioningOrStopped()) 
                            ? lastSuccessTime+config.getLogWarningGraceTime().toMilliseconds() 
                            : (currentProblemStartTimeCache != null) 
                                    ? currentProblemStartTimeCache+config.getLogWarningGraceTimeOnStartup().toMilliseconds() 
                                    : nowTime+config.getLogWarningGraceTimeOnStartup().toMilliseconds();
            if (!lastWasProblem) {
                if (expiryTime <= nowTime) {
                    currentProblemLoggedAsWarning = true;
                    if (entity==null || Entities.isManagedActive(entity)) {
                        log.warn("Read of " + getBriefDescription() + " gave " + type + ": " + val);
                        if (log.isDebugEnabled() && val instanceof Throwable)
                            log.debug("Trace for "+type+" reading "+getBriefDescription()+": "+val, (Throwable)val);
                    } else {
                        log.debug("Read (unmanaged) of " + getBriefDescription() + " gave " + type + ": " + val);
                    }
                } else {
                    if (log.isDebugEnabled())
                        log.debug("Read of " + getBriefDescription() + " gave " + type + " (in grace period): " + val);
                }
                lastWasProblem = true;
                currentProblemStartTime = nowTime;
            } else {
                if (expiryTime <= nowTime) {
                    currentProblemLoggedAsWarning = true;
                    if (entity==null || Entities.isManagedActive(entity)) {
                        log.warn("Read of " + getBriefDescription() + " gave " + type +
                                " (grace period expired, occurring for " + Duration.millis(nowTime - currentProblemStartTimeCache) +
                                (config.hasExceptionHandler() ? "" : ", no exception handler set for sensor") +
                                ")" +
                                ": " + val);
                        if (log.isDebugEnabled() && val instanceof Throwable)
                            log.debug("Trace for " + type + " reading " + getBriefDescription() + ": " + val, (Throwable) val);
                    } else {
                        if (log.isDebugEnabled())
                            log.debug("Read (unmanaged) of " + getBriefDescription() + " gave " + type + " (grace period expired): " + val);
                    }
                } else {
                    if (log.isDebugEnabled()) 
                        log.debug("Recurring {} reading {} in {} (still in grace period): {}", new Object[] {type, this, getBriefDescription(), val});
                }
            }
        }
    }

    protected boolean isTransitioningOrStopped() {
        if (entity==null) return false;
        Transition expected = entity.getAttribute(Attributes.SERVICE_STATE_EXPECTED);
        if (expected==null) return false;
        return (expected.getState()==Lifecycle.STARTING || expected.getState()==Lifecycle.STOPPING || expected.getState()==Lifecycle.STOPPED);
    }

    @SuppressWarnings("unchecked")
    protected void setSensor(Object v) {
        if (Entities.isNoLongerManaged(entity)) {
            if (Tasks.isInterrupted()) return;
            log.warn(""+entity+" is not managed; feed "+this+" setting "+sensor+" to "+v+" at this time is not supported ("+Tasks.current()+")");
        }
        
        if (v == FeedConfig.UNCHANGED) {
            // nothing
        } else if (v == FeedConfig.REMOVE) {
            ((EntityInternal)entity).sensors().remove(sensor);
            feed.onRemoveSensor(sensor);
        } else if (sensor == FeedConfig.NO_SENSOR) {
            // nothing
        } else {
            Object coercedV = TypeCoercions.coerce(v, sensor.getType());
            if (suppressDuplicates && Objects.equal(coercedV, entity.getAttribute(sensor))) {
                // no change; nothing
            } else {
                entity.sensors().set(sensor, coercedV);
                feed.onPublishSensor(sensor, coercedV);
            }
        }
    }

    @Override
    public String toString() {
        return super.toString()+"["+getDescription()+"]";
    }
    
    @Override
    public String getDescription() {
        return
                // we used to show the sensor name, but it was redundant with info on the FeedConfig
                //sensor.getName() + " " /*+entity.getId()*/ +" <- " +
                "" + config;
    }
    
    protected String getBriefDescription() {
        return ""+entity+"->"+(sensor==FeedConfig.NO_SENSOR ? "(dynamic sensors)" : ""+sensor);
    }
        
}
