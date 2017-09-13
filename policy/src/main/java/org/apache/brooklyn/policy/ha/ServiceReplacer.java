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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceProblemsLogic;
import org.apache.brooklyn.core.entity.trait.MemberReplaceable;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.BasicNotificationSensor;
import org.apache.brooklyn.entity.group.StopFailedRuntimeException;
import org.apache.brooklyn.policy.ha.HASensors.FailureDescriptor;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Ticker;
import com.google.common.collect.Lists;

/** attaches to a DynamicCluster and replaces a failed member in response to HASensors.ENTITY_FAILED or other sensor;
 * if this fails, it sets the Cluster state to on-fire */
@Catalog(name="Service Replacer", description="HA policy for replacing a failed member of a group")
public class ServiceReplacer extends AbstractPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceReplacer.class);

    // TODO if there are multiple failures perhaps we should abort quickly
    
    public static final BasicNotificationSensor<FailureDescriptor> ENTITY_REPLACEMENT_FAILED = new BasicNotificationSensor<FailureDescriptor>(
            FailureDescriptor.class, "ha.entityFailed.replacement", "Indicates that an entity replacement attempt has failed");

    @SetFromFlag("setOnFireOnFailure")
    public static final ConfigKey<Boolean> SET_ON_FIRE_ON_FAILURE = ConfigKeys.newBooleanConfigKey("setOnFireOnFailure", "", true);
    
    /** monitors this sensor, by default ENTITY_RESTART_FAILED */
    @SetFromFlag("failureSensorToMonitor")
    @SuppressWarnings("rawtypes")
    public static final ConfigKey<Sensor> FAILURE_SENSOR_TO_MONITOR = new BasicConfigKey<Sensor>(Sensor.class, "failureSensorToMonitor", "", ServiceRestarter.ENTITY_RESTART_FAILED); 

    /** skips replace if replacement has failed this many times failure re-occurs within this time interval */
    @SetFromFlag("failOnRecurringFailuresInThisDuration")
    public static final ConfigKey<Long> FAIL_ON_RECURRING_FAILURES_IN_THIS_DURATION = ConfigKeys.newLongConfigKey(
            "failOnRecurringFailuresInThisDuration", 
            "abandon replace if replacement has failed many times within this time interval",
            5*60*1000L);

    /** skips replace if replacement has failed this many times failure re-occurs within this time interval */
    @SetFromFlag("failOnNumRecurringFailures")
    public static final ConfigKey<Integer> FAIL_ON_NUM_RECURRING_FAILURES = ConfigKeys.newIntegerConfigKey(
            "failOnNumRecurringFailures", 
            "abandon replace if replacement has failed this many times (100% of attempts) within the time interval",
            5);

    @SetFromFlag("ticker")
    public static final ConfigKey<Ticker> TICKER = ConfigKeys.newConfigKey(Ticker.class,
            "ticker", 
            "A time source (defaults to system-clock, which is almost certainly what's wanted, except in tests)",
            null);

    protected final List<Long> consecutiveReplacementFailureTimes = Lists.newCopyOnWriteArrayList();
    
    public ServiceReplacer() {
    }
    
    @Override
    public void setEntity(final EntityLocal entity) {
        checkArgument(entity instanceof MemberReplaceable, "ServiceReplacer must take a MemberReplaceable, not %s", entity);
        Sensor<?> failureSensorToMonitor = checkNotNull(getConfig(FAILURE_SENSOR_TO_MONITOR), "failureSensorToMonitor");
        
        super.setEntity(entity);

        subscriptions().subscribeToMembers((Group)entity, failureSensorToMonitor, new SensorEventListener<Object>() {
                @Override public void onEvent(final SensorEvent<Object> event) {
                    // Must execute in another thread - if we called entity.replaceMember in the event-listener's thread
                    // then we'd block all other events being delivered to this entity's other subscribers.
                    // Relies on synchronization of `onDetectedFailure`.
                    // See same pattern used in ServiceRestarter.
                    
                    // TODO Could use BasicExecutionManager.setTaskSchedulerForTag to prevent race of two
                    // events being received in rapid succession, and onDetectedFailure being executed out-of-order
                    // for them; or could write events to a blocking queue and have onDetectedFailure read from that.
                    
                    if (isRunning()) {
                        highlightViolation("Failure detected");
                        LOG.warn("ServiceReplacer notified; dispatching job for "+entity+" ("+event.getValue()+")");
                        ((EntityInternal)entity).getExecutionContext().submit(MutableMap.of(), new Runnable() {
                            @Override public void run() {
                                onDetectedFailure(event);
                            }});
                    } else {
                        LOG.warn("ServiceReplacer not running, so not acting on failure detected at "+entity+" ("+event.getValue()+", child of "+entity+")");
                    }
                }
            });
        highlightTriggers(failureSensorToMonitor, "members");
    }
    
    // TODO semaphores would be better to allow at-most-one-blocking behaviour
    protected synchronized void onDetectedFailure(SensorEvent<Object> event) {
        final Entity failedEntity = event.getSource();
        final Object reason = event.getValue();
        String violationText = "Failure detected at "+failedEntity+(reason!=null ? " ("+reason+")" : "");
        
        if (isSuspended()) {
            highlightViolation(violationText+" but policy is suspended");
            LOG.warn("ServiceReplacer suspended, so not acting on failure detected at "+failedEntity+" ("+reason+", child of "+entity+")");
            return;
        }


        Integer failOnNumRecurringFailures = getConfig(FAIL_ON_NUM_RECURRING_FAILURES);
        long failOnRecurringFailuresInThisDuration = getConfig(FAIL_ON_RECURRING_FAILURES_IN_THIS_DURATION);
        long oldestPermitted = currentTimeMillis() - failOnRecurringFailuresInThisDuration;
        // trim old ones
        for (Iterator<Long> iter = consecutiveReplacementFailureTimes.iterator(); iter.hasNext();) {
            Long timestamp = iter.next();
            if (timestamp < oldestPermitted) {
                iter.remove();
            } else {
                break;
            }
        }
        
        if (consecutiveReplacementFailureTimes.size() >= failOnNumRecurringFailures) {
            highlightViolation(violationText+" but too many recent failures detected: "
                + consecutiveReplacementFailureTimes.size()+" in "+failOnRecurringFailuresInThisDuration+" exceeds limit of "+failOnNumRecurringFailures);
            LOG.error("ServiceReplacer not acting on failure detected at "+failedEntity+" ("+reason+", child of "+entity+"), because too many recent replacement failures");
            return;
        }
        
        highlightViolation(violationText+", triggering restart");
        LOG.warn("ServiceReplacer acting on failure detected at "+failedEntity+" ("+reason+", child of "+entity+")");
        Task<?> t = ((EntityInternal)entity).getExecutionContext().submit(MutableMap.of(), new Runnable() {

            @Override
            public void run() {
                try {
                    Entities.invokeEffectorWithArgs(entity, entity, MemberReplaceable.REPLACE_MEMBER, failedEntity.getId()).get();
                    consecutiveReplacementFailureTimes.clear();
                } catch (Exception e) {
                    if (Exceptions.getFirstThrowableOfType(e, StopFailedRuntimeException.class) != null) {
                        LOG.info("ServiceReplacer: ignoring error reported from stopping failed node "+failedEntity);
                        return;
                    }
                    highlightViolation(violationText+" and replace attempt failed: "+Exceptions.collapseText(e));
                    onReplacementFailed("Replace failure ("+Exceptions.collapseText(e)+") at "+entity+": "+reason);
                }
            }
        });
        highlightAction("Replacing "+failedEntity, t);
    }

    protected long currentTimeMillis() {
        Ticker ticker = getConfig(TICKER);
        return (ticker == null) ? System.currentTimeMillis() : TimeUnit.NANOSECONDS.toMillis(ticker.read());
    }
    
    protected void onReplacementFailed(String msg) {
        LOG.warn("ServiceReplacer failed for "+entity+": "+msg);
        consecutiveReplacementFailureTimes.add(currentTimeMillis());
        
        if (getConfig(SET_ON_FIRE_ON_FAILURE)) {
            ServiceProblemsLogic.updateProblemsIndicator(entity, "ServiceReplacer", "replacement failed: "+msg);
        }
        entity.sensors().emit(ENTITY_REPLACEMENT_FAILED, new FailureDescriptor(entity, msg));
    }
}
