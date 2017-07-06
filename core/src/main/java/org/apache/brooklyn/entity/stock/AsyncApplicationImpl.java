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

import static org.apache.brooklyn.core.entity.Attributes.SERVICE_STATE_ACTUAL;
import static org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.clearMapSensorEntry;
import static org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.updateMapSensorEntry;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractApplication;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceProblemsLogic;
import org.apache.brooklyn.enricher.stock.AbstractMultipleSensorAggregator;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

public class AsyncApplicationImpl extends AbstractApplication implements AsyncApplication {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncApplicationImpl.class);

    @Override
    public void init() {
        // Code below copied from BasicAppliationImpl.
        // Set the default name *before* calling super.init(), and only do so if we don't have an 
        // explicit default. This is a belt-and-braces fix: before we overwrote the defaultDisplayName
        // that was inferred from the catalog item name.
        if (Strings.isBlank(getConfig(DEFAULT_DISPLAY_NAME))) {
            setDefaultDisplayName("Application ("+getId()+")");
        }
        super.init();
    }
    
    @Override
    protected void initEnrichers() {
        // Deliberately not calling `super.initEnrichers()`. For our state (i.e. "service.state" 
        // and "service.isUp"), we rely on the `serviceStateComputer`. This keeps things a lot
        // simpler. However, it means that if someone manually sets a "service.notUp.indicators" 
        // or "service.problems" then that won't cause the entity to transition to false or ON_FIRE.
        
        enrichers().add(EnricherSpec.create(ServiceStateComputer.class)
                .configure(ServiceStateComputer.FROM_CHILDREN, true)
                .configure(ServiceStateComputer.UP_QUORUM_CHECK, config().get(UP_QUORUM_CHECK))
                .configure(ServiceStateComputer.RUNNING_QUORUM_CHECK, config().get(RUNNING_QUORUM_CHECK)));

    }

    // Only overrides AbstractApplication.start so as to disable the publishing of expected=running.
    // Code is copy-pasted verbatim from AbstractAppliation, except for not setting Lifecycle.RUNNING. 
    @Override
    public void start(Collection<? extends Location> locations) {
        this.addLocations(locations);
        // 2016-01: only pass locations passed to us, as per ML discussion
        Collection<? extends Location> locationsToUse = locations==null ? ImmutableSet.<Location>of() : locations;
        ServiceProblemsLogic.clearProblemsIndicator(this, START);
        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, Attributes.SERVICE_STATE_ACTUAL, "Application starting");
        ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(this, START.getName());
        setExpectedStateAndRecordLifecycleEvent(Lifecycle.STARTING);
        try {
            try {
                
                preStart(locationsToUse);
                
                // Opportunity to block startup until other dependent components are available
                Object val = config().get(START_LATCH);
                if (val != null) LOG.debug("{} finished waiting for start-latch; continuing...", this);
                
                doStart(locationsToUse);
                postStart(locationsToUse);
                
            } catch (ProblemStartingChildrenException e) {
                throw Exceptions.propagate(e);
            } catch (Exception e) {
                // should remember problems, apart from those that happened starting children
                // fixed bug introduced by the fix in dacf18b831e1e5e1383d662a873643a3c3cabac6
                // where failures in special code at application root don't cause app to go on fire 
                ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, START.getName(), Exceptions.collapseText(e));
                throw Exceptions.propagate(e);
            }
            
        } catch (Exception e) {
            recordApplicationEvent(Lifecycle.ON_FIRE);
            ServiceStateLogic.setExpectedStateRunningWithErrors(this);
            
            // no need to log here; the effector invocation should do that
            throw Exceptions.propagate(e);
            
        } finally {
            ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(this, Attributes.SERVICE_STATE_ACTUAL);
        }
        
        // CHANGE FROM SUPER: NOT CALLING THESE
        // ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
        // setExpectedStateAndRecordLifecycleEvent(Lifecycle.RUNNING);
        //
        // logApplicationLifecycle("Started");
    }

    /**
     * Calculates the "service.state" and "service.isUp", based on the state of the children. It 
     * also transitions from expected=starting to expected=running once all of the children have
     * finished their starting.
     * 
     * This does <em>not</em> just rely on the "service.problems" and "service.notUp.indicators"
     * because those make different assumptions about the expected state. Instead it seems much
     * easier to implement the specific logic for async startup here.
     * 
     * The important part of the implementation is {@link #onUpdated()}, and its helper methods
     * for {@link #computeServiceUp(Lifecycle)}, {@link #computeServiceState(Lifecycle)} and
     * {@link #computeExpectedState(Lifecycle, Lifecycle)}.
     * 
     * This class is not to be instantiated directly. Instead, if cusotmization is desired then override 
     * {@link AsyncApplicationImpl#initEnrichers()} to create and add this enricher (with the same unique 
     * tag, to replace the default).
     */
    public static class ServiceStateComputer extends AbstractMultipleSensorAggregator<Void> implements SensorEventListener<Object> {
        /** standard unique tag identifying instances of this enricher at runtime */
        public final static String DEFAULT_UNIQUE_TAG = "async-service-state-computer";

        public static final ConfigKey<QuorumCheck> RUNNING_QUORUM_CHECK = ConfigKeys.builder(QuorumCheck.class, "runningQuorumCheck")
                .description("Logic for checking whether this service is running, based on children and/or "
                        + "members running (by default requires all, but ignores any that are stopping)")
                .defaultValue(QuorumCheck.QuorumChecks.all())
                .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
                .build();

        public static final ConfigKey<QuorumCheck> UP_QUORUM_CHECK = ConfigKeys.builder(QuorumCheck.class, "upQuorumCheck")
              .description("Logic for checking whether this service is up, based on children and/or members (by default requires all)")
              .defaultValue(QuorumCheck.QuorumChecks.all())
              .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
              .build();
        
        // TODO How does this relate to quorum?!
        @SuppressWarnings("serial")
        public static final ConfigKey<Set<Lifecycle>> ENTITY_FAILED_STATES = ConfigKeys.builder(new TypeToken<Set<Lifecycle>>() {})
                .name("entityFailedStates")
                .description("Service states that indicate a child/member has failed (by default just ON_FIRE will mean not healthy)")
                .defaultValue(ImmutableSet.of(Lifecycle.ON_FIRE))
                .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
                .build();

        @SuppressWarnings("serial")
        public static final ConfigKey<Set<Lifecycle>> ENTITY_TRANSITION_STATES_ON_STARTING = ConfigKeys.builder(new TypeToken<Set<Lifecycle>>() {})
                .name("entityTransitionStatesOnStarting")
                .description("Service states which indicate a child/member is still starting "
                        + "(used to compute when we have finished starting)")
                .defaultValue(MutableSet.of(null, Lifecycle.CREATED, Lifecycle.STARTING).asUnmodifiable())
                .build();

        @SuppressWarnings("serial")
        public static final ConfigKey<Set<Lifecycle>> ENTITY_IGNORED_STATES_ON_STARTING = ConfigKeys.builder(new TypeToken<Set<Lifecycle>>() {})
                .name("entityIgnoredStatesOnStarting")
                .description("Service states of a child/member that mean we'll ignore it, for calculating "
                        + "our own state when 'staring' (by default ignores children that are stopping/stopped)")
                .defaultValue(ImmutableSet.of(Lifecycle.STOPPING, Lifecycle.STOPPED, Lifecycle.DESTROYED))
                .build();

        @SuppressWarnings("serial")
        public static final ConfigKey<Set<Lifecycle>> ENTITY_IGNORED_STATES_ON_OTHERS = ConfigKeys.builder(new TypeToken<Set<Lifecycle>>() {})
                .name("entityIgnoredStatesOnOthers")
                .description("Service states of a child/member that mean we'll ignore it, for calculating "
                        + "our own state when we are not 'staring' (by default ignores children that are starting/stopping)")
                .defaultValue(MutableSet.of(null, Lifecycle.STOPPING, Lifecycle.STOPPED, Lifecycle.DESTROYED, Lifecycle.CREATED, Lifecycle.STARTING).asUnmodifiable())
                .build();

        public static final ConfigKey<Boolean> IGNORE_ENTITIES_WITH_SERVICE_UP_NULL = ConfigKeys.builder(Boolean.class)
                .name("ignoreEntitiesWithServiceUpNull")
                .description("Whether to ignore children reporting null values for service up "
                        + "(i.e. don't treat them as 'down' when computing our own 'service.isUp')")
                .defaultValue(true)
                .build();

        static final Set<ConfigKey<?>> RECONFIGURABLE_KEYS = ImmutableSet.<ConfigKey<?>>of(
                UP_QUORUM_CHECK, RUNNING_QUORUM_CHECK,
                ENTITY_IGNORED_STATES_ON_STARTING, ENTITY_IGNORED_STATES_ON_OTHERS,
                ENTITY_FAILED_STATES, ENTITY_TRANSITION_STATES_ON_STARTING);

        static final List<Sensor<?>> SOURCE_SENSORS = ImmutableList.<Sensor<?>>of(SERVICE_UP, SERVICE_STATE_ACTUAL);

        @Override
        public AsyncApplicationImpl getEntity() {
            return (AsyncApplicationImpl) super.getEntity();
        }
        
        @Override
        protected void setEntityLoadingTargetConfig() {
            // ensure parent's behaviour never happens
            if (getConfig(TARGET_SENSOR)!=null)
                throw new IllegalArgumentException("Must not set "+TARGET_SENSOR+" when using "+this);
        }

        @Override
        public void setEntity(EntityLocal entity) {
            if (!(entity instanceof AsyncApplicationImpl)) {
                throw new IllegalArgumentException("enricher designed to work only with async-apps");
            }
            if (!isRebinding() && Boolean.FALSE.equals(config().get(SUPPRESS_DUPLICATES))) {
                throw new IllegalArgumentException("Must not set "+SUPPRESS_DUPLICATES+" to false when using "+this);
            }
            super.setEntity(entity);
            if (suppressDuplicates==null) {
                // only publish on changes, unless it is configured otherwise
                suppressDuplicates = true;
            }
            
            // Need to update again, e.g. if stop() effector marks this as expected=stopped.
            // There'd be a risk of infinite loop if we didn't suppressDuplicates!
            subscriptions().subscribe(entity, Attributes.SERVICE_STATE_EXPECTED, new SensorEventListener<Lifecycle.Transition>() {
                @Override public void onEvent(SensorEvent<Lifecycle.Transition> event) {
                    onUpdated();
                }});
        }

        @Override
        protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
            if (RECONFIGURABLE_KEYS.contains(key)) {
                return;
            } else {
                super.doReconfigureConfig(key, val);
            }
        }

        @Override
        protected void onChanged() {
            super.onChanged();
            onUpdated();
        }

        @Override
        protected Collection<Sensor<?>> getSourceSensors() {
            return SOURCE_SENSORS;
        }

        @Override
        protected void onUpdated() {
            if (entity == null || !isRunning() || !Entities.isManaged(entity)) {
                // e.g. invoked during setup or entity has become unmanaged; just ignore
                BrooklynLogging.log(LOG, BrooklynLogging.levelDebugOrTraceIfReadOnly(entity),
                    "Ignoring {} onUpdated when entity is not in valid state ({})", this, entity);
                return;
            }

            Lifecycle.Transition oldExpectedStateTransition = entity.sensors().get(Attributes.SERVICE_STATE_EXPECTED);
            Lifecycle oldExpectedState = (oldExpectedStateTransition != null) ? oldExpectedStateTransition.getState() : null;
            
            ValueAndReason<Boolean> newServiceUp = computeServiceUp(oldExpectedState);
            ValueAndReason<Lifecycle> newServiceState = computeServiceState(oldExpectedState);
            Lifecycle newExpectedState = computeExpectedState(oldExpectedState, newServiceState.val);

            emit(Attributes.SERVICE_STATE_ACTUAL, newServiceState.val);
            emit(Attributes.SERVICE_UP, newServiceUp.val);
            
            if (Boolean.TRUE.equals(newServiceUp.val)) {
                clearMapSensorEntry(entity, Attributes.SERVICE_NOT_UP_INDICATORS, DEFAULT_UNIQUE_TAG);
            } else {
                updateMapSensorEntry(entity, Attributes.SERVICE_NOT_UP_INDICATORS, DEFAULT_UNIQUE_TAG, newServiceUp.reason);
            }
            if (newServiceState.val != null && newServiceState.val == Lifecycle.ON_FIRE) {
                updateMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, DEFAULT_UNIQUE_TAG, newServiceState.reason);
            } else {
                clearMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, DEFAULT_UNIQUE_TAG);
            }
            
            if (oldExpectedState != newExpectedState) {
                // TODO could check no-one else has changed expectedState (e.g. by calling "stop")
                // TODO do we need to subscribe to our own serviceStateExpected, in case someone calls stop?
                getEntity().setExpectedStateAndRecordLifecycleEvent(newExpectedState);
            }
        }

        /**
         * Count the entities that are up versus not up. Compare this with the quorum required.
         */
        protected ValueAndReason<Boolean> computeServiceUp(Lifecycle expectedState) {
            boolean ignoreNull = getConfig(IGNORE_ENTITIES_WITH_SERVICE_UP_NULL);
            Set<Lifecycle> ignoredStates;
            if (expectedState == Lifecycle.STARTING) {
                ignoredStates = getConfig(ENTITY_IGNORED_STATES_ON_STARTING);
            } else {
                ignoredStates = getConfig(ENTITY_IGNORED_STATES_ON_OTHERS);
            }
            
            Map<Entity, Boolean> values = getValues(SERVICE_UP);
            List<Entity> violators = MutableList.of();
            
            int entries = 0;
            int numUp = 0;
            for (Map.Entry<Entity, Boolean> entry : values.entrySet()) {
                Lifecycle entityState = entry.getKey().getAttribute(SERVICE_STATE_ACTUAL);
                
                if (ignoreNull && entry.getValue()==null) {
                    continue;
                }
                if (ignoredStates.contains(entityState)) {
                    continue;
                }
                entries++;

                if (Boolean.TRUE.equals(entry.getValue())) {
                    numUp++;
                } else {
                    violators.add(entry.getKey());
                }
            }

            QuorumCheck qc = getRequiredConfig(UP_QUORUM_CHECK);
            if (qc.isQuorate(numUp, violators.size()+numUp)) {
                // quorate
                return new ValueAndReason<>(Boolean.TRUE, "quorate");
            }
            
            String reason;
            if (values.isEmpty()) {
                reason = "No entities present";
            } else if (entries == 0) {
                reason = "No entities (in correct state) publishing service up";
            } else if (violators.isEmpty()) {
                reason = "Not enough entities";
            } else if (violators.size() == 1) {
                reason = violators.get(0)+" is not up";
            } else if (violators.size() == entries) {
                reason = "None of the entities are up";
            } else {
                reason = violators.size()+" entities are not up, including "+violators.get(0);
            }            
            return new ValueAndReason<>(Boolean.FALSE, reason);
        }

        protected ValueAndReason<Lifecycle> computeServiceState(Lifecycle expectedState) {
            if (expectedState != null && (expectedState != Lifecycle.STARTING && expectedState != Lifecycle.RUNNING)) {
                return new ValueAndReason<>(expectedState, "expected state "+expectedState);
            }
            
            Set<Lifecycle> ignoredStates;
            if (expectedState == Lifecycle.STARTING) {
                ignoredStates = getConfig(ENTITY_IGNORED_STATES_ON_STARTING);
            } else {
                ignoredStates = getConfig(ENTITY_IGNORED_STATES_ON_OTHERS);
            }
            Set<Lifecycle> transitionStates;
            if (expectedState == Lifecycle.STARTING) {
                transitionStates = getConfig(ENTITY_TRANSITION_STATES_ON_STARTING);
            } else {
                transitionStates = ImmutableSet.of();
            }

            Map<Entity, Lifecycle> values = getValues(SERVICE_STATE_ACTUAL);
            List<Entity> violators = MutableList.of();
            int entries = 0;
            int numRunning = 0;
            int numTransitioning = 0;
            
            for (Map.Entry<Entity,Lifecycle> entry : values.entrySet()) {
                if (ignoredStates.contains(entry.getValue())) {
                    continue;
                }
                entries++;
                
                if (entry.getValue() == Lifecycle.RUNNING) {
                    numRunning++;
                } else if (transitionStates.contains(entry.getValue())) {
                    numTransitioning++;
                } else {
                    violators.add(entry.getKey());
                }
            }

            QuorumCheck qc = getConfig(RUNNING_QUORUM_CHECK);
            if (qc.isQuorate(numRunning, violators.size()+numRunning+numTransitioning)) {
                // quorate
                return new ValueAndReason<Lifecycle>(Lifecycle.RUNNING, "quorate");
            }
            boolean canEverBeQuorate = qc.isQuorate(numRunning+numTransitioning, violators.size()+numRunning+numTransitioning);
            if (expectedState == Lifecycle.STARTING && canEverBeQuorate) {
                // quorate
                return new ValueAndReason<Lifecycle>(Lifecycle.STARTING, "not yet quorate");
            }
            
            String reason;
            if (values.isEmpty()) {
                reason = "No entities present";
            } else if (entries == 0) {
                reason = "No entities in interesting states";
            } else if (violators.isEmpty()) {
                reason = "Not enough entities";
            } else if (violators.size() == 1) {
                reason = violators.get(0)+" is not healthy";
            } else if (violators.size() == entries) {
                reason = "None of the entities are healthy";
            } else {
                reason = violators.size()+" entities are not healthy, including "+violators.get(0);
            }            
            return new ValueAndReason<>(Lifecycle.ON_FIRE, reason);

        }

        protected Lifecycle computeExpectedState(Lifecycle oldExpectedState, Lifecycle newActualState) {
            if (oldExpectedState != Lifecycle.STARTING) {
                return oldExpectedState;
            }
            
            // Are any of our children still starting?
            Map<Entity, Lifecycle> values = getValues(SERVICE_STATE_ACTUAL);
            boolean childIsStarting = values.containsValue(Lifecycle.STARTING) || values.containsValue(Lifecycle.CREATED) || values.containsValue(null);

            if (!childIsStarting) {
                // We only transition to expected=RUNNING if all our children are no longer starting.
                // When actual=running, this matters if quorum != all;
                // When actual=on_fire, this matters for recovering.
                return Lifecycle.RUNNING;
            }
            return oldExpectedState;
        }
        
        /** not used; see specific `computeXxx` methods, invoked by overridden onUpdated */
        @Override
        protected Object compute() {
            return null;
        }
        
        static class ValueAndReason<T> {
            final T val;
            final String reason;
            
            ValueAndReason(T val, String reason) {
                this.val = val;
                this.reason = reason;
            }
            
            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this).add("val", val).add("reason", reason).toString();
            }
        }
    }
}
