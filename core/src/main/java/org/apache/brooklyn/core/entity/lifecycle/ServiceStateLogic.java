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
package org.apache.brooklyn.core.entity.lifecycle;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.*;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.EnricherSpec.ExtensibleEnricherSpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.BrooklynLogging.LoggingLevel;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle.Transition;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.enricher.stock.AbstractMultipleSensorAggregator;
import org.apache.brooklyn.enricher.stock.Enrichers;
import org.apache.brooklyn.enricher.stock.UpdatingMap;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.collections.QuorumCheck.QuorumChecks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

/** Logic, sensors and enrichers, and conveniences, for computing service status */
public class ServiceStateLogic {

    private static final Logger log = LoggerFactory.getLogger(ServiceStateLogic.class);

    public static final AttributeSensor<Boolean> SERVICE_UP = Attributes.SERVICE_UP;
    public static final AttributeSensor<Map<String,Object>> SERVICE_NOT_UP_INDICATORS = Attributes.SERVICE_NOT_UP_INDICATORS;
    public static final AttributeSensor<Map<String,Object>> SERVICE_NOT_UP_DIAGNOSTICS = Attributes.SERVICE_NOT_UP_DIAGNOSTICS;

    public static final AttributeSensor<Lifecycle> SERVICE_STATE_ACTUAL = Attributes.SERVICE_STATE_ACTUAL;
    public static final AttributeSensor<Lifecycle.Transition> SERVICE_STATE_EXPECTED = Attributes.SERVICE_STATE_EXPECTED;
    public static final AttributeSensor<Map<String,Object>> SERVICE_PROBLEMS = Attributes.SERVICE_PROBLEMS;

    /** static only; not for instantiation */
    private ServiceStateLogic() {}

    public static <TKey,TVal> TVal getMapSensorEntry(Entity entity, AttributeSensor<Map<TKey,TVal>> sensor, TKey key) {
        Map<TKey, TVal> map = entity.getAttribute(sensor);
        if (map==null) return null;
        return map.get(key);
    }

    @SuppressWarnings("unchecked")
    public static <TKey,TVal> void clearMapSensorEntry(Entity entity, AttributeSensor<Map<TKey,TVal>> sensor, TKey key) {
        updateMapSensorEntry(entity, sensor, key, (TVal)Entities.REMOVE);
    }

    /** update the given key in the given map sensor */
    public static <TKey,TVal> void updateMapSensorEntry(Entity entity, AttributeSensor<Map<TKey,TVal>> sensor, final TKey key, final TVal v) {
        /*
         * Important to *not* modify the existing attribute value; must make a copy, modify that, and publish.
         * This is because a Propagator enricher will set this same value on another entity. There was very
         * strange behaviour when this was done for a SERVICE_UP_INDICATORS sensor - the updates done here
         * applied to the attribute of both entities!
         *
         * Need to do this update atomically (i.e. sequentially) because there is no threading control for
         * what is calling updateMapSensorEntity. It is called directly on start, on initialising enrichers,
         * and in event listeners. These calls could be concurrent.
         */
        Function<Map<TKey,TVal>, Maybe<Map<TKey,TVal>>> modifier = new Function<Map<TKey,TVal>, Maybe<Map<TKey,TVal>>>() {
            @Override public Maybe<Map<TKey, TVal>> apply(Map<TKey, TVal> map) {
                boolean created = (map==null);
                if (created) map = MutableMap.of();

                boolean changed;
                if (v == Entities.REMOVE) {
                    changed = map.containsKey(key);
                    if (changed) {
                        map = MutableMap.copyOf(map);
                        map.remove(key);
                    }
                } else {
                    TVal oldV = map.get(key);
                    if (oldV==null) {
                        changed = (v!=null || !map.containsKey(key));
                    } else {
                        changed = !oldV.equals(v);
                    }
                    if (changed) {
                        map = MutableMap.copyOf(map);
                        map.put(key, v);
                    }
                }
                if (changed || created) {
                    return Maybe.of(map);
                } else {
                    return Maybe.absent();
                }
            }
        };

        if (!Entities.isNoLongerManaged(entity)) {
            entity.sensors().modify(sensor, modifier);
        }
    }

    public static void setExpectedState(Entity entity, Lifecycle state) {
        setExpectedState(entity, state, entity.getAttribute(SERVICE_STATE_ACTUAL)!=null && entity.getAttribute(SERVICE_STATE_ACTUAL)!=Lifecycle.ON_FIRE);
    }
    
    public static void setExpectedStateRunningWithErrors(Entity entity) {
        setExpectedState(entity, Lifecycle.RUNNING, false);
    }
    
    private static void setExpectedState(Entity entity, Lifecycle state, boolean waitBrieflyForServiceUpIfRunning) {
        if (waitBrieflyForServiceUpIfRunning && state == Lifecycle.RUNNING) {
            recomputeWantingNoIssuesWhenBecomingExpectedRunning("setting expected state", entity, RecomputeWaitMode.LONG);
        }
        ((EntityInternal)entity).sensors().set(Attributes.SERVICE_STATE_EXPECTED, new Lifecycle.Transition(state, new Date()));

        Maybe<Enricher> enricher = EntityAdjuncts.tryFindWithUniqueTag(entity.enrichers(), ComputeServiceState.DEFAULT_ENRICHER_UNIQUE_TAG);
        if (enricher.isPresent() && enricher.get() instanceof ComputeServiceState) {
            ((ComputeServiceState)enricher.get()).onEvent(null);
        }
    }

    public static Lifecycle getExpectedState(Entity entity) {
        Transition expected = entity.getAttribute(Attributes.SERVICE_STATE_EXPECTED);
        if (expected==null) return null;
        return expected.getState();
    }

    private static boolean isEmptyOrNull(Map x) {
        return x==null || x.isEmpty();
    }
    private enum RecomputeWaitMode { LONG, SHORT, RECOMPUTE_ONLY, NONE }
    private static boolean recomputeWantingNoIssuesWhenBecomingExpectedRunning(String when, Entity entity, RecomputeWaitMode canWait) {
        if (!Entities.isManagedActive(entity)) return true;

        Map<String, Object> problems = entity.getAttribute(SERVICE_PROBLEMS);
        Boolean up = entity.getAttribute(Attributes.SERVICE_UP);
        if (Boolean.TRUE.equals(up) && isEmptyOrNull(problems)) return true;
        if (canWait==RecomputeWaitMode.NONE) return false;

        log.debug("Recompute indicated setting RUNNING ("+when+") on service issue; up="+up+", problems="+problems+",on " + entity + " (mode "+canWait+")");
        try {
            Iterables.filter(entity.enrichers(), x -> x instanceof ComputeServiceIndicatorsFromChildrenAndMembers).forEach(
                    x -> {
                        ComputeServiceIndicatorsFromChildrenAndMembers mx = (ComputeServiceIndicatorsFromChildrenAndMembers) x;
                        if (mx.isRunning()) {
                            log.debug("Recompute rerunning "+mx);
                            mx.onUpdated();
                            log.debug("Recomputed values now: problems="+
                                    entity.sensors().get(SERVICE_PROBLEMS) + ", not_up_indicators=" + entity.sensors().get(SERVICE_NOT_UP_INDICATORS) );
                        }
                    }
            );

            Map<String, Object> notUpIndicators = entity.sensors().get(Attributes.SERVICE_NOT_UP_INDICATORS);
            if (notUpIndicators == null || notUpIndicators.isEmpty()) {
                Maybe<Enricher> css = EntityAdjuncts.tryFindWithUniqueTag(entity.enrichers(), ServiceNotUpLogic.DEFAULT_ENRICHER_UNIQUE_TAG);
                if (css.isPresent()) {
                    SensorEvent<Map<String, Object>> pseudoEvent = new BasicSensorEvent<>(Attributes.SERVICE_NOT_UP_INDICATORS, entity, notUpIndicators);
                    ((SensorEventListener) css.get()).onEvent(pseudoEvent);
                    up = entity.getAttribute(Attributes.SERVICE_UP);
                    log.debug("Recompute for service indicators now gives: service.isUp="+up+" after: "+css);
                }
            } else {
                log.debug("Recomputed not_up_indicators now: " + notUpIndicators);
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            if (!Entities.isManagedActive(entity)) return true;
            log.debug("Service is not up when setting RUNNING on " + entity+", and attempt to run standard prep workflows failed with exception: "+e);
            if (log.isTraceEnabled()) log.trace("Exception trace", e);
        }

        if (recomputeWantingNoIssuesWhenBecomingExpectedRunning(when+" (after recompute)", entity, RecomputeWaitMode.NONE)) return true;
        if (canWait==RecomputeWaitMode.RECOMPUTE_ONLY) return false;

        assert canWait==RecomputeWaitMode.LONG || canWait==RecomputeWaitMode.SHORT;
        // repeat with pauses briefly to allow any recent in-flight service-ups or state changes or problem-clearing processing to complete;
        // should only be necessary when using setExpectedState, where canWait is true, unless the actual is used without expected;
        // but leaving it as it used to be to minimise surprises for now;
        // probably also no need to recompute above either with this (could be tidied up a lot)
        Stopwatch timer = Stopwatch.createStarted();
        final Duration LONG_WAIT = Duration.THIRTY_SECONDS;  // this should be enough time for in-flight racing activities, even when slow and heavily contended
        Task current = Tasks.current();
        boolean notTimedout = Repeater.create()
                .every(ValueResolver.REAL_QUICK_PERIOD)
                .limitTimeTo(canWait==RecomputeWaitMode.LONG ? LONG_WAIT : ValueResolver.PRETTY_QUICK_WAIT)
                .until(() -> {
                    Set<Task<?>> tasksHere = BrooklynTaskTags.getTasksInEntityContext(
                            ((EntityInternal) entity).getManagementContext().getExecutionManager(), entity);
                    java.util.Optional<Task<?>> unrelatedSubmission = tasksHere.stream()
                            .filter(t ->
                                    !t.isDone() &&
                                    BrooklynTaskTags.hasTag(t, BrooklynTaskTags.SENSOR_TAG) &&
                                    !Tasks.isAncestor(current, anc -> Objects.equal(anc, t)))
                            .findAny();
                    // abort when self and children have no unrelated submission tasks, or if something is known to be on_fire;
                    // otherwise give it LONG_WAIT (arbitrary but inoffensive) to settle in that case (things running very slow)
                    if (!unrelatedSubmission.isPresent()) return true;
                    return recomputeWantingNoIssuesWhenBecomingExpectedRunning(when+" (recheck after "+Duration.of(timer.elapsed())+")", entity, RecomputeWaitMode.NONE);
                })
                .run();

        boolean nowUp = recomputeWantingNoIssuesWhenBecomingExpectedRunning(when+" (recheck after "+
                (notTimedout ? "completion" : "timeout")+", after "+Duration.of(timer.elapsed())+")", entity, RecomputeWaitMode.RECOMPUTE_ONLY);
        if (nowUp) {
            log.debug("Recompute determined "+entity+" is up, after " + Duration.of(timer));
            return true;
        }

        if (!Entities.isManagedActive(entity)) return true;

        log.warn("Service is not up when "+when+" on " + entity + "; delayed " + Duration.of(timer) + " "
                + "but: " + Attributes.SERVICE_UP + "=" + entity.getAttribute(Attributes.SERVICE_UP) + ", "
                + "not-up-indicators=" + entity.getAttribute(Attributes.SERVICE_NOT_UP_INDICATORS)
                + ", problems=" + entity.getAttribute(Attributes.SERVICE_PROBLEMS)
        );
        // slight chance above has updated since the check, but previuos log messages should make clear what happened
        return false;
    }

    public static Lifecycle getActualState(Entity entity) {
        return entity.getAttribute(Attributes.SERVICE_STATE_ACTUAL);
    }

    public static boolean isExpectedState(Entity entity, Lifecycle state) {
        return getExpectedState(entity)==state;
    }

    public static class ServiceNotUpLogic implements Function<SensorEvent<Map<String, Object>>, Object> {
        public static final String DEFAULT_ENRICHER_UNIQUE_TAG = "service.isUp if no service.notUp.indicators";

        private ServiceNotUpLogic() {}

        public static final EnricherSpec<?> newEnricherForServiceUpIfNotUpIndicatorsEmpty() {
            // The function means: if the serviceNotUpIndicators is not null, then serviceNotUpIndicators.size()==0;
            // otherwise return the default value.
            return Enrichers.builder()
                .transforming(SERVICE_NOT_UP_INDICATORS).<Object>publishing(Attributes.SERVICE_UP)
                .suppressDuplicates(true)
                .computingFromEvent(new ServiceNotUpLogic())
                .uniqueTag(DEFAULT_ENRICHER_UNIQUE_TAG)
                .build();
        }

        /** puts the given value into the {@link Attributes#SERVICE_NOT_UP_INDICATORS} map as if the
         * {@link UpdatingMap} enricher for the given key */
        public static void updateNotUpIndicator(Entity entity, String key, Object value) {
            updateMapSensorEntry(entity, Attributes.SERVICE_NOT_UP_INDICATORS, key, value);
        }
        /** clears any entry for the given key in the {@link Attributes#SERVICE_NOT_UP_INDICATORS} map */
        public static void clearNotUpIndicator(Entity entity, String key) {
            clearMapSensorEntry(entity, Attributes.SERVICE_NOT_UP_INDICATORS, key);
        }
        /** as {@link #updateNotUpIndicator(Entity, String, Object)} using the given sensor as the key */
        public static void updateNotUpIndicator(Entity entity, Sensor<?> sensor, Object value) {
            updateMapSensorEntry(entity, Attributes.SERVICE_NOT_UP_INDICATORS, sensor.getName(), value);
        }
        /** as {@link #clearNotUpIndicator(Entity, String)} using the given sensor as the key */
        public static void clearNotUpIndicator(Entity entity, Sensor<?> sensor) {
            clearMapSensorEntry(entity, Attributes.SERVICE_NOT_UP_INDICATORS, sensor.getName());
        }

        public static void updateNotUpIndicatorRequiringNonEmptyList(Entity entity, AttributeSensor<? extends Collection<?>> collectionSensor) {
            Collection<?> nodes = entity.getAttribute(collectionSensor);
            if (nodes==null || nodes.isEmpty()) ServiceNotUpLogic.updateNotUpIndicator(entity, collectionSensor, "Should have at least one entry");
            else ServiceNotUpLogic.clearNotUpIndicator(entity, collectionSensor);
        }
        public static void updateNotUpIndicatorRequiringNonEmptyMap(Entity entity, AttributeSensor<? extends Map<?,?>> mapSensor) {
            Map<?, ?> nodes = entity.getAttribute(mapSensor);
            if (nodes==null || nodes.isEmpty()) ServiceNotUpLogic.updateNotUpIndicator(entity, mapSensor, "Should have at least one entry");
            else ServiceNotUpLogic.clearNotUpIndicator(entity, mapSensor);
        }

        @Override
        public Object apply(SensorEvent<Map<String, Object>> input) {
            Entity entity = input.getSource();
            Map<String, Object> currentInput = entity.sensors().get(SERVICE_NOT_UP_INDICATORS);
            if (!Objects.equal(input.getValue(), currentInput)) {
                log.debug("Skipping computation of '"+DEFAULT_ENRICHER_UNIQUE_TAG+"' for "+entity+" because indicators are stale: received "+input+", but current is "+currentInput);
                return Entities.UNCHANGED;
            }
            Object result = Functionals.<Map<String, ?>>ifNotEquals(null).<Object>apply(Functions.forPredicate(CollectionFunctionals.<String>mapSizeEquals(0)))
                    .defaultValue(Entities.UNCHANGED).apply(input.getValue());
            if (!Objects.equal(result, Entities.UNCHANGED)) {
                Boolean prevValue = entity.sensors().get(SERVICE_UP);
                if (!Objects.equal(result, prevValue) && (prevValue!=null || result!=Boolean.TRUE)) {
                    log.debug("Enricher '" + DEFAULT_ENRICHER_UNIQUE_TAG + "' for " + entity + " determined service up changed from " + prevValue + " to " + result + " due to indicators: " + input);
                }
            }
            return result;
        }
    }

    /** Enricher which sets {@link Attributes#SERVICE_STATE_ACTUAL} on changes to
     * {@link Attributes#SERVICE_STATE_EXPECTED}, {@link Attributes#SERVICE_PROBLEMS}, and {@link Attributes#SERVICE_UP}
     * <p>
     * The default implementation uses {@link #computeActualStateWhenExpectedRunning(SensorEvent)} if the last expected transition
     * was to {@link Lifecycle#RUNNING} and
     * {@link #computeActualStateWhenNotExpectedRunning(org.apache.brooklyn.core.entity.lifecycle.Lifecycle.Transition)} otherwise.
     * If these methods return null, the {@link Attributes#SERVICE_STATE_ACTUAL} sensor will be cleared (removed).
     * Either of these methods can be overridden for custom logic, and that custom enricher can be created using
     * newEnricher... methods on this class, and added to an entity.
     */
    public static class ComputeServiceState extends AbstractEnricher implements SensorEventListener<Object> {
        private static final Logger log = LoggerFactory.getLogger(ComputeServiceState.class);
        public static final String DEFAULT_ENRICHER_UNIQUE_TAG = "service.state.actual";

        private final AtomicInteger warnCounter = new AtomicInteger();

        public ComputeServiceState() {}

        @Override
        public void init() {
            super.init();
            if (uniqueTag==null) uniqueTag = DEFAULT_ENRICHER_UNIQUE_TAG;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setEntity(EntityLocal entity) {
            super.setEntity(entity);
            if (suppressDuplicates==null) {
                // only publish on changes, unless it is configured otherwise
                suppressDuplicates = true;
            }

            Map<String, ?> notifyOfInitialValue = ImmutableMap.of("notifyOfInitialValue", Boolean.TRUE);
            subscriptions().subscribe(notifyOfInitialValue, entity, SERVICE_PROBLEMS, this);
            subscriptions().subscribe(notifyOfInitialValue, entity, SERVICE_UP, this);
            subscriptions().subscribe(notifyOfInitialValue, entity, SERVICE_STATE_EXPECTED, this);
            
            highlightTriggers(MutableList.of(SERVICE_PROBLEMS, SERVICE_UP, SERVICE_STATE_EXPECTED), null);
        }

        @Override
        public void onEvent(@Nullable SensorEvent<Object> event) {
            Preconditions.checkNotNull(entity, "Cannot handle subscriptions or compute state until associated with an entity");

            Lifecycle.Transition serviceExpected = entity.getAttribute(SERVICE_STATE_EXPECTED);

            if (serviceExpected!=null && serviceExpected.getState()==Lifecycle.RUNNING) {
                setActualState( computeActualStateWhenExpectedRunning(event) );
            } else {
                setActualState( computeActualStateWhenNotExpectedRunning(serviceExpected) );
            }
        }

        transient int recomputeDepth=0;
        protected Maybe<Lifecycle> computeActualStateWhenExpectedRunning(SensorEvent<Object> event) {
            if (recomputeDepth>0) {
                return Maybe.absent("Skipping actual state computation because already computing");
            }
            try {
                while (true) {
                    Map<String, Object> problems = entity.getAttribute(SERVICE_PROBLEMS);
                    boolean noProblems = problems == null || problems.isEmpty();

                    Boolean serviceUp = entity.getAttribute(SERVICE_UP);

                    if (Boolean.TRUE.equals(serviceUp) && noProblems) {
                        return Maybe.of(Lifecycle.RUNNING);
                    } else {
                        if (!Entities.isManagedActive(entity)) {
                            return Maybe.absent("entity not managed active");
                        }
                        // with delay when writing expected state, it should not be necessary to have a wait/retry
                        if (!Lifecycle.ON_FIRE.equals(entity.getAttribute(SERVICE_STATE_ACTUAL))) {
                            boolean retryable = recomputeDepth == 0;
                            // allow recompute if event is null (intermediate recomputation?)
                            // but need to prevent
                            retryable = retryable && (event == null || !Attributes.SERVICE_UP.equals(event.getSensor()));
                            if (retryable) {
                                recomputeDepth++;
                                // occasional race here; might want to give a grace period if entity has just transitioned; allow children to catch up;
                                // we should have done the wait when expected running, but possibly it hasn't caught up yet
                                log.debug("Entity " + entity + " would be computed on-fire due to problems (up=" + serviceUp + ", problems=" + problems + "), will attempt re-check");
                                recomputeWantingNoIssuesWhenBecomingExpectedRunning("computing actual state", entity,
                                        RecomputeWaitMode.SHORT  // NONE would probalby be fine here, with none of the recompute above,
                                        // at least whenever expected state is used, due to how it waits now; but leaving it as is until more confirmation
                                );
                                continue;
                            }
                        }
                        BrooklynLogging.log(log, BrooklynLogging.levelDependingIfReadOnly(entity, LoggingLevel.WARN, LoggingLevel.TRACE, LoggingLevel.DEBUG),
                                "Setting " + entity + " " + Lifecycle.ON_FIRE + " due to problems when expected running, " +
                                        "trigger=" + event + ", " +
                                        "up=" + serviceUp + ", " +
                                        (noProblems ? "not-up-indicators: " + entity.getAttribute(SERVICE_NOT_UP_INDICATORS) : "problems: " + problems));
                        return Maybe.of(Lifecycle.ON_FIRE);
                    }
                }
            } finally {
                recomputeDepth = 0;
            }
        }

        protected Maybe<Lifecycle> computeActualStateWhenNotExpectedRunning(Lifecycle.Transition stateTransition) {
            Map<String, Object> problems = entity.getAttribute(SERVICE_PROBLEMS);
            Boolean up = entity.getAttribute(SERVICE_UP);

            if (stateTransition!=null) {
                // if expected state is present but not running, just echo the expected state (ignore problems and up-ness)
                return Maybe.of(stateTransition.getState());

            } else if (problems!=null && !problems.isEmpty()) {
                // if there is no expected state, then if service is not up, say stopped, else say on fire (whether service up is true or not present)
                if (Boolean.FALSE.equals(up)) {
                    return Maybe.of(Lifecycle.STOPPED);
                } else {
                    BrooklynLogging.log(log, BrooklynLogging.levelDependingIfReadOnly(entity, LoggingLevel.WARN, LoggingLevel.TRACE, LoggingLevel.DEBUG),
                        "Computed "+entity+" "+Lifecycle.ON_FIRE+" due to problems when expected "+stateTransition+" / up="+up+": "+problems);
                    return Maybe.of(Lifecycle.ON_FIRE);
                }
            } else {
                // no expected transition and no problems
                // if the problems map is non-null, then infer from service up;
                // if there is no problems map, then leave unchanged (user may have set it explicitly)
                if (problems!=null)
                    return Maybe.of(up==null ? null /* remove if up is not set */ :
                        (up ? Lifecycle.RUNNING : Lifecycle.STOPPED));
                else
                    return Maybe.absent();
            }
        }

        protected void setActualState(Maybe<Lifecycle> state) {
            if (log.isTraceEnabled()) log.trace("{} setting actual state {}", this, state);
            if (((EntityInternal)entity).getManagementSupport().isNoLongerManaged()) {
                // won't catch everything, but catches some
                BrooklynLogging.log(log, BrooklynLogging.levelDebugOrTraceIfReadOnly(entity),
                    entity+" is no longer managed when told to set actual state to "+state+"; suppressing");
                return;
            }
            Object newVal = (state.isAbsent() ? Entities.UNCHANGED : (state.get() == null ? Entities.REMOVE : state.get()));
            emit(SERVICE_STATE_ACTUAL, newVal);
        }

    }

    public static final EnricherSpec<?> newEnricherForServiceStateFromProblemsAndUp() {
        return newEnricherForServiceState(ComputeServiceState.class);
    }
    public static final EnricherSpec<?> newEnricherForServiceState(Class<? extends Enricher> type) {
        return EnricherSpec.create(type);
    }

    /**
     * An enricher that sets {@link Startable#SERVICE_UP servive.isUp} on an entity only when all children are
     * also reporting as healthy.
     * <p>
     * Equivalent to the following YAML configuration.
     * <pre>
     * brooklyn.enrichers:
     *   - type: org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic$ComputeServiceIndicatorsFromChildrenAndMembers
     *     brooklyn.config:
     *       enricher.aggregating.fromChildren: true
     *       enricher.aggregating.fromMembers: false
     *       enricher.suppressDuplicates: true
     *       enricher.service_state.children_and_members.quorum.up: all
     *       enricher.service_state.children_and_members.ignore_entities.service_state_values: [ "STOPPING", "STOPPED", "DESTROYED" ]
     * </pre>
     */
    public static final EnricherSpec<?> newEnricherForServiceUpFromChildren() {
        return newEnricherForServiceUp(Boolean.TRUE, Boolean.FALSE);
    }
    public static final EnricherSpec<?> newEnricherForServiceUpFromMembers() {
        return newEnricherForServiceUp(Boolean.FALSE, Boolean.TRUE);
    }
    public static final EnricherSpec<?> newEnricherForServiceUpFromChildrenWithQuorumCheck(QuorumCheck quorumCheck) {
        EnricherSpec<?> serviceUp = newEnricherForServiceUpFromChildren()
                .configure(ComputeServiceIndicatorsFromChildrenAndMembers.RUNNING_QUORUM_CHECK, quorumCheck);
        return serviceUp;
    }
    public static final EnricherSpec<?> newEnricherForServiceUp(Boolean fromChildren, Boolean fromMembers) {
        EnricherSpec<?> serviceUp = EnricherSpec.create(ComputeServiceIndicatorsFromChildrenAndMembers.class)
                .configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_CHILDREN, fromChildren)
                .configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_MEMBERS, fromMembers)
                .configure(ComputeServiceIndicatorsFromChildrenAndMembers.SUPPRESS_DUPLICATES, Boolean.TRUE)
                .configure(ComputeServiceIndicatorsFromChildrenAndMembers.RUNNING_QUORUM_CHECK, QuorumChecks.all())
                .configure(ComputeServiceIndicatorsFromChildrenAndMembers.IGNORE_ENTITIES_WITH_THESE_SERVICE_STATES, ImmutableSet.of(Lifecycle.STOPPING, Lifecycle.STOPPED, Lifecycle.DESTROYED));
        return serviceUp;
    }

    public static class ServiceProblemsLogic {
        /** static only; not for instantiation */
        private ServiceProblemsLogic() {}

        /** puts the given value into the {@link Attributes#SERVICE_PROBLEMS} map as if the
         * {@link UpdatingMap} enricher for the given sensor reported this value */
        public static void updateProblemsIndicator(Entity entity, Sensor<?> sensor, Object value) {
            updateMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, sensor.getName(), value);
        }
        /** clears any entry for the given sensor in the {@link Attributes#SERVICE_PROBLEMS} map */
        public static void clearProblemsIndicator(Entity entity, Sensor<?> sensor) {
            clearMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, sensor.getName());
        }
        /** as {@link #updateProblemsIndicator(Entity, Sensor, Object)} */
        public static void updateProblemsIndicator(Entity entity, Effector<?> eff, Object value) {
            updateMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, eff.getName(), value);
        }
        /** as {@link #clearProblemsIndicator(Entity, Sensor)} */
        public static void clearProblemsIndicator(Entity entity, Effector<?> eff) {
            clearMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, eff.getName());
        }
        /** as {@link #updateProblemsIndicator(Entity, Sensor, Object)} */
        public static void updateProblemsIndicator(Entity entity, String key, Object value) {
            updateMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, key, value);
        }
        /** as {@link #clearProblemsIndicator(Entity, Sensor)} */
        public static void clearProblemsIndicator(Entity entity, String key) {
            clearMapSensorEntry(entity, Attributes.SERVICE_PROBLEMS, key);
        }
    }

    public static class ComputeServiceIndicatorsFromChildrenAndMembers extends AbstractMultipleSensorAggregator<Void> implements SensorEventListener<Object> {
        /** standard unique tag identifying instances of this enricher at runtime, also used for the map sensor if no unique tag specified */
        public final static String DEFAULT_UNIQUE_TAG = "service-lifecycle-indicators-from-children-and-members";

        /** as {@link #DEFAULT_UNIQUE_TAG}, but when a second distinct instance is responsible for computing service up */
        public final static String DEFAULT_UNIQUE_TAG_UP = "service-not-up-indicators-from-children-and-members";

        public static final ConfigKey<QuorumCheck> UP_QUORUM_CHECK = ConfigKeys.builder(QuorumCheck.class, "enricher.service_state.children_and_members.quorum.up")
            .description("Logic for checking whether this service is up, based on children and/or members, defaulting to allowing none but if there are any requiring at least one to be up")
            .defaultValue(QuorumCheck.QuorumChecks.atLeastOneUnlessEmpty())
            .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
            .build();
        public static final ConfigKey<QuorumCheck> RUNNING_QUORUM_CHECK = ConfigKeys.builder(QuorumCheck.class, "enricher.service_state.children_and_members.quorum.running")
            .description("Logic for checking whether this service is healthy, based on children and/or members running, defaulting to requiring none to be ON-FIRE")
            .defaultValue(QuorumCheck.QuorumChecks.all())
            .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
            .build();
        // TODO items below should probably also have inheritance NONE ?
        public static final ConfigKey<Boolean> DERIVE_SERVICE_NOT_UP = ConfigKeys.newBooleanConfigKey("enricher.service_state.children_and_members.service_up.publish", "Whether to derive a service-not-up indicator from children", true);
        public static final ConfigKey<Boolean> DERIVE_SERVICE_PROBLEMS = ConfigKeys.newBooleanConfigKey("enricher.service_state.children_and_members.service_problems.publish", "Whether to derive a service-problem indicator from children", true);
        public static final ConfigKey<Boolean> IGNORE_ENTITIES_WITH_SERVICE_UP_NULL = ConfigKeys.newBooleanConfigKey("enricher.service_state.children_and_members.ignore_entities.service_up_null", "Whether to ignore children reporting null values for service up", true);
        @SuppressWarnings("serial")
        public static final ConfigKey<Set<Lifecycle>> IGNORE_ENTITIES_WITH_THESE_SERVICE_STATES = ConfigKeys.newConfigKey(new TypeToken<Set<Lifecycle>>() {},
            "enricher.service_state.children_and_members.ignore_entities.service_state_values",
            "Service states of children (including null) which indicate they should be ignored when looking at children service states; anything apart from RUNNING not in this list will be treated as not healthy (by default just ON_FIRE will mean not healthy)",
            MutableSet.<Lifecycle>builder().addAll(Lifecycle.values()).add(null).remove(Lifecycle.RUNNING).remove(Lifecycle.ON_FIRE).build().asUnmodifiable());

        protected String getKeyForMapSensor() {
            return Preconditions.checkNotNull(super.getUniqueTag());
        }

        @Override
        protected void setEntityLoadingConfig() {
            fromChildren = true;
            fromMembers = true;
            // above sets default
            super.setEntityLoadingConfig();
            if (isAggregatingMembers() && (!(entity instanceof Group))) {
                if (fromChildren) fromMembers=false;
                else throw new IllegalStateException("Cannot monitor only members for non-group entity "+entity+": "+this);
            }
            Preconditions.checkNotNull(getKeyForMapSensor());
        }

        @Override
        protected void setEntityLoadingTargetConfig() {
            if (getConfig(TARGET_SENSOR)!=null)
                throw new IllegalArgumentException("Must not set "+TARGET_SENSOR+" when using "+this);
        }

        @Override
        public void setEntity(EntityLocal entity) {
            super.setEntity(entity);
            if (suppressDuplicates==null) {
                // only publish on changes, unless it is configured otherwise
                suppressDuplicates = true;
            }
        }

        final static Set<ConfigKey<?>> RECONFIGURABLE_KEYS = ImmutableSet.<ConfigKey<?>>of(
            UP_QUORUM_CHECK, RUNNING_QUORUM_CHECK,
            DERIVE_SERVICE_NOT_UP, DERIVE_SERVICE_NOT_UP,
            IGNORE_ENTITIES_WITH_SERVICE_UP_NULL, IGNORE_ENTITIES_WITH_THESE_SERVICE_STATES);

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
            if (entity != null && isRunning())
                onUpdated();
        }

        private final List<Sensor<?>> SOURCE_SENSORS = ImmutableList.<Sensor<?>>of(SERVICE_UP, SERVICE_STATE_ACTUAL);
        @Override
        protected Collection<Sensor<?>> getSourceSensors() {
            return SOURCE_SENSORS;
        }

        @Override
        public void onUpdated() {
            if (entity==null || !Entities.isManagedActive(entity)) {
                // either invoked during setup or entity has become unmanaged; just ignore
                BrooklynLogging.log(log, BrooklynLogging.levelDebugOrTraceIfReadOnly(entity),
                    "Ignoring service indicators onUpdated at {} from invalid/unmanaged entity ({})", this, entity);
                return;
            }

            // override superclass to publish multiple sensors
            if (getConfig(DERIVE_SERVICE_PROBLEMS)) {
                updateMapSensor(SERVICE_PROBLEMS, computeServiceProblems());
            }

            if (getConfig(DERIVE_SERVICE_NOT_UP)) {
                updateMapSensor(SERVICE_NOT_UP_INDICATORS, computeServiceNotUp());
            }
        }

        protected Object computeServiceNotUp() {
            Map<Entity, Boolean> values = getValues(SERVICE_UP);
            List<Entity> violators = MutableList.of();
            boolean ignoreNull = getConfig(IGNORE_ENTITIES_WITH_SERVICE_UP_NULL);
            Set<Lifecycle> ignoreStates = getConfig(IGNORE_ENTITIES_WITH_THESE_SERVICE_STATES);
            int entries=0;
            int numUp=0;
            for (Map.Entry<Entity, Boolean> state: values.entrySet()) {
                if (ignoreNull && state.getValue()==null)
                    continue;
                entries++;
                Lifecycle entityState = state.getKey().getAttribute(SERVICE_STATE_ACTUAL);

                if (Boolean.TRUE.equals(state.getValue())) numUp++;
                else if (!ignoreStates.contains(entityState)) {
                    violators.add(state.getKey());
                }
            }

            QuorumCheck qc = getConfig(UP_QUORUM_CHECK);
            if (qc!=null) {
                if (qc.isQuorate(numUp, violators.size()+numUp))
                    // quorate
                    return null;

                if (values.isEmpty()) return "No entities present";
                if (entries==0) return "No entities publishing service up";
                if (violators.isEmpty()) return "Not enough entities";
            } else {
                if (violators.isEmpty())
                    return null;
            }

            if (violators.size()==1) return violators.get(0)+" is not up";
            if (violators.size()==entries) return "None of the entities are up";
            return violators.size()+" entities are not up, including "+violators.get(0);
        }

        protected Object computeServiceProblems() {
            Map<Entity, Lifecycle> values = getValues(SERVICE_STATE_ACTUAL);
            int numRunning=0;
            Map<Entity,String> onesNotHealthy=MutableMap.of();
            Set<Lifecycle> ignoreStates = getConfig(IGNORE_ENTITIES_WITH_THESE_SERVICE_STATES);
            for (Map.Entry<Entity,Lifecycle> state: values.entrySet()) {
                if (state.getValue()==Lifecycle.RUNNING) numRunning++;
                else if (!ignoreStates.contains(state.getValue()))
                    onesNotHealthy.put(state.getKey(), ""+state.getValue());
            }

            QuorumCheck qc = getConfig(RUNNING_QUORUM_CHECK);
            if (qc!=null) {
                if (qc.isQuorate(numRunning, onesNotHealthy.size()+numRunning))
                    // quorate
                    return null;

                if (onesNotHealthy.isEmpty())
                    return "Not enough entities running to be quorate";
            } else {
                if (onesNotHealthy.isEmpty())
                    return null;
            }

            return "Required entit"+Strings.ies(onesNotHealthy.size())+" not healthy: "+
                (onesNotHealthy.size()>3
                        ? nameOfEntity(onesNotHealthy.keySet().iterator().next())+" ("+onesNotHealthy.values().iterator().next()+") and "+(onesNotHealthy.size()-1)+" others"
                        : onesNotHealthy.entrySet().stream().map(entry -> nameOfEntity(entry.getKey())+" ("+entry.getValue()+")").collect(Collectors.joining(", ")));
        }

        private List<String> nameOfEntity(List<Entity> entities) {
            List<String> result = MutableList.of();
            for (Entity e: entities) result.add(nameOfEntity(e));
            return result;
        }

        private String nameOfEntity(Entity entity) {
            String name = entity.getDisplayName();
            if (name.contains(entity.getId())) return name;
            else return name + " ("+entity.getId()+")";
        }

        protected void updateMapSensor(AttributeSensor<Map<String, Object>> sensor, Object value) {
            if (log.isTraceEnabled()) log.trace("{} updating map sensor {} with {}", new Object[] { this, sensor, value });

            if (value!=null) {
                updateMapSensorEntry(entity, sensor, getKeyForMapSensor(), value);
            } else {
                clearMapSensorEntry(entity, sensor, getKeyForMapSensor());
            }
        }

        /** not used; see specific `computeXxx` methods, invoked by overridden onUpdated */
        @Override
        protected Object compute() {
            return null;
        }
    }

    public static class ComputeServiceIndicatorsFromChildrenAndMembersSpec extends ExtensibleEnricherSpec<ComputeServiceIndicatorsFromChildrenAndMembers,ComputeServiceIndicatorsFromChildrenAndMembersSpec> {
        private static final long serialVersionUID = -607444925297963712L;

        protected ComputeServiceIndicatorsFromChildrenAndMembersSpec() {
            this(ComputeServiceIndicatorsFromChildrenAndMembers.class);
        }

        protected ComputeServiceIndicatorsFromChildrenAndMembersSpec(Class<? extends ComputeServiceIndicatorsFromChildrenAndMembers> clazz) {
            super(clazz);
        }

        public void addTo(Entity entity) {
            entity.enrichers().add(this);
        }

        public ComputeServiceIndicatorsFromChildrenAndMembersSpec suppressDuplicates(boolean val) {
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.SUPPRESS_DUPLICATES, val);
            return self();
        }
        public ComputeServiceIndicatorsFromChildrenAndMembersSpec checkChildrenAndMembers() {
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_MEMBERS, true);
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_CHILDREN, true);
            return self();
        }
        public ComputeServiceIndicatorsFromChildrenAndMembersSpec checkMembersOnly() {
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_MEMBERS, true);
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_CHILDREN, false);
            return self();
        }
        public ComputeServiceIndicatorsFromChildrenAndMembersSpec checkChildrenOnly() {
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_MEMBERS, false);
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.FROM_CHILDREN, true);
            return self();
        }

        public ComputeServiceIndicatorsFromChildrenAndMembersSpec requireUpChildren(QuorumCheck check) {
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.UP_QUORUM_CHECK, check);
            return self();
        }
        public ComputeServiceIndicatorsFromChildrenAndMembersSpec requireRunningChildren(QuorumCheck check) {
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.RUNNING_QUORUM_CHECK, check);
            return self();
        }

        public ComputeServiceIndicatorsFromChildrenAndMembersSpec entityFilter(Predicate<? super Entity> val) {
            configure(ComputeServiceIndicatorsFromChildrenAndMembers.ENTITY_FILTER, val);
            return self();
        }
    }

    /** provides the default {@link ComputeServiceIndicatorsFromChildrenAndMembers} enricher,
     * using the default unique tag ({@link ComputeServiceIndicatorsFromChildrenAndMembers#DEFAULT_UNIQUE_TAG}),
     * configured here to require none on fire, and either no children or at least one up child,
     * the spec can be further configured as appropriate */
    public static ComputeServiceIndicatorsFromChildrenAndMembersSpec newEnricherFromChildren() {
        return new ComputeServiceIndicatorsFromChildrenAndMembersSpec()
            .uniqueTag(ComputeServiceIndicatorsFromChildrenAndMembers.DEFAULT_UNIQUE_TAG);
    }

    /** as {@link #newEnricherFromChildren()} but only publishing service not-up indicators,
     * using a different unique tag ({@link ComputeServiceIndicatorsFromChildrenAndMembers#DEFAULT_UNIQUE_TAG_UP}),
     * listening to children only, ignoring lifecycle/service-state,
     * and using the same logic
     * (viz looking only at children (not members) and requiring either no children or at least one child up) by default */
    public static ComputeServiceIndicatorsFromChildrenAndMembersSpec newEnricherFromChildrenUp() {
        return newEnricherFromChildren()
            .uniqueTag(ComputeServiceIndicatorsFromChildrenAndMembers.DEFAULT_UNIQUE_TAG_UP)
            .checkChildrenOnly()
            .configure(ComputeServiceIndicatorsFromChildrenAndMembers.DERIVE_SERVICE_PROBLEMS, false);
    }

    /** as {@link #newEnricherFromChildren()} but only publishing service problems,
     * listening to children and members, ignoring service up,
     * and using the same logic
     * (viz looking at children and members and requiring none are on fire) by default */
    public static ComputeServiceIndicatorsFromChildrenAndMembersSpec newEnricherFromChildrenState() {
        return newEnricherFromChildren()
            .configure(ComputeServiceIndicatorsFromChildrenAndMembers.DERIVE_SERVICE_NOT_UP, false);
    }

}
