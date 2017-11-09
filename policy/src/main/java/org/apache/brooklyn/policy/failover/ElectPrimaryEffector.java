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
package org.apache.brooklyn.policy.failover;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.EffectorTasks.EffectorBodyTaskFactory;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceProblemsLogic;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

/**
This effector will scan candidates among children or members to determine which should be noted as "primary".  
The primary is selected from service-up candidates based on a numeric weight as a sensor or config on the candidates 
(`ha.primary.weight`, unless overridden), with higher weights being preferred.  
In the case of ties, or a new candidate emerging with a weight higher than a current healthy primary, 
behaviour can be configured with `primary.selection.mode`.

@return a map containing a message, newPrimary, oldPrimary, and a {@link ResultCode} code.
*/
@Beta
public class ElectPrimaryEffector implements EntityInitializer, ElectPrimaryConfig {

    private static final Logger log = LoggerFactory.getLogger(ElectPrimaryEffector.class);
    
    public static enum ResultCode { PRIMARY_UNCHANGED, NEW_PRIMARY_ELECTED, NO_PRIMARY_AVAILABLE }
    
    public static final Effector<Object> EFFECTOR = Effectors.effector(Object.class, "electPrimary").
        description("Scan to detect whether there is or should be a new primary").buildAbstract();
    
    private final ConfigBag paramsCreationTime;
    
    public ElectPrimaryEffector(ConfigBag params) {
        this.paramsCreationTime = params;
    }
    
    public ElectPrimaryEffector(Map<String,String> params) {
        this(ConfigBag.newInstance(params));
    }
    
    // wire up the entity to call the task factory to create the task on invocation
    
    @Override
    public void apply(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity) {
        ((EntityInternal)entity).getMutableEntityType().addEffector(makeEffector(paramsCreationTime));
    }
    
    public static Effector<Object> makeEffector(ConfigBag params) {
        return Effectors.effector(EFFECTOR).impl(new EffectorBodyTaskFactory<Object>(new ElectPrimaryEffectorBody(params))).build();
    }

    protected static class ElectPrimaryEffectorBody extends EffectorBody<Object> {
        private final ConfigBag paramsCreationTime;

        public ElectPrimaryEffectorBody(ConfigBag paramsCreationTime) {
            this.paramsCreationTime = paramsCreationTime;
        }

        // these are the actual tasks we do
        
        @Override
        public Object call(ConfigBag paramsInvocationTime) {
            ConfigBag params = ConfigBag.newInstanceCopying(paramsCreationTime).copy(paramsInvocationTime);
            
            try {
                Entity newPrimary = DynamicTasks.queue("check primaries", new CheckPrimaries(entity(), params)).getUnchecked();
                
                Entity currentActive = getCurrentActive(entity(), params);
                if (newPrimary==null) {
//                    If no primary can be found, the effector will:
//                        * add a "primary-election" problem so that service state logic, if applicable, will know that the entity is unhealthy
//                        * set service up false
//                        * if the local entity is expected to be RUNNING, it will set actual state to ON_FIRE
//                        * if the local entity has no expectation, it will set actual state to STOPPED
//                        * demote any old primary
                    ServiceProblemsLogic.updateProblemsIndicator(entity(), "primary", "No primary could be found");
                    ServiceNotUpLogic.updateNotUpIndicator(entity(), "primary", "No primary could be found");
                    entity().sensors().set(Attributes.SERVICE_UP, false);
                    if (Lifecycle.RUNNING.equals( ServiceStateLogic.getExpectedState(entity()) )) {
                        entity().sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
                    } else {
                        entity().sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPED);
                    }
                    entity().sensors().set(Sensors.newSensor(Entity.class, params.get(PRIMARY_SENSOR_NAME)), null);
                    
                    DynamicTasks.queue(Tasks.create("demote "+currentActive, new Demote(params)) ).getUnchecked();                
                    return MutableMap.of("code", ResultCode.NO_PRIMARY_AVAILABLE, "message", "No primary available", "primary", null);
                }
                
                if (newPrimary.equals(currentActive)) {
                    // If there is a primary and it is unchanged, the effector will end.
                    return MutableMap.of("code", ResultCode.PRIMARY_UNCHANGED, "message", "No change required", "primary", newPrimary);
                }
                
                log.info("Detected new primary "+newPrimary+" at "+entity()+" (previously had "+currentActive+")");
//                If a new primary is detected, the effector will:
//                    * set the local entity to the STARTING state
//                    * clear any "primary-election" problem
//                    * publish the new primary in a sensor called `primary` (or the sensor set in `primary.sensor.name`)
//                    * cancel any other ongoing promote calls, and if there is an ongoing demote call on the entity being promoted, cancel that also
//                    * in parallel
//                        * invoke `promote` (or the effector called `primary.promote.effector.name`) on the local entity or the entity being promoted
//                        * invoke `demote` (or the effector called `primary.promote.effector.name`) on the local entity or the entity being demoted, if an entity is being demoted
//                    * set service up true
//                    * set the local entity to the RUNNING state
                
                ServiceNotUpLogic.updateNotUpIndicator(entity(), "primary", "Invoking promotion/demotion effectors");
                boolean wasRunning = entity().sensors().get(Attributes.SERVICE_STATE_ACTUAL) == Lifecycle.RUNNING;
                if (wasRunning) {
                    log.debug("Transititioning "+entity()+" to starting while promoting/demoting");
                    ServiceStateLogic.setExpectedState(entity(), Lifecycle.STARTING);
                }
                ServiceProblemsLogic.clearProblemsIndicator(entity(), "primary");
                entity().sensors().set(Sensors.newSensor(Entity.class, params.get(PRIMARY_SENSOR_NAME)), newPrimary);
                try {
                    // TODO cancel other promote/demote calls
        
                    promoteAndDemote(params, currentActive, newPrimary);
                    
                    log.debug("Promoted/demoted primary for "+entity()+", now setting service up "+(wasRunning ? "and running" : "(but not setting as 'running' because it wasn't 'running' before)"));
                    ServiceNotUpLogic.clearNotUpIndicator(entity(), "primary");
                    if (wasRunning) ServiceStateLogic.setExpectedState(entity(), Lifecycle.RUNNING);
                    
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    log.debug("Error promoting/demoting primary for "+entity()+" (rethrowing): "+e);
                    ServiceProblemsLogic.updateProblemsIndicator(entity(), "primary", Exceptions.collapseText(e));
                    ServiceNotUpLogic.clearNotUpIndicator(entity(), "primary");
                    ServiceStateLogic.setExpectedStateRunningWithErrors(entity());
                    throw Exceptions.propagate(e);
                }
    
                return MutableMap.of("code", ResultCode.NEW_PRIMARY_ELECTED, "message", "New primary found", "primary", newPrimary);
                
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);

                if (Entities.isNoLongerManaged(entity())) {
                    // ignore errors if shutting down
                    return "<no-longer-managed>";
                }

                Lifecycle expected = ServiceStateLogic.getExpectedState(entity());
                if (expected==Lifecycle.RUNNING || expected==Lifecycle.STARTING) {
                    // including SelectionModeStrictFailed
                    log.warn("Error electing new primary at "+entity()+": "+Exceptions.collapseText(e));
                    ServiceProblemsLogic.updateProblemsIndicator(entity(), "primary", "Error electing primary: "+
                        Exceptions.collapseText(e));
                    entity().sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
                    
                    throw Exceptions.propagateAnnotated("Error electing primary (when "+expected.toString().toLowerCase()+")", e);
                }
                
                throw Exceptions.propagateAnnotated("Error electing primary (when not starting/running)", e);
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        protected void promoteAndDemote(ConfigBag params, Entity oldPrimary, Entity newPrimary) {
            params.configureStringKey("oldPrimary", oldPrimary);
            params.configureStringKey("newPrimary", newPrimary);
            MutableList<Task> tasks = MutableList.<Task>of();
            
            if (newPrimary!=null) tasks.append(Tasks.create("promote "+newPrimary, new Promote(params)));
            else tasks.append(Tasks.warning("No new primary; nothing to promote", null, false));
            
            if (oldPrimary!=null) tasks.append(Tasks.create("demote "+oldPrimary, new Demote(params)));
            else tasks.append(Tasks.warning("No old primary; nothing to demote", null, false));
            
            log.debug("Running "+tasks);
            List<?> result = DynamicTasks.queue(Tasks.parallel("promote/demote", (List<Task<?>>)(List) tasks)).getUnchecked();
            log.debug("Ran "+tasks+", results: "+result);
        }
        
        protected class Promote implements Callable<Object> {
            final ConfigBag params;
            public Promote(ConfigBag params) { this.params = params; }

            @Override
            public Object call() throws Exception {
                String promoteEffectorName = params.get(PROMOTE_EFFECTOR_NAME);
                Effector<?> eff = entity().getEffector(promoteEffectorName);
                if (eff!=null) {
                    return DynamicTasks.queue( Effectors.invocation(entity(), eff, params) ).asTask().getUnchecked();
                }
                EntityInternal newPrimary = (EntityInternal)params.getStringKey("newPrimary");
                if (newPrimary==null) {
                    return "Nothing to promote; no new primary";
                }
                eff = newPrimary.getEffector(promoteEffectorName);
                if (eff!=null) {
                    return DynamicTasks.queue( Effectors.invocation(newPrimary, eff, params) ).asTask().getUnchecked();
                }
                if (params.containsKey(PROMOTE_EFFECTOR_NAME)) {
                    throw new IllegalStateException("Key "+PROMOTE_EFFECTOR_NAME.getName()+" set as "+promoteEffectorName+
                        " but that effector isn't available on this entity or new primary "+newPrimary);
                }
                return "No promotion effector '"+promoteEffectorName+"'; nothing to do";
            }
        }
        
        protected class Demote implements Callable<Object> {
            final ConfigBag params;
            public Demote(ConfigBag params) { this.params = params; }

            @Override
            public Object call() throws Exception {
                String demoteEffectorName = params.get(DEMOTE_EFFECTOR_NAME);
                Effector<?> eff = entity().getEffector(demoteEffectorName);
                if (eff!=null) {
                    return DynamicTasks.queue( Effectors.invocation(entity(), eff, params) ).asTask().getUnchecked();
                }
                EntityInternal oldPrimary = (EntityInternal)params.getStringKey("oldPrimary");
                if (oldPrimary==null) {
                    return "Nothing to demote; no old primary";
                }
                eff = oldPrimary.getEffector(demoteEffectorName);
                if (eff!=null) {
                    return DynamicTasks.queue( Effectors.invocation(oldPrimary, eff, params) ).asTask().getUnchecked();
                }
                if (params.containsKey(DEMOTE_EFFECTOR_NAME)) {
                    throw new IllegalStateException("Key "+DEMOTE_EFFECTOR_NAME.getName()+" set as "+demoteEffectorName+
                        " but that effector isn't available on this entity or old primary "+oldPrimary);
                }
                return "No demotion effector '"+demoteEffectorName+"'; nothing to do";
            }
        }
    }


    @VisibleForTesting
    public static class CheckPrimaries implements Callable<Entity> {
        final ConfigBag params;
        final Entity entity;
        public CheckPrimaries(Entity entity, ConfigBag params) { this.entity = entity; this.params = params; }

        @Override
        public Entity call() throws Exception {
            Stopwatch elapsedTime = Stopwatch.createStarted();
            boolean extendedForNonViableRunning = false;
            outer: while (true) {
                TargetMode target = params.get(TARGET_MODE);
                Iterable<Entity> candidates = 
                    target==TargetMode.CHILDREN ? entity.getChildren() :
                        target==TargetMode.MEMBERS ? ((Group)entity).getMembers() :
                            // auto - prefer members
                            entity instanceof Group ? ((Group)entity).getMembers() : entity.getChildren();
                
                SelectionMode mode = params.get(SELECTION_MODE);
                Entity currentActive = getCurrentActive(entity, params);
                if (mode==SelectionMode.FAILOVER && currentActive!=null && Iterables.contains(candidates, currentActive) && isViable(currentActive)) {
                    return currentActive;
                }
                // find best live and/or time to wait
                Duration delayForBest = Duration.ZERO;
                Duration period = Duration.millis(10);
                
                if (candidates.iterator().hasNext()) {
                    List<WeightedEntity> weightedEntities = MutableList.of();
                    for (Entity candidate: candidates) {
                        weightedEntities.add(new WeightedEntity(candidate));
                    }
                    Collections.sort(weightedEntities);
                    WeightedEntity anyBestTheoreticalW = weightedEntities.iterator().next();
                    WeightedEntity bestLive = null;
                    for (WeightedEntity w: weightedEntities) {
                        if (w.score < -0.00001) {
                            // this is disallowed, as are all following
                            break;
                        }
                        if (Math.abs(anyBestTheoreticalW.score - w.score) >= 0.0000000001) {
                            if (bestLive==null && !delayForBest.isLongerThan(elapsedTime)) {
                                // no viable highest-score and can't wait any longer;
                                // take the next highest that we find
                                if (isViable(w.entity)) {
                                    log.debug("Theoretical best primary at "+entity+" ("+anyBestTheoreticalW+", maybe others) not available, using next best: "+w);
                                    return w.entity;
                                } else {
                                    continue;
                                }
                            } else {
                                // don't bother looking at non-highest scoring nodes, we know what to do
                                break;
                            }
                        }
                        
                        // examining the highest scoring ones
                        
                        if (isViable(w.entity)) {
                            log.debug("Viable best primary at "+entity+" detected: "+w);
                            if (bestLive==null) {
                                bestLive = w;
                            } else if (mode==SelectionMode.STRICT) {
                                // in strict mode, should only have one node with the best score,
                                // so this is a failure. we might want to allow some grace if a
                                // method is promoting a new one by changing (non-atomically) which
                                // single entity has a weight 1 where others are 0.
                                // but preferred way to avoid that is to increment weight at new primary instead.
                                throw new SelectionModeStrictFailedException(w.entity, bestLive.entity, bestLive.score);
                            } else if (w.entity.equals(currentActive)) {
                                // always prefer current active if it is best
                                // (safe to bail out here but we won't yet)
                                bestLive = w;
                            } else {
                                // two equally good bests, neither current active
                                // could prefer either but go with the first (no-op here) 
                            }
                            
                        } else {
                            // this best not viable - determine why and how long to wait
                            Lifecycle state = w.entity.getAttribute(Attributes.SERVICE_STATE_ACTUAL);
                            log.debug("Theoretical best primary at "+entity+": "+w.entity+" "+(state==null ? "<no-state-yet>" : state)+" (not viable); may re-check");
                            if (state==Lifecycle.STARTING) {
                                delayForBest = Duration.max(delayForBest, entity.config().get(BEST_STARTING_WAIT_TIMEOUT));
                                
                            } else if (state==Lifecycle.RUNNING) {
                                
                                // running, not on fire, but not viable - either a race (has just become viable)
                                // or a coding error (caller did not set "up" correctly); give extra 5s if we haven't already
                                if (!extendedForNonViableRunning) {
                                    delayForBest = Duration.max(delayForBest, Duration.of(elapsedTime).add(entity.config().get(BEST_WAIT_TIMEOUT)));
                                    extendedForNonViableRunning = true;
                                }
                            } else {
                                delayForBest = Duration.max(delayForBest, entity.config().get(BEST_WAIT_TIMEOUT));
                            }
                        }
                    }
                    
                    // finished looking at all nodes, or all theoretical bests and found one
                    if (bestLive!=null) {
                        // found a best live (preferring current if viable,
                        // but doesn't wait for current)
                        return bestLive.entity;
                    }
                } else {
                    delayForBest = entity.config().get(BEST_WAIT_TIMEOUT);
                }
                Duration delay = delayForBest.subtract(Duration.of(elapsedTime));
                if (delay.isPositive()) {
                    delay = Duration.min(delay, period);
                    period = Duration.min(Duration.ONE_SECOND, period.multiply(1.5));
                    log.debug("Delaying "+delay+" ("+delayForBest+" allowed, "+Duration.of(elapsedTime)+" elapsed) then rechecking for best primary at "+entity);
                    // there was a theoretical best that wasn't started
                    Time.sleep(delay);
                    continue outer;
                }

                // none viable or worth waiting for
                return null;
            }
        }

        private class WeightedEntity implements Comparable<WeightedEntity> {
            public final Entity entity;
            public final double score;
            
            public WeightedEntity(Entity candidate) {
                this.entity = candidate;
                this.score = score(candidate);
            }

            @Override
            public int compareTo(WeightedEntity o) {
                double v = o.score - this.score;
                return (v > 0.00000001 ? 1 : v<-0.00000001 ? -1 : 0);
            }
            @Override
            public String toString() {
                return entity+":"+score;
            }
        }
        
        protected boolean isViable(Entity candidate) {
            if (!Lifecycle.RUNNING.equals( candidate.getAttribute(Attributes.SERVICE_STATE_ACTUAL) )) return false;
            if (!Boolean.TRUE.equals( candidate.getAttribute(Attributes.SERVICE_UP) )) return false;
            if (score(candidate) <= -0.000000001) return false;
            return true;
        }
        
        private double score(Entity candidate) {
            Double s = candidate.getAttribute(Sensors.newDoubleSensor(params.get(PRIMARY_WEIGHT_NAME)));
            if (s!=null) return s;
            s = candidate.getConfig(ConfigKeys.newDoubleConfigKey(params.get(PRIMARY_WEIGHT_NAME)));
            if (s!=null) return s;
            return 0;
        }
    }

    private static Entity getCurrentActive(Entity entity, ConfigBag params) {
        return entity.getAttribute(Sensors.newSensor(Entity.class, params.get(PRIMARY_SENSOR_NAME)));
    }

    private static class SelectionModeStrictFailedException extends UserFacingException {
        private static final long serialVersionUID = -6253854814553229953L;
        
        // fields not needed at present
//        final Entity entity1;
//        final Entity entity2;
//        final double score;

        public SelectionModeStrictFailedException(Entity entity1, Entity entity2, double score) {
            super("Cannot select primary in strict mode: entities "+entity1+" and "+entity2+" have same score "+score);
//            this.entity1 = entity1;
//            this.entity2 = entity2;
//            this.score = score;
        }
    }
}
