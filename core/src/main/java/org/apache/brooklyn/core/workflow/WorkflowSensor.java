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
package org.apache.brooklyn.core.workflow;

import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddSensorInitializer;
import org.apache.brooklyn.core.effector.AddSensorInitializerAbstractProto;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.AbstractAddTriggerableSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.steps.appmodel.EntityValueToSet;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/** 
 * Configurable {@link EntityInitializer} which adds a sensor feed running a given workflow.
 */
@Beta
public final class WorkflowSensor<T> extends AbstractAddTriggerableSensor<T> implements WorkflowCommonConfig {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowSensor.class);

    // override to be object
    public static final ConfigKey<String> SENSOR_TYPE = ConfigKeys.newConfigKeyWithDefault(AddSensorInitializer.SENSOR_TYPE, Object.class.getName());

    public static final ConfigKey<EntityValueToSet> SENSOR = ConfigKeys.newConfigKey(EntityValueToSet.class, "sensor");

    public static final ConfigKey<Map<String,Object>> INPUT = WorkflowCommonConfig.INPUT;
    public static final ConfigKey<List<Object>> STEPS = WorkflowCommonConfig.STEPS;

    // do we need to have an option not to run when initialization is done?

    public WorkflowSensor() {}
    public WorkflowSensor(ConfigBag params) {
        super(params);
    }

    @Override
    public void apply(final EntityLocal entity) {
        ConfigBag params = initParams();

        // previously if a commandUrl was used we would listen for the install dir to be set; but that doesn't survive rebind;
        // now we install on first run as part of the SshFeed
        apply(entity, params);
    }

    private void apply(final EntityLocal entity, final ConfigBag params) {
        AttributeSensor<T> sensor = addSensor(entity);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding workflow sensor {} to {}", sensor.getName(), entity);
        }

        WorkflowPollCallable wc = new WorkflowPollCallable("Workflow for sensor " + sensor.getName(), params, null);
        FunctionPollConfig<Object,Object> pollConfig = new FunctionPollConfig<Object,T>(sensor)
                .callable(wc)
                .onSuccess(TypeCoercions.<T>function((Class)sensor.getTypeToken().getRawType()));

        standardPollConfig(entity, initParams(), pollConfig);

        FunctionFeed.Builder feedBuilder = FunctionFeed.builder()
                .name("Sensor Workflow Feed: "+sensor.getName())
                .entity(entity)
                .onlyIfServiceUp(Maybe.ofDisallowingNull(EntityInitializers.resolve(params, ONLY_IF_SERVICE_UP)).or(false))
                .poll(pollConfig);

        FunctionFeed feed = feedBuilder.build();
        wc.init(feed);
        entity.addFeed(feed);
    }

    @Override
    protected AttributeSensor<T> sensor(Entity entity) {
        // overridden because sensor type defaults to object here; and we support 'sensor'
        EntityValueToSet s = lookupSensorReference(entity);
        // could assert compatibility with defined sensor, or tighten; but currently we don't
        TypeToken<T> clazz = s.type == null ? (TypeToken)TypeToken.of(Object.class) : getType(entity, s.type, s.name);
        return Sensors.newSensor(clazz, s.name);
    }

    @Override
    protected TypeToken<T> getType(Entity entity, String className) {
        return getType(entity, className, lookupSensorReference(entity).name);
    }

    static <T> TypeToken<T> getType(Entity entity, String className, String sensorName) {
        return AddSensorInitializerAbstractProto.getType(entity, className, sensorName);
    }

    protected EntityValueToSet lookupSensorReference(Entity entity) {
        // overridden because sensor type defaults to object here; and we support 'sensor'
        EntityValueToSet s = initParam(SENSOR);
        if (s==null) s = new EntityValueToSet();

        if (s.entity!=null && !s.entity.equals(entity)) throw new IllegalArgumentException("Not permitted to specify different entity ("+s.entity+") for workflow-sensor on "+entity);

        String t2 = initParam(SENSOR_TYPE);
        if (Object.class.getName().equals(t2)) t2 = null;
        if (s.type!=null && t2!=null && !t2.equals(s.type)) throw new IllegalArgumentException("Incompatible types "+s.type+" and "+t2);
        if (t2!=null) s.type = t2;

        String n2 = initParam(SENSOR_NAME);
        if (s.name!=null && n2!=null && !n2.equals(s.name)) throw new IllegalArgumentException("Incompatible names "+s.name+" and "+n2);
        if (n2!=null) s.name = n2;
        return s;
    }

    static class WorkflowPollCallable implements Callable<Object> {
        private final String workflowCallableName;
        private BrooklynObject entityOrAdjunct;
        private final Map<String,Object> params;

        protected WorkflowPollCallable(String workflowCallableName, ConfigBag params, BrooklynObject entityOrAdjunct) {
            this.workflowCallableName = workflowCallableName;
            this.params = params.getAllConfigRaw();
            this.entityOrAdjunct = entityOrAdjunct;
        }

        /** used in some places where the entity/adjunct is set late */
        public void init(BrooklynObject entityOrAdjunct) {
            this.entityOrAdjunct = entityOrAdjunct;
        }

        @Override
        public Object call() throws Exception {
            BrooklynObject entityOrAdjunct = this.entityOrAdjunct;
            if (entityOrAdjunct==null) entityOrAdjunct = BrooklynTaskTags.getContextEntityAdjunct(Tasks.current(), false);
            if (entityOrAdjunct==null) entityOrAdjunct = BrooklynTaskTags.getContextEntity(Tasks.current());
            if (entityOrAdjunct==null) throw new IllegalStateException("No entity adjunct or entity available for "+this);

            WorkflowExecutionContext wc = WorkflowExecutionContext.newInstancePersisted(entityOrAdjunct, WorkflowExecutionContext.WorkflowContextType.SENSOR,
                    workflowCallableName, ConfigBag.newInstance(params), null, null, null);
            Task<Object> wt = wc.getTask(false /* condition checked by poll config framework */).get();
            if (entityOrAdjunct instanceof EntityAdjunct) {
                // add tag to each task so it shows up in list on mgmt context
                BrooklynTaskTags.addTagDynamically(wt, BrooklynTaskTags.tagForContextAdjunct((EntityAdjunct) entityOrAdjunct));
            }

            if (entityOrAdjunct instanceof WorkflowPolicy) {
                ((WorkflowPolicy)entityOrAdjunct).highlightAction("Workflow running", wt);
            }
            try {
                Object result = DynamicTasks.queue(wt).getUnchecked();

                if (entityOrAdjunct instanceof WorkflowPolicy) {
                    ((WorkflowPolicy)entityOrAdjunct).highlightAction("Workflow run (success)", wt);
                }

                return result;
            } catch (Exception e) {
                if (entityOrAdjunct instanceof WorkflowPolicy) {
                    ((WorkflowPolicy)entityOrAdjunct).highlightAction("Workflow run, with error: "+
                            Exceptions.collapseText(e), wt);
                }

                throw e;
            }
        }

        @Override
        public String toString() {
            return "WorkflowPollCallable{" +
                    "workflowCallableName='" + workflowCallableName + '\'' +
                    ", context=" + entityOrAdjunct +
                    ", params=" + params +
                    '}';
        }
    }
}
