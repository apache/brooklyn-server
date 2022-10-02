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
package org.apache.brooklyn.rest.resources;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils.CreationResult;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowPersistReplayErrorsTest;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/** Tests {@link EntityResource} workflow methods */
public class EntityWorkflowsRestTest extends BrooklynRestResourceTest {

    private static final Logger log = LoggerFactory.getLogger(EntityWorkflowsRestTest.class);

    private Entity entity;
    private Effector<?> effector;

    private Task<?> lastTask;

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer();
        WorkflowBasicTest.addWorkflowStepTypes(getManagementContext());
    }
    
    @BeforeMethod(alwaysRun = true)
    public void setUpOneTest() throws Exception {
        initEntity();
    }

    @SuppressWarnings("deprecation")
    protected void initEntity() {
        if (entity != null && Entities.isManaged(entity)) {
            Entities.destroy(entity.getApplication(), true);
        }
        
        CreationResult<BasicApplication, Void> app = EntityManagementUtils.createStarting(getManagementContext(),
            EntitySpec.create(BasicApplication.class)
                .child(EntitySpec.create(TestEntity.class)) );
        app.blockUntilComplete();

        entity = Iterables.getOnlyElement( app.get().getChildren() );

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "increment-x")
                .configure(WorkflowEffector.STEPS, WorkflowPersistReplayErrorsTest.INCREMENTING_X_STEPS)
        );
        eff.apply((EntityLocal) entity);
        effector = Asserts.assertPresent(entity.getEntityType().getEffectorByName("increment-x"));
    }

    private void assertHealthy(Response response) {
        if (!HttpAsserts.isHealthyStatusCode(response.getStatus())) {
            Asserts.fail("Bad response: "+response.getStatus()+" "+response.readEntity(String.class));
        }
    }

    @Test
    public void testEntityWorkflow() {
        lastTask = entity.invoke(effector, null);
        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newSensor(Object.class, "x"), 1);
        // get workflow, not finished

        Response response = client().path("/applications/"+entity.getApplicationId()+"/entities/"+entity.getId()+"/workflows")
                .accept(MediaType.APPLICATION_JSON).get();
        assertHealthy(response);
        Map<String, WorkflowExecutionContext> workflows = response.readEntity(new GenericType<Map<String,WorkflowExecutionContext>>(new TypeToken<Map<String,WorkflowExecutionContext>>() {}.getType()) {});
        WorkflowExecutionContext wf1 = Iterables.getOnlyElement(workflows.values());
        Asserts.assertEquals(wf1.getStatus(), WorkflowExecutionContext.WorkflowStatus.RUNNING);

        entity.sensors().set(Sensors.newBooleanSensor("gate"), true);
        lastTask.getUnchecked();

        // get workflow now and assert finished
        response = client().path("/applications/"+entity.getApplicationId()+"/entities/"+entity.getId()+"/workflow/"+wf1.getWorkflowId())
                .accept(MediaType.APPLICATION_JSON).get();
        assertHealthy(response);
        WorkflowExecutionContext wf2 = response.readEntity(WorkflowExecutionContext.class);
        Asserts.assertEquals(wf2.getStatus(), WorkflowExecutionContext.WorkflowStatus.SUCCESS);
    }
    
}
