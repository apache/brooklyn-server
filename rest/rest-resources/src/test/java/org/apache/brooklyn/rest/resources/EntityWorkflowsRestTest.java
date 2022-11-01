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

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowPersistReplayErrorsTest;
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.text.Strings;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        List<WorkflowExecutionContext> workflows = response.readEntity(new GenericType<List<WorkflowExecutionContext>>(new TypeToken<List<WorkflowExecutionContext>>() {}.getType()) {});
        WorkflowExecutionContext wf1 = Iterables.getOnlyElement(workflows);
        Asserts.assertEquals(wf1.getStatus(), WorkflowExecutionContext.WorkflowStatus.RUNNING);

        entity.sensors().set(Sensors.newBooleanSensor("gate"), true);
        lastTask.getUnchecked();

        // get workflow now and assert finished
        response = client().path("/applications/"+entity.getApplicationId()+"/entities/"+entity.getId()+"/workflows/"+wf1.getWorkflowId())
                .accept(MediaType.APPLICATION_JSON).get();
        assertHealthy(response);
        WorkflowExecutionContext wf2 = response.readEntity(WorkflowExecutionContext.class);
        Asserts.assertEquals(wf2.getStatus(), WorkflowExecutionContext.WorkflowStatus.SUCCESS);
    }

    @Test(groups="Integration")  // because slow
    public void testWorkflowApiTimeouts() {
        String wf = "input:\n" +
                "  password1: PASSWORD\n" +
                "steps:\n" +
                "  - sleep 500ms\n"+
                "  - return done\n";

        Response response = client().path("/applications/"+entity.getApplicationId()+"/entities/"+entity.getId()+"/workflows")
                .header(HttpHeaders.CONTENT_TYPE, "text/yaml")
                .accept(MediaType.APPLICATION_JSON).post(wf);
        assertHealthy(response);
        TaskSummary task = response.readEntity(TaskSummary.class);

        // not done yet
        response = client().path("/activities/"+task.getId())
                .accept(MediaType.APPLICATION_JSON).get();
        assertHealthy(response);
        task = response.readEntity(TaskSummary.class);
        Asserts.assertNull(task.getEndTimeUtc());
        Asserts.assertEquals(response.getStatus(), 202);
        Asserts.assertNull( task.getResult() );

        // now it will be done
        response = client().path("/activities/"+task.getId()).query("timeout", "5s")
                .accept(MediaType.APPLICATION_JSON).get();
        assertHealthy(response);
        Asserts.assertEquals(response.getStatus(), 200);
        task = response.readEntity(TaskSummary.class);
        Asserts.assertNotNull(task.getEndTimeUtc());
        Asserts.assertEquals(task.getResult(), "done");
    }

    @Test
    public void testWorkflowSecretsVisible() throws IOException {
        doTestWorkflowSecrets(false);
    }

    @Test
    public void testWorkflowSecretsSanitized() throws IOException {
        doTestWorkflowSecrets(true);
    }

    void doTestWorkflowSecrets(boolean sanitized) throws IOException {
        String wf = "input:\n" +
                "  password1: PASSWORD\n" +
                "steps:\n" +
                "  - input:\n" +
                "      public: ${password1}0\n" +
                "      password2: ${password1}2\n" +
                "    step: log EXPECTED password2 is ${password2} and public ${public} which are permitted displays of a pseudo-secret, but should be the only one\n" +
                "    output:\n" +
                "      password3: ${password1}3\n";

        try (ClassLogWatcher logWatcher = new ClassLogWatcher("org.apache.brooklyn.core.workflow")) {

            Response response = client().path("/applications/" + entity.getApplicationId() + "/entities/" + entity.getId() + "/workflows")
                    .header(HttpHeaders.CONTENT_TYPE, "text/yaml")
                    .accept(MediaType.APPLICATION_JSON).post(wf);
            assertHealthy(response);
            TaskSummary task = response.readEntity(TaskSummary.class);

            response = client().path("/activities/" + task.getId()).query("timeout", "30s")
                    .query("suppressSecrets", sanitized)
                    .accept(MediaType.APPLICATION_JSON).get();
            assertHealthy(response);
            Asserts.assertEquals(response.getStatus(), 200);
            task = response.readEntity(TaskSummary.class);
            Asserts.assertNotNull(task.getEndTimeUtc());
            Asserts.assertInstanceOf(task.getResult(), Map.class);

            // password not in result
            if (!sanitized) {
                Asserts.assertThat(((Map) task.getResult()).get("password3"), s -> "PASSWORD3".equals(s));
            } else {
                Asserts.assertThat(((Map) task.getResult()).get("password3"), s -> s.toString().contains("suppressed") && !s.toString().contains("PASSWORD"));
            }

            Function<Object,String> yamlCheck = obj -> {
                // no other mention of password
                String yaml = null;
                try {
                    yaml = BeanWithTypeUtils.newYamlMapper(manager, false, null, false).writeValueAsString(obj);
                } catch (JsonProcessingException e) {
                    throw Exceptions.propagate(e);
                }

                // should only be needed if log output is included
                yaml = Strings.replaceAllRegex(yaml, "(?s)"+"EXPECTED password2 is PASSWORD2 .* pseudo-secret", "REMOVED");

                if (sanitized) {
                    // no mention of password value, if sanitized (affects workflow_yaml tag)
                    Asserts.assertThat(yaml, s -> !s.contains("PASSWORD"));
                } else {
                    Asserts.assertThat(yaml, s -> s.contains("PASSWORD"));
                }
                return yaml;
            };

            yamlCheck.apply(task);

            response = client().path("/applications/" + entity.getApplicationId() + "/entities/" + entity.getId() + "/workflows/"+task.getId())
                    .query("suppressSecrets", sanitized)
                    .accept(MediaType.APPLICATION_JSON).get();
            String wecS = response.readEntity(String.class);
            yamlCheck.apply(wecS);

            response = client().path("/applications/" + entity.getApplicationId() + "/entities/" + entity.getId() + "/workflows/"+task.getId())
                    .query("suppressSecrets", sanitized)
                    .accept(MediaType.APPLICATION_JSON).get();
            WorkflowExecutionContext wec = response.readEntity(WorkflowExecutionContext.class);
            yamlCheck.apply(wec);

            response = client().path("/applications/" + entity.getApplicationId() + "/entities/" + entity.getId() + "/workflows")
                    .query("suppressSecrets", sanitized)
                    .accept(MediaType.APPLICATION_JSON).get();
            List<WorkflowExecutionContext> wecL = response.readEntity(new GenericType<List<WorkflowExecutionContext>>() {});
            yamlCheck.apply(wecL);

            // no secrets from inputs or outputs are logged (apart from expected)
            List<String> linesWithPassword = logWatcher.getMessages().stream().filter(x -> x.toLowerCase().contains("password")).collect(Collectors.toList());
            Asserts.assertSize(linesWithPassword, 1);
            Asserts.assertStringContainsIgnoreCase(linesWithPassword.iterator().next(), "expected", "pseudo-secret");
        }
    }
    
}
