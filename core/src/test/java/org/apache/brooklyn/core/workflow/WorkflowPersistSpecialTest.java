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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializer;
import org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializerTest;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

public class WorkflowPersistSpecialTest extends RebindTestFixture<BasicApplication> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowPersistSpecialTest.class);

    @Override
    protected BasicApplication createApp() {
        return mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
    }

    @Override
    protected LocalManagementContext decorateOrigOrNewManagementContext(LocalManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        return super.decorateOrigOrNewManagementContext(mgmt);
    }

    @Override protected BasicApplication rebind() throws Exception {
        return rebind(RebindOptions.create().terminateOrigManagementContext(true));
    }

    @Test
    public void testSerializeWorkflowWithError() throws IOException {
        BasicApplication app = createApp();
        XmlMementoSerializer<Object> xml = XmlMementoSerializer.XmlMementoSerializerBuilder.from(mgmt()).build();
        xml.setLookupContext(new XmlMementoSerializerTest.LookupContextTestImpl("testing", mgmt(), true));
        ObjectMapper json = BeanWithTypeUtils.newMapper(mgmt(), false, null, true);
        WorkflowExecutionContext wc;
        StringWriter sw;
        String s;

        // read legacy
        s = ResourceUtils.create(this).getResourceAsString(getClass().getPackage().getName().replaceAll("\\.", "/")+"/workflow-with-error-with-trace.xml");
        s = Strings.replaceAll(s, "__ENTITY_ID__", app.getId());
        Asserts.assertStringContains(s, "<error ");
        Asserts.assertStringContains(s, "<trace>org.apache.brooklyn.core.workflow.WorkflowExecutionContext$Body.call");
        wc = (WorkflowExecutionContext) xml.deserialize(new StringReader(s));
        Asserts.assertInstanceOf(wc.getCurrentStepInstance().getError(), PropagatedRuntimeException.class);
        Asserts.assertNotNull(wc.getCurrentStepInstance().getError().getCause());
        Asserts.assertNotNull(wc.getEntity());

        // write above legacy, assert new format
        sw = new StringWriter();
        xml.serialize(wc, sw);
        Asserts.assertStringDoesNotContain(sw.toString(), "<error ");
        Asserts.assertStringDoesNotContain(sw.toString(), "<trace>org.apache.brooklyn.core.workflow.WorkflowExecutionContext$Body.call");

        // and json can write and read
        s = json.writeValueAsString(wc);
        Asserts.assertStringContains(s, "\"error\":\"WorkflowFailException: ");
        Asserts.assertStringDoesNotContain(s, "org.apache.brooklyn.core.workflow.WorkflowExecutionContext$Body.call");
        Asserts.assertStringDoesNotContain(s, "\"errorRecord\":");
        wc = json.readValue(s, WorkflowExecutionContext.class);
        Asserts.assertNotNull(wc.getEntity());
        Asserts.assertEquals(wc.getCurrentStepInstance().getError().getClass(), RuntimeException.class);
        Asserts.assertNull(wc.getCurrentStepInstance().getError().getCause());

        // write
        wc = WorkflowBasicTest.runWorkflow(app, "steps: [ fail message Testing failure ]", "test-failure");
        wc.getTaskSkippingCondition().get().blockUntilEnded();
        Asserts.assertNotNull(wc.getEntity());
        Asserts.assertNotNull(wc.getCurrentStepInstance().getError().getCause());

        /* the 'error' field, with its trace, is no longer persisted via xstream; we store a simpler record instead.
        *  the reason for this is that many errors include context objects which don't serialize well and cannot otherwise be intercepted.
        *  the exception is if the exception implement serializable.  */
        sw = new StringWriter();
        xml.serialize(wc, sw);
        Asserts.assertStringDoesNotContain(sw.toString(), "<error ");
        Asserts.assertStringDoesNotContain(sw.toString(), "<trace>org.apache.brooklyn.core.workflow.WorkflowExecutionContext$Body.call");
        wc = (WorkflowExecutionContext) xml.deserialize(new StringReader(sw.toString()));
        Asserts.assertNotNull(wc.getEntity());
        Asserts.assertEquals(wc.getCurrentStepInstance().getError().getClass(), RuntimeException.class);
        Asserts.assertNull(wc.getCurrentStepInstance().getError().getCause());
    }

}
