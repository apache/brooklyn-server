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
package org.apache.brooklyn.core.effector.script;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;

public class ScriptEffectorTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testAddJavaScriptEffector() {
        BasicEntity entity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new ScriptEffector(ConfigBag.newInstance(ImmutableMap.of(
                        AddEffector.EFFECTOR_NAME, "javaScriptEffector",
                        ScriptEffector.EFFECTOR_SCRIPT_LANGUAGE, "js",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_VAR, "o",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_TYPE, String.class,
                        ScriptEffector.EFFECTOR_SCRIPT_CONTENT, "var o; o = \"jsval\";")))));
        Maybe<Effector<?>> javaScriptEffector = entity.getEntityType().getEffectorByName("javaScriptEffector");
        Assert.assertTrue(javaScriptEffector.isPresentAndNonNull(), "The JavaScript effector does not exist");
        Object result = Entities.invokeEffector(entity, entity, javaScriptEffector.get()).getUnchecked();
        Assert.assertTrue(result instanceof String, "Returned value is of type String");
        Assert.assertEquals(result, "jsval", "Returned value is not correct");
    }

    @Test
    public void testAddPythonEffector() {
        BasicEntity entity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new ScriptEffector(ConfigBag.newInstance(ImmutableMap.of(
                        AddEffector.EFFECTOR_NAME, "pythonEffector",
                        ScriptEffector.EFFECTOR_SCRIPT_LANGUAGE, "python",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_VAR, "o",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_TYPE, String.class,
                        ScriptEffector.EFFECTOR_SCRIPT_CONTENT, "o = \"pyval\"")))));
        Maybe<Effector<?>> pythonEffector = entity.getEntityType().getEffectorByName("pythonEffector");
        Assert.assertTrue(pythonEffector.isPresentAndNonNull(), "The Python effector does not exist");
        Object result = Entities.invokeEffector(entity, entity, pythonEffector.get()).getUnchecked();
        Assert.assertTrue(result instanceof String, "Returned value is of type String");
        Assert.assertEquals(result, "pyval", "Returned value is not correct");
    }
}
