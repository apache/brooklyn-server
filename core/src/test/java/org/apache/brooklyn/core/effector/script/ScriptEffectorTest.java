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
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;

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
        Assert.assertTrue(javaScriptEffector.isPresentAndNonNull(), "The effector does not exist");
        Object result = Entities.invokeEffector(entity, entity, javaScriptEffector.get()).getUnchecked();
        Assert.assertTrue(result instanceof String, "Returned value is not of type String");
        Assert.assertEquals(result, "jsval", "Returned value is not correct");
    }

    @Test
    public void testJavaScriptEffectorEntityAccess() {
        BasicEntity entity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new ScriptEffector(ConfigBag.newInstance(ImmutableMap.of(
                        AddEffector.EFFECTOR_NAME, "entityEffector",
                        ScriptEffector.EFFECTOR_SCRIPT_LANGUAGE, "js",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_VAR, "a",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_TYPE, Entity.class,
                        ScriptEffector.EFFECTOR_SCRIPT_CONTENT, "var a; a = entity.getApplication();")))));
        Maybe<Effector<?>> entityEffector = entity.getEntityType().getEffectorByName("entityEffector");
        Assert.assertTrue(entityEffector.isPresentAndNonNull(), "The effector does not exist");
        Object result = Entities.invokeEffector(entity, entity, entityEffector.get()).getUnchecked();
        Assert.assertTrue(result instanceof Entity, "Returned value is not of type Entity");
        Assert.assertEquals(result, app, "Returned value is not correct");
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Script language not supported: ruby")
    public void testAddRubyEffector() {
        app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new ScriptEffector(ConfigBag.newInstance(ImmutableMap.of(
                        AddEffector.EFFECTOR_NAME, "rubyEffector",
                        ScriptEffector.EFFECTOR_SCRIPT_LANGUAGE, "ruby",
                        ScriptEffector.EFFECTOR_SCRIPT_CONTENT, "print \"Ruby\\n\"")))));
    }

    // TODO requires python.path to be set as JVM property
    @Test(enabled = false)
    public void testAddPythonEffector() {
        // System.setProperty("python.path", "/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/");
        BasicEntity entity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new ScriptEffector(ConfigBag.newInstance(ImmutableMap.of(
                        AddEffector.EFFECTOR_NAME, "pythonEffector",
                        ScriptEffector.EFFECTOR_SCRIPT_LANGUAGE, "jython",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_VAR, "o",
                        ScriptEffector.EFFECTOR_SCRIPT_RETURN_TYPE, String.class,
                        ScriptEffector.EFFECTOR_SCRIPT_CONTENT, "o = \"pyval\"")))));
        Maybe<Effector<?>> pythonEffector = entity.getEntityType().getEffectorByName("pythonEffector");
        Assert.assertTrue(pythonEffector.isPresentAndNonNull(), "The effector does not exist");
        Object result = Entities.invokeEffector(entity, entity, pythonEffector.get()).getUnchecked();
        Assert.assertTrue(result instanceof String, "Returned value is not of type String");
        Assert.assertEquals(result, "pyval", "Returned value is not correct");
    }
}
