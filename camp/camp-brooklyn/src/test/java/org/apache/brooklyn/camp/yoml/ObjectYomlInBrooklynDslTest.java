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
package org.apache.brooklyn.camp.yoml;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsAtTopLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class ObjectYomlInBrooklynDslTest extends AbstractYamlTest {

    private static final Logger log = LoggerFactory.getLogger(ObjectYomlInBrooklynDslTest.class);
    
    private BasicBrooklynTypeRegistry registry() {
        return (BasicBrooklynTypeRegistry) mgmt().getTypeRegistry();
    }
    
    private void add(RegisteredType type) {
        add(type, false);
    }
    private void add(RegisteredType type, boolean canForce) {
        registry().addToLocalUnpersistedTypeRegistry(type, canForce);
    }
    
    @YomlAllFieldsAtTopLevel
    public static class ItemA {
        String name;
        /* required for 'object.fields' */ public void setName(String name) { this.name = name; }
        @Override public String toString() { return super.toString()+"[name="+name+"]"; }
    }
    
    private final static RegisteredType SAMPLE_TYPE = BrooklynYomlTypeRegistry.newYomlRegisteredType(
        RegisteredTypeKind.BEAN, null, "1", ItemA.class);

    void doTest(String ...lines) throws Exception {
        add(SAMPLE_TYPE);
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.obj:");
        yaml += Joiner.on("\n      ").join("", "", (Object[])lines);

        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement( app.getChildren() );
        
        Object obj = entity.config().get(ConfigKeys.newConfigKey(Object.class, "test.obj"));
        log.info("Object for "+JavaClassNames.callerNiceClassAndMethod(1)+" : "+obj);
        Asserts.assertInstanceOf(obj, ItemA.class);
        Assert.assertEquals(((ItemA)obj).name, "bob");
    }

    @Test
    public void testOldStyle() throws Exception {
        doTest(
            "$brooklyn:object:",
            "  type: "+ItemA.class.getName(),
            "  object.fields:",
            "    name: bob");
    }

    @Test
    public void testYomlSyntax() throws Exception {
        doTest(
            "$brooklyn:object-yoml:",
            "  type: "+ItemA.class.getName(),
            "  name: bob");
    }

}
