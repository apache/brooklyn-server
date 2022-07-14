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
package org.apache.brooklyn.camp.brooklyn.catalog;

import static org.testng.Assert.assertEquals;

import java.util.List;

import com.google.common.base.Predicate;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.objs.ConstraintSerialization;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.Test;

import com.google.common.reflect.TypeToken;

public class SpecParameterParsingTest  extends AbstractYamlTest {

    private static final int NUM_APP_DEFAULT_CONFIG_KEYS = SpecParameterUnwrappingTest.NUM_APP_DEFAULT_CONFIG_KEYS;
    
    @Test
    public void testYamlInputsParsed() {
        String itemId = ver("test.inputs", "0.0.1");
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: test.inputs",
                "  version: 0.0.1",
                "  itemType: entity",
                "  item: ",
                "    type: "+ BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - simple",
                "    - name: explicit_name",
                "    - name: third_input",
                "      type: integer",
                "      pinned: false");
        EntitySpec<?> item = mgmt().getTypeRegistry().createSpec(mgmt().getTypeRegistry().get(itemId), null, EntitySpec.class);
        List<SpecParameter<?>> inputs = item.getParameters();
        assertEquals(inputs.size(), NUM_APP_DEFAULT_CONFIG_KEYS + 3, "inputs="+inputs);
        SpecParameter<?> firstInput = inputs.get(0);
        assertEquals(firstInput.getLabel(), "simple");
        assertEquals(firstInput.isPinned(), true);
        assertEquals(firstInput.getConfigKey().getName(), "simple");
        assertEquals(firstInput.getConfigKey().getTypeToken(), TypeToken.of(String.class));
        
        SpecParameter<?> secondInput = inputs.get(1);
        assertEquals(secondInput.getLabel(), "explicit_name");
        assertEquals(secondInput.isPinned(), true);
        assertEquals(secondInput.getConfigKey().getName(), "explicit_name");
        assertEquals(secondInput.getConfigKey().getTypeToken(), TypeToken.of(String.class));
        
        SpecParameter<?> thirdInput = inputs.get(2);
        assertEquals(thirdInput.getLabel(), "third_input");
        assertEquals(thirdInput.isPinned(), false);
        assertEquals(thirdInput.getConfigKey().getName(), "third_input");
        assertEquals(thirdInput.getConfigKey().getTypeToken(), TypeToken.of(Integer.class));
    }

    @Test
    public void testYamlInputConstraintRegexParse() {
        String itemId = ver("test.inputs", "0.0.1");
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: test.inputs",
                "  version: 0.0.1",
                "  itemType: entity",
                "  item: ",
                "    type: "+ BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - name: p",
                "      constraints:",
                "      - required",
                "      - regex: \\d\\d?-\\d\\d(\\d\\d)?");
        EntitySpec<?> item = mgmt().getTypeRegistry().createSpec(mgmt().getTypeRegistry().get(itemId), null, EntitySpec.class);
        List<SpecParameter<?>> inputs = item.getParameters();
        assertEquals(inputs.size(), NUM_APP_DEFAULT_CONFIG_KEYS + 1, "inputs="+inputs);
        SpecParameter<?> firstInput = inputs.get(0);
        assertEquals(firstInput.getLabel(), "p");
        Predicate c = firstInput.getConfigKey().getConstraint();
        Asserts.assertTrue(c.apply("12-2000"));
        Asserts.assertFalse(c.apply("DEC-2000"));

        Object serialized = ConstraintSerialization.INSTANCE.toJsonList(c);
        Asserts.assertInstanceOf(serialized, List.class);
        Asserts.assertSize( (List)serialized, 2);
        Asserts.assertEquals( ((List)serialized).get(0), "required");
        Asserts.assertEquals( ((List)serialized).get(1), MutableMap.of("regex", "\\d\\d?-\\d\\d(\\d\\d)?"));
    }

    @Test
    public void testYamlInputConstraintRequiredUnlessParse() {
        String itemId = ver("test.inputs", "0.0.1");
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: test.inputs",
                "  version: 0.0.1",
                "  itemType: entity",
                "  item: ",
                "    type: "+ BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - name: p",
                "      constraints:",
                "      - requiredUnlessAnyOf: [ q, r ]",
                "    - q");
        EntitySpec<?> item = mgmt().getTypeRegistry().createSpec(mgmt().getTypeRegistry().get(itemId), null, EntitySpec.class);
        List<SpecParameter<?>> inputs = item.getParameters();
        assertEquals(inputs.size(), NUM_APP_DEFAULT_CONFIG_KEYS + 2, "inputs="+inputs);
        SpecParameter<?> firstInput = inputs.get(0);
        assertEquals(firstInput.getLabel(), "p");
        Predicate c = firstInput.getConfigKey().getConstraint();

        Object serialized = ConstraintSerialization.INSTANCE.toJsonList(c);
        Asserts.assertInstanceOf(serialized, List.class);
        Asserts.assertSize( (List)serialized, 1);
        Asserts.assertEquals( ((List)serialized).get(0), MutableMap.of("requiredUnlessAnyOf", MutableList.of("q", "r")));
    }

}
