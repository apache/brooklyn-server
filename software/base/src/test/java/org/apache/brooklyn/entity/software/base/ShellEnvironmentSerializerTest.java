/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.entity.software.base;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Date;

import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ShellEnvironmentSerializerTest extends BrooklynAppUnitTestSupport {
    ShellEnvironmentSerializer ser;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        ser = new ShellEnvironmentSerializer(mgmt);
    }

    @Test
    public void testSerialize() {
        String str = "some string \" with ' special; characters {}[]";
        Date date = new Date();
        String appExpected = "{\"type\":\"org.apache.brooklyn.api.entity.Entity\",\"id\":\"" + app.getId() + "\"}";
        assertSerialize(str, str);
        assertSerialize(3.14, "3.14");
        assertSerialize(3.140, "3.14");
        assertSerialize(.140, "0.14");
        assertSerialize(0.140, "0.14");
        assertSerialize(Boolean.TRUE, "true");
        assertSerialize(Boolean.FALSE, "false");
        assertSerialize(date, Time.makeIso8601DateStringZ(date.toInstant()));
        assertSerialize(date.toInstant(), Time.makeIso8601DateStringZ(date.toInstant()));
        assertSerialize(Duration.FIVE_MINUTES, "5m");
        assertSerialize(null, null);
        assertSerialize(ImmutableList.of(str, 3.14, 0.14));
        assertSerialize(ImmutableMap.of("string", str, "num1", 3.14, "num2", 0.14));
        assertSerialize(ImmutableMap.of("list", ImmutableList.of(str, 3.14, 0.14), 
                "map", ImmutableMap.of("string", str, "num1", 3.14, "num2", 0.14)));
        assertSerialize(app, appExpected);
        assertSerialize(ImmutableList.of(app), "[" + appExpected + "]");
        assertSerialize(ImmutableMap.of("app", app), "{\"app\":" + appExpected + "}");
        assertSerialize(mgmt, "{\"type\":\"org.apache.brooklyn.api.mgmt.ManagementContext\"}");
        // https://issues.apache.org/jira/browse/BROOKLYN-304
        assertSerialize(getClass().getClassLoader(), "{\"type\":\""+getClass().getClassLoader().getClass().getCanonicalName()+"\"}");
        assertSerialize(getClass(), getClass().getName());
    }

    @Test
    public void testSerializeYamlStringNormalizedToJson() {
        // YAML list → JSON array
        assertEquals(ser.serialize("- item1\n- item2\n"), "[\"item1\",\"item2\"]");

        // YAML list of maps → JSON array of objects
        String yamlList = "- name: element1\n  password: s3cr3t\n";
        String jsonResult = ser.serialize(yamlList);
        assertTrue(jsonResult.startsWith("["), "Expected JSON array, got: " + jsonResult);
        assertFalse(jsonResult.contains("- name:"), "Should not contain YAML list syntax: " + jsonResult);

        // Plain string → unchanged
        assertEquals(ser.serialize("plain string value"), "plain string value");

        // JSON string → unchanged (already JSON, re-serialization produces same output)
        assertEquals(ser.serialize("[\"a\",\"b\"]"), "[\"a\",\"b\"]");
    }

    private void assertSerialize(Object actual, String expected) {
        assertEquals(ser.serialize(actual), expected);
    }
    private void assertSerialize(Object obj) {
        String serialized = ser.serialize(obj);
        Gson gson = new GsonBuilder().create();
        assertEquals(obj, gson.fromJson(serialized, Object.class));
    }
}
