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

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class SoftwareProcessShellEnvironmentTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testEnvironment() {
        final EnvRecordingLocation recordingMachine = mgmt.getLocationManager().createLocation(LocationSpec.create(EnvRecordingLocation.class)
                .configure("address", "127.0.0.1"));
        VanillaSoftwareProcess entity = app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class));
        Date dt = new Date();
        Integer someInt = 12;
        Duration someDuration = Duration.FIVE_MINUTES;
        String someString = "simple string";
        String specialString = "some \"\\\" escaped string ' ; to\" [] {}";
        entity.config().set(VanillaSoftwareProcess.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>builder()
                .put("some_int", someInt)
                .put("some_duration", someDuration.toString())
                .put("some_string", someString)
                .put("some_boolean", Boolean.TRUE)
                .put("some_map", ImmutableMap.of(
                        "more_keys", "more_values",
                        "some_more_keys", "some_more_values",
                        "some_duration", someDuration,
                        "special_value", specialString))
                .put("some_list", ImmutableList.of(
                        "idx1",
                        "idx2",
                        specialString))
                .put("some_time", dt)
                .put("some_bean", new SimpleBean("bean-string", -1))
                .put("self", entity)
                .build());

        app.start(ImmutableList.of(recordingMachine));
        Map<String, ?> env = recordingMachine.getRecordedEnv().iterator().next();

        String escapedString = "\"" + specialString.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        assertEquals(env.get("some_int"), someInt.toString());
        assertEquals(env.get("some_string"), someString);
        assertEquals(env.get("some_boolean"), Boolean.TRUE.toString());
        assertEquals(env.get("some_map"), "{"
                + "\"more_keys\":\"more_values\","
                + "\"some_more_keys\":\"some_more_values\","
                + "\"some_duration\":\""+someDuration.toString()+"\","
                + "\"special_value\":" + escapedString
                + "}");
        assertEquals(env.get("some_list"), "[\"idx1\",\"idx2\"," + escapedString + "]");
        assertEquals(env.get("some_time"), Long.toString(dt.getTime()));
        assertEquals(env.get("some_bean"), "{\"propString\":\"bean-string\",\"propInt\":-1}");
        assertEquals(env.get("self"), "{\"type\":\"org.apache.brooklyn.api.entity.Entity\",\"id\":\"" + entity.getId() + "\"}");
    }

    public static class SimpleBean {
        private String propString;
        private int propInt;
        public SimpleBean() {}
        public SimpleBean(String propString, int propInt) {
            super();
            this.propString = propString;
            this.propInt = propInt;
        }
        public String getPropString() {
            return propString;
        }
        public void setPropString(String propString) {
            this.propString = propString;
        }
        public int getPropInt() {
            return propInt;
        }
        public void setPropInt(int propInt) {
            this.propInt = propInt;
        }
    }

    public static class EnvRecordingLocation extends SshMachineLocation {
        private List<Map<String, ?>> env = Lists.newCopyOnWriteArrayList();

        @Override
        public int execScript(Map<String, ?> props, String summaryForLogging, List<String> commands, Map<String, ?> env) {
            recordEnv(env);
            return 0;
        }

        private void recordEnv(Map<String, ?> env) {
            this.env.add(env);
        }
        
        public List<Map<String, ?>> getRecordedEnv() {
            return env;
        }
    }
}
