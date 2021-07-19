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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.rest.api.LogbookApi;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.http.HttpStatus;
import org.testng.annotations.*;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;

/**
 * Tests the {@link LogbookApi} implementation.
 */
@Test(singleThreaded = true, suiteName = "LogbookResourceTest")
public class LogbookResourceTest extends BrooklynRestResourceTest {

    static class LogbookResourceTestHelper {
        private static File LOG_TEMP_FILE;

        public synchronized static File installSampleLog() {
            if (LOG_TEMP_FILE == null) {
                // copy from classpath so tests don't make assumptions about file system, e.g. if running from jar
                LOG_TEMP_FILE = Os.newTempFile(LogbookResourceTest.class, "log");
                try {
                    FileWriter fw = new FileWriter(LOG_TEMP_FILE);
                    fw.write(ResourceUtils.create(LogbookResourceTest.class).getResourceAsString("classpath:/logbook.log.sample"));
                    fw.close();
                } catch (IOException e) {
                    throw Exceptions.propagate(e);
                }
            }
            return LOG_TEMP_FILE;
        }

        public static void installSampleLog(BrooklynProperties brooklynProperties) {
            installSampleLog();
            brooklynProperties.put("brooklyn.logbook.fileLogStore.path", LOG_TEMP_FILE.getAbsolutePath());
        }
    }

    @Override
    protected BrooklynProperties getBrooklynProperties() {
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        LogbookResourceTestHelper.installSampleLog(brooklynProperties);
        return brooklynProperties;
    }

    @Test
    public void testQueryLogbookNoArgs(){

        // Post null query.
        Response response = client()
                .path("/logbook")
                .accept(MediaType.APPLICATION_JSON)
                .post(null);

        assertEquals(response.getStatus(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testQueryLogbookUnknownArgs() throws IOException {

        // Prepare query with unknown args.
        ImmutableMap<Object, Object> qb = ImmutableMap.builder()
                .put("unknownArg", false)
                .build();

        Response response = client()
                .path("/logbook")
                .accept(MediaType.APPLICATION_JSON)
                .post(toJsonEntity(qb));

        assertEquals(response.getStatus(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testQueryLogbookValidArgs() throws IOException {

        // Prepare a valid query.
        ImmutableMap<Object, Object> qb = ImmutableMap.builder()
                .put("numberOfItems", 3)
                .put("tail", false)
                .put("levels", ImmutableList.of("WARN", "DEBUG"))
                .build();

        Response response = client()
                .path("/logbook")
                .accept(MediaType.APPLICATION_JSON)
                .post(toJsonEntity(qb));

        assertEquals(response.getStatus(), HttpStatus.SC_OK);

        ArrayList<BrooklynLogEntry> brooklynLogEntries = response.readEntity(new GenericType<ArrayList<BrooklynLogEntry>>() {});
        assertEquals(brooklynLogEntries.size(), 3);
    }

    // ------------ THE TEST GROUP BELOW IS FOR UNAUTHORIZED ACCESS CASES -----------------------

    /**
     * This class is required to configure different startup Brooklyn properties for tests that depend on that.
     */
    private static class AbstractLogbookResourceWithEntitlementTest extends BrooklynRestResourceTest {

        /**
         * @return The 'brooklyn.entitlements.global' brooklyn property.
         */
        protected String getBrooklynEntitlementsGlobal() {
            return "root"; // root is default, however, lets make it explicit.
        }

        @Override
        protected BrooklynProperties getBrooklynProperties() {
            BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
            brooklynProperties.put("brooklyn.entitlements.global", this.getBrooklynEntitlementsGlobal());
            LogbookResourceTestHelper.installSampleLog(brooklynProperties);
            return brooklynProperties;
        }

        /**
         * Test to verify if logbook access is not authorized.
         *
         * @throws Exception in case of test infrastructure errors.
         */
        private void testQueryLogbookNotAuthorized() throws Exception {

            // Prepare a valid query.
            ImmutableMap<Object, Object> qb = ImmutableMap.builder()
                    .put("numberOfItems", 3)
                    .put("tail", false)
                    .put("levels", ImmutableList.of("WARN", "DEBUG"))
                    .build();

            // Access the logbook resource with un-entitled user.
            Response response = client()
                    .path("/logbook")
                    .accept(MediaType.APPLICATION_JSON)
                    .post(toJsonEntity(qb));

            assertEquals(response.getStatus(), HttpStatus.SC_UNAUTHORIZED);
        }
    }

    @Test(singleThreaded = true, suiteName = "LogbookResourceTest")
    public static class LogbookResourceGlobalEntitlementMinimalTest extends AbstractLogbookResourceWithEntitlementTest {

        @Override
        protected String getBrooklynEntitlementsGlobal() {
            return "minimal";
        }

        @Test
        public void testQueryLogbookNotAuthorized() throws Exception {
            super.testQueryLogbookNotAuthorized();
        }
    }

    @Test(singleThreaded = true, suiteName = "LogbookResourceTest")
    public static class LogbookResourceGlobalEntitlementUserTest extends AbstractLogbookResourceWithEntitlementTest {

        @Override
        protected String getBrooklynEntitlementsGlobal() {
            return "user";
        }

        @Test
        public void testQueryLogbookNotAuthorized() throws Exception {
            super.testQueryLogbookNotAuthorized();
        }
    }

    @Test(singleThreaded = true, suiteName = "LogbookResourceTest")
    public static class LogbookResourceGlobalEntitlementReadonlyTest extends AbstractLogbookResourceWithEntitlementTest {

        @Override
        protected String getBrooklynEntitlementsGlobal() {
            return "readonly";
        }

        @Test
        public void testQueryLogbookNotAuthorized() throws Exception {
            super.testQueryLogbookNotAuthorized();
        }
    }
}
