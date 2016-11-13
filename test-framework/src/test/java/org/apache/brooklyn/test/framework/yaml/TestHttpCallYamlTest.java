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
package org.apache.brooklyn.test.framework.yaml;

import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.framework.TestHttpCall;
import org.apache.brooklyn.test.http.TestHttpRequestHandler;
import org.apache.brooklyn.test.http.TestHttpServer;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

//Checks that the test cases work in YAML
@Test
public class TestHttpCallYamlTest extends AbstractYamlRebindTest {

    // TODO See comments in TestCaseYamlTest

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TestHttpCallYamlTest.class);

    private TestHttpServer server;
    private String testId;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testId = Identifiers.makeRandomId(8);
        server = new TestHttpServer()
                .handler("/201", new TestHttpRequestHandler()
                        .response("Created - " + testId)
                        .code(201))
                .handler("/204", new TestHttpRequestHandler().code(204))
                .handler("/index.html", new TestHttpRequestHandler()
                        .response("<html><body><h1>Im a H1 tag!</h1></body></html>")
                        .code(200))
                .handler("/body.json", new TestHttpRequestHandler()
                        .response("{\"a\":\"b\",\"c\":\"d\",\"e\":123,\"g\":false}")
                        .code(200 + Identifiers.randomInt(99)))
                .start();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (server != null) server.stop();
        }
    }
    
    @Test
    public void testSimpleGet() throws Exception {
        origApp = (BasicApplication) createStartWaitAndLogApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  id: target-app",
                "- type: " + TestHttpCall.class.getName(),
                "  brooklyn.config:",
                "    targetId: target-app",
                "    timeout: " + Asserts.DEFAULT_LONG_TIMEOUT,
                "    url: " + Urls.mergePaths(server.getUrl(), "index.html"),
                "    applyAssertionTo: status",
                "    assert:",
                "      equals: 200"
                );
    }
    
    @Test
    public void testUrlConstructedFromTargetEntity() throws Exception {
        origApp = (BasicApplication) createStartWaitAndLogApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  id: target-app",
                "  brooklyn.config:",
                "    main.uri: " + server.getUrl(),
                "- type: " + TestHttpCall.class.getName(),
                "  brooklyn.config:",
                "    targetId: target-app",
                "    url:",
                "      $brooklyn:formatString:",
                "      - \"%s/index.html\"",
                "      - $brooklyn:entity(config(\"targetId\")).config(\"main.uri\")",
                "    applyAssertionTo: status",
                "    assert:",
                "      equals: 200"
                );
    }
}
