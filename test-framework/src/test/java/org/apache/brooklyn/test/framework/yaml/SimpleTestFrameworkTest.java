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

import org.apache.brooklyn.test.framework.live.TestFrameworkLiveTest;
import org.apache.brooklyn.test.framework.live.TestFrameworkRun;
import org.apache.brooklyn.test.framework.live.TestFrameworkSuiteBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SimpleTestFrameworkTest extends TestFrameworkLiveTest {

    @DataProvider
    @Override
    public Object[][] tests() {
        return TestFrameworkSuiteBuilder.create()
            // Can be set on the command line with -Dtest.framework.locations=... to load locations used in the tests
            // .locationsFolder("<path to>/locations")
            .test()
                .prerequisiteResource("test-framework-examples/simple-catalog.bom")
                .prerequisiteResource("test-framework-examples/simple-catalog.locations.bom")
                .testResource("test-framework-examples/simple-catalog.tests.bom")
                .location("simple-entity-dummy")
                .add()
            .buildProvider();
    }

    // Set the system property "test.framework.interactive" to true to pause
    // execution (don't tear down) on failure.
    @Test(dataProvider="tests", groups="Integration")
    public void runTestFrameworkTest(TestFrameworkRun testRun) {
        super.runTestFrameworkTest(testRun);
    }

}
