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
package org.apache.brooklyn.core.test.qa.performance;

import org.apache.brooklyn.test.performance.PerformanceTestDescriptor;
import org.apache.brooklyn.util.core.xstream.XmlUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class XmlPerformanceTest extends AbstractPerformanceTest {

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    protected int numIterations() {
        return 100;
    }
    
    @Test(groups = { "Integration", "Acceptance" })
    public void testXpath() throws Exception {
        int numIterations = numIterations();
        double minRatePerSec = 100 * PERFORMANCE_EXPECTATION;

        measure(PerformanceTestDescriptor.create()
                .summary("XmlPerformanceTest.testXpath")
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .job(new Runnable() {
                    @Override
                    public void run() {
                        String xml = "<a><b>myb</b></a>";
                        XmlUtil.xpath(xml, "/a/b[text()]");
                    }}));
    }
}
