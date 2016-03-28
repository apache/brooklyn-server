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
package org.apache.brooklyn.rest;

import static org.apache.brooklyn.KarafTestUtils.defaultOptionsWith;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;

import java.util.concurrent.Callable;

import org.apache.brooklyn.KarafTestUtils;
import org.apache.brooklyn.entity.brooklynnode.BrooklynNode;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.http.HttpStatus;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@Ignore // TODO: re-enable after brooklyn is properly initialized within the OSGI environment
public class BrooklynRestApiLauncherTest {

    private static final String HTTP_PORT = "9998";
    private static final String ROOT_URL = "http://localhost:" + HTTP_PORT;

    @Configuration
    public static Option[] configuration() throws Exception {
        return defaultOptionsWith(
            editConfigurationFilePut("etc/org.ops4j.pax.web.cfg", "org.osgi.service.http.port", HTTP_PORT),
            features(KarafTestUtils.brooklynFeaturesRepository(), "brooklyn-software-base")
            // Uncomment this for remote debugging the tests on port 5005
            // ,KarafDistributionOption.debugConfiguration()
        );
    }

    @Test
    public void testStart() throws Exception {
        ensureBrooklynStarted();

        final String testUrl = ROOT_URL + "/v1/catalog/entities";
        int code = Asserts.succeedsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                int code = HttpTool.getHttpStatusCode(testUrl);
                if (code == HttpStatus.SC_FORBIDDEN) {
                    throw new RuntimeException("Retry request");
                } else {
                    return code;
                }
            }
        });
        HttpAsserts.assertHealthyStatusCode(code);
        HttpAsserts.assertContentContainsText(testUrl, BrooklynNode.class.getSimpleName());
    }

    private void ensureBrooklynStarted() {
        final String upUrl = ROOT_URL + "/v1/server/up";
        HttpAsserts.assertContentEventuallyMatches(upUrl, "true");
    }
}
