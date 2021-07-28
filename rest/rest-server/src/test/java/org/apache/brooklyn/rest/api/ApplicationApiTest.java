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
package org.apache.brooklyn.rest.api;

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.rest.BrooklynRestApiLauncherTestFixture;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.testng.annotations.Test;

import java.util.Map;

public class ApplicationApiTest extends BrooklynRestApiLauncherTestFixture {


    @Test(groups = "Integration")
    public void testMultipartFormWithInvalidChar() throws Exception {
        useServerForTest(newServer());
        String body = "------WebKitFormBoundaryaQhM7RFMi4ZiXOj2\n\r" +
                "Content-Disposition: form-data; name=\"plan\"\n\r\n\r" +
                "services:\n\r" +
                "- type: org.apache.brooklyn.entity.stock.BasicEntity\n\r" +
                "  brooklyn.config:\n\r" +
                "    example: $brooklyn:formatString(\"%s\", \"vault\")\n\r\n\r\n\r" +
                "------WebKitFormBoundaryaQhM7RFMi4ZiXOj2\n\r" +
                "Content-Disposition: form-data; name=\"format\"\n\r\n\r" +
                "brooklyn-camp\n\r" +
                "------WebKitFormBoundaryaQhM7RFMi4ZiXOj2--\n\r";
        ImmutableMap<String, String> headers = ImmutableMap.of("Content-Type", "multipart/form-data; boundary=----WebKitFormBoundaryaQhM7RFMi4ZiXOj2");
        assertPostMultiPart("admin", "/v1/applications", body.getBytes(), headers);
    }

    public void assertPostMultiPart(String user, String path, byte[] body, Map<String, String> headers) throws Exception {
        HttpToolResponse response = httpPost(user, path, body, headers);
        assertHealthyStatusCode(response);
    }


}