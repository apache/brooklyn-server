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

package org.apache.brooklyn.rest.entitlement;

import static org.testng.Assert.assertEquals;

import java.net.URI;
import java.util.Map;

import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

public class ScriptApiEntitlementsTest extends AbstractRestApiEntitlementsTest {

    @Test(groups = "Integration")
    public void testGroovy() throws Exception {
        String script = "1 + 1";
        String path = "/v1/script/groovy";
        HttpToolResponse rootRepsonse = httpPost("myRoot", path, script.getBytes());
        assertHealthyStatusCode(rootRepsonse);
        Map<?, ?> groovyOutput = new Gson().fromJson(rootRepsonse.getContentAsString(), Map.class);
        assertEquals(groovyOutput.get("result"), "2");
        assert401(path);
        assertForbiddenPost("myUser", path, script.getBytes());
        assertForbiddenPost("myReadonly", path, script.getBytes());
        assertForbiddenPost("myMinimal", path, script.getBytes());
        assertForbiddenPost("unrecognisedUser", path, script.getBytes());
    }

    @Override
    protected HttpToolResponse httpPost(String user, String path, byte[] body) throws Exception {
        final ImmutableMap<String, String> headers = ImmutableMap.of(
                "Content-Type", "application/text");
        final URI uri = URI.create(getBaseUriRest()).resolve(path);
        return HttpTool.httpPost(newClient(user), uri, headers, body);
    }
}
