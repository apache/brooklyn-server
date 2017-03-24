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
package org.apache.brooklyn.util.core.http;

import static org.testng.Assert.assertEquals;

import java.net.URI;

import org.apache.brooklyn.util.http.HttpTool;
import org.apache.http.client.HttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;


/*
  A test illustrating failure to connect to a site (https://httpbin.org) that requires SSL SNI (Server Naming
  Indication), see https://issues.apache.org/jira/browse/BROOKLYN-456.

  setup() adds debug output, look for the following lines in the output:

      *** ClientHello, TLSv1.2
    RandomCookie:  GMT: 1473261907 bytes = { 196, 87, 35, 69, 71, 71, 215, 118, 246, 113, 0, 129, 18, 108, 212, 242, 67, 91, 158, 8, 78, 208, 233, 111, 82, 140, 106, 17 }
    Session ID:  {}
    Cipher Suites: [... blabla lots of suites... ]
    Compression Methods:  { 0 }
    Extension elliptic_curves, curve names: {secp256r1, secp384r1, secp521r1, sect283k1, sect283r1, sect409k1, sect409r1, sect571k1, sect571r1, secp256k1}
    Extension ec_point_formats, formats: [uncompressed]
    Extension signature_algorithms, signature_algorithms: SHA512withECDSA, SHA512withRSA, SHA384withECDSA, SHA384withRSA, SHA256withECDSA, SHA256withRSA, SHA256withDSA, SHA224withECDSA, SHA224withRSA, SHA224withDSA, SHA1withECDSA, SHA1withRSA, SHA1withDSA
    ***

  Note above that there are three "extension" items, ("elliptic_curves", "ec_point_formats", "signature_algorithms"),
  but among them there is no "server_name" extension.

  The result is that the initial SSL handshake fails with "SSLException: Received fatal alert: internal_error".

 */
public class HttpToolSNITest {
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        System.setProperty("javax.net.debug", "ssl:handshake");
    }


    @Test(groups = {"Integration", "Broken"})
    public void testHttpGetRequiringSNI() throws Exception {
        URI baseUri = new URI("https://httpbin.org/get?id=myId");

        HttpClient client = org.apache.brooklyn.util.http.HttpTool.httpClientBuilder().build();
        org.apache.brooklyn.util.http.HttpToolResponse result = HttpTool.httpGet(client, baseUri, ImmutableMap.<String,String>of());
        assertEquals(result.getResponseCode(), 200);
    }
}
