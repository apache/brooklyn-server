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
        String body = "------WebKitFormBoundaryaQhM7RFMi4ZiXOj2\n\rContent-Disposition: form-data; services:\n\r- type: org.apache.brooklyn.entity.stock.BasicEntity\n\rbrooklyn.config:\n\rexample: $brooklyn:formatString(\"%s\", \"vault\")\n\r\n\r\n\r------WebKitFormBoundaryaQhM7RFMi4ZiXOj2\n\rContent-Disposition: form-data; name=\"format\"\n\r\n\rcamp\n\r------WebKitFormBoundaryaQhM7RFMi4ZiXOj2--\n\r";
        ImmutableMap<String, String> headers = ImmutableMap.of("Content-Type", "multipart/form-data; boundary=----WebKitFormBoundaryaQhM7RFMi4ZiXOj2");
        assertPostMultiPart("admin", "/v1/applications", body.getBytes(), headers);
    }

    public void assertPostMultiPart(String user, String path, byte[] body, Map<String, String> headers) throws Exception {
        HttpToolResponse response = httpPost(user, path, body, headers);
        assertHealthyStatusCode(response);
    }


}