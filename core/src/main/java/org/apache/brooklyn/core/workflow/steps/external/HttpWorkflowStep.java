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
package org.apache.brooklyn.core.workflow.steps.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.javalang.BrooklynHttpConfig;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.auth.UsernamePassword;
import org.apache.brooklyn.util.http.executor.HttpConfig;
import org.apache.brooklyn.util.http.executor.HttpExecutor;
import org.apache.brooklyn.util.http.executor.HttpRequest;
import org.apache.brooklyn.util.http.executor.HttpResponse;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.Predicate;

public class HttpWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(HttpWorkflowStep.class);

    public static final String SHORTHAND = "${endpoint}";

    public static final ConfigKey<String> ENDPOINT = ConfigKeys.newStringConfigKey("endpoint");
    public static final ConfigKey<Map<String,Object>> QUERY = new MapConfigKey.Builder(Object.class, "query").build();
    public static final ConfigKey<Object> BODY = ConfigKeys.newConfigKey(Object.class, "body");
    public static final ConfigKey<String> CHARSET = ConfigKeys.newStringConfigKey("charset", "Character set to interpret content as when converting to string, and for converting body to bytes to upload if body is set");
    public static final ConfigKey<DslPredicates.DslPredicate<Integer>> STATUS_CODE = ConfigKeys.newConfigKey(new TypeToken<DslPredicates.DslPredicate<Integer>>() {}, "status_code");
    public static final ConfigKey<Map<String, String>> HEADERS = new MapConfigKey<>(String.class, "headers");
    public static final ConfigKey<String> METHOD = ConfigKeys.newStringConfigKey("method");

    /** directly customizable here, otherwise based on entity and brooklyn.properties per {@link BrooklynHttpConfig} */
    public static final ConfigKey<HttpConfig> HTTPS_CONFIG = ConfigKeys.newConfigKey(HttpConfig.class, "config");

    public static final ConfigKey<String> USERNAME = ConfigKeys.newStringConfigKey("username", "Username for HTTP request, if required");
    public static final ConfigKey<String> PASSWORD = ConfigKeys.newStringConfigKey("password", "Password for HTTP request, if required");


    // used for http feed, but not sure needed
//    public static final ConfigKey<Boolean> PREEMPTIVE_BASIC_AUTH = ConfigKeys.newBooleanConfigKey(
//            "preemptiveBasicAuth",
//            "Whether to pre-emptively including a basic-auth header of the username:password (rather than waiting for a challenge)",
//            Boolean.FALSE);

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        String endpoint = context.getInput(ENDPOINT);
        if (Strings.isBlank(endpoint)) {
            throw new IllegalStateException("Endpoint required for http step");
        }
        String proto = Urls.getProtocol(endpoint);
        if (proto==null) {
            endpoint = "https://"+endpoint;
        }

        HttpRequest.Builder httpb = new HttpRequest.Builder();
        URIBuilder urib;
        try {
            urib = new URIBuilder(endpoint);
            Map<String, Object> params = context.getInput(QUERY);
            if (params!=null) {
                new ShellEnvironmentSerializer(context.getManagementContext()).serialize(params)
                    .forEach((k, v) -> urib.addParameter(k, v));
            }
            httpb.uri(urib.build());
        } catch (URISyntaxException e) {
            throw Exceptions.propagateAnnotated("Invalid URI: "+endpoint, e);
        }

        String username = context.getInput(USERNAME);
        String password = context.getInput(PASSWORD);
        if (Strings.isNonBlank(username) || Strings.isNonBlank(password)) {
            if (Strings.isNonBlank(username) && Strings.isNonBlank(password)) {
                httpb.credentials(new UsernamePassword(username, password));
            } else {
                throw new IllegalStateException("Must supply either both 'username' and 'password' or neither");
            }
        }

        Map<String, String> headers = context.getInput(HEADERS);
        if (headers!=null) httpb.headers(headers);

        httpb.config(context.getInput(HTTPS_CONFIG));

        String method = context.getInput(METHOD);
        if (Strings.isBlank(method)) method = "get";
        httpb.method(method);

        String charsetS = context.getInput(CHARSET);
        Charset charset = Strings.isBlank(charsetS) ? Charset.defaultCharset() : Charset.forName(charsetS);

        Object body = context.getInput(BODY);
        if (body!=null) {
            if (body instanceof byte[]) {
                httpb.body((byte[]) body);
            } else {
                try {
                    String bodyS = BeanWithTypeUtils.newMapper(context.getManagementContext(), false, null, false).writeValueAsString(body);
                    httpb.body(bodyS.getBytes(charset));
                } catch (JsonProcessingException e) {
                    throw Exceptions.propagate(e);
                }
            }
        }

        final long startTime = System.currentTimeMillis();
        HttpExecutor httpExecutor = BrooklynHttpConfig.newHttpExecutor(context.getEntity());
        HttpResponse response;
        try {
            response = httpExecutor.execute(httpb.build());
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }

        byte[] contentBytes = Streams.readFully(response.getContent());
        final long endTime = System.currentTimeMillis();

        String contentString = new String(contentBytes, charset);

        Predicate<Integer> exitcode = context.getInput(STATUS_CODE);
        if (exitcode==null) exitcode = code -> HttpTool.isStatusCodeHealthy(code);

        Object content_json = null;
        try {
            content_json = BeanWithTypeUtils.newSimpleMapper().readValue(contentString, Object.class);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.debug("Content from web request is not json; not setting content_json: "+e);
        }
        context.setOutput(MutableMap.of("status_code", response.code(), "headers", response.headers(), "content", contentString, "content_bytes", contentBytes, "content_json", content_json, "duration", Duration.millis(endTime - startTime)));
        // make sure the output is set even if there is an error
        checkExitCode(response.code(), exitcode);
        return context.getOutput();
    }

    protected void checkExitCode(Integer code, Predicate<Integer> exitcode) {
        if (exitcode==null) return;
        if (exitcode instanceof DslPredicates.DslPredicateBase) {
            Object implicit = ((DslPredicates.DslPredicateBase) exitcode).implicitEqualsUnwrapped();
            if (implicit!=null) {
                if ("any".equalsIgnoreCase(""+implicit)) {
                    // if any is supplied as the implicit value, we accept; e.g. user says "exit_code: any"
                    return;
                }
            }
            // no other implicit values need be treated specially; 0 or 1 or 255 will work.
            // ranges still require `exit_code: { range: [0, 4] }`, same with `exit_code: { less-than: 5 }`.
        }
        if (!exitcode.test(code)) {
            throw new IllegalStateException("Invalid response status code '"+code+"'");
        }
    }

    @Override protected Boolean isDefaultIdempotent() { return false; }
}
