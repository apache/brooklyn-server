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
package org.apache.brooklyn.util.core.javalang;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.executor.HttpConfig;
import org.apache.brooklyn.util.http.executor.HttpExecutor;
import org.apache.brooklyn.util.http.executor.apacheclient.HttpExecutorImpl;

import java.util.function.Consumer;
import java.util.function.Function;

public class BrooklynHttpConfig {

    public static final String HTTPS_CONFIG = "brooklyn.https.config.";
    public static final ConfigKey<Boolean> TRUST_ALL = ConfigKeys.newBooleanConfigKey(HTTPS_CONFIG + "trustAll",
        "Whether HTTPS and TLS connections should trust all certificates");
    public static final ConfigKey<Boolean> TRUST_SELF_SIGNED = ConfigKeys.newBooleanConfigKey(HTTPS_CONFIG + "trustSelfSigned",
            "Whether HTTPS and TLS connections should trust self-signed certificates");
    public static final ConfigKey<Boolean> LAX_REDIRECT = ConfigKeys.newBooleanConfigKey(HTTPS_CONFIG + "laxRedirect",
            "Whether HTTPS and TLS connections should be lax about redirecting");

    private static final boolean DEFAULT_FOR_MGMT_LAX_AND_TRUSTING = true;

    public static HttpConfig.Builder httpConfigBuilder(ManagementContext mgmt, boolean lookForContextEntity) {
        return httpConfigBuilder(mgmt, DEFAULT_FOR_MGMT_LAX_AND_TRUSTING, lookForContextEntity);
    }
    public static HttpConfig.Builder httpConfigBuilder(ManagementContext mgmt, boolean defaultLaxAndTrusting, boolean lookForContextEntity) {
        HttpConfig.Builder hcb = httpConfigBuilderDefault(defaultLaxAndTrusting, false);
        apply(hcb, mgmt.getConfig()::getConfig);
        if (lookForContextEntity) applyContextEntity(hcb);
        return hcb;
    }

    private static void apply(HttpConfig.Builder hcb, Function<ConfigKey<Boolean>,Boolean> getter) {
        applyIfNonNull(getter.apply(TRUST_ALL), hcb::trustAll);
        applyIfNonNull(getter.apply(TRUST_SELF_SIGNED), hcb::trustSelfSigned);
        applyIfNonNull(getter.apply(LAX_REDIRECT), hcb::laxRedirect);
    }

    private static void applyContextEntity(HttpConfig.Builder hcb) {
        BrooklynObject entity = BrooklynTaskTags.getContextEntity(Tasks.current());
        if (entity!=null) apply(hcb, entity.config()::get);
    }

    private static void applyIfNonNull(Boolean v, Consumer<Boolean> target) {
        if (v!=null) target.accept(v);
    }

    public static HttpConfig.Builder httpConfigBuilder(BrooklynObject entity) {
        return httpConfigBuilder(entity, DEFAULT_FOR_MGMT_LAX_AND_TRUSTING);
    }
    public static HttpConfig.Builder httpConfigBuilder(BrooklynObject entity, boolean defaultLaxAndTrusting) {
        HttpConfig.Builder hcb = httpConfigBuilder(((BrooklynObjectInternal)entity).getManagementContext(), defaultLaxAndTrusting, false);
        apply(hcb, entity.config()::get);
        return hcb;
    }

    private static HttpConfig.Builder httpConfigBuilderDefault(boolean laxAndTrusting, boolean lookForContextEntities) {
        HttpConfig.Builder hcb = HttpConfig.builder().laxRedirect(laxAndTrusting).trustAll(laxAndTrusting).trustSelfSigned(laxAndTrusting);
        if (lookForContextEntities) applyContextEntity(hcb);
        return hcb;
    }



    public static HttpTool.HttpClientBuilder httpClientBuilder(ManagementContext mgmt, boolean lookForContextEntity) {
        return httpClientBuilder(mgmt, DEFAULT_FOR_MGMT_LAX_AND_TRUSTING, lookForContextEntity);
    }
    public static HttpTool.HttpClientBuilder httpClientBuilder(ManagementContext mgmt, boolean defaultLaxAndTrusting, boolean lookForContextEntity) {
        HttpTool.HttpClientBuilder hcb = httpClientBuilderDefault(defaultLaxAndTrusting, false);
        apply(hcb, mgmt.getConfig()::getConfig);
        if (lookForContextEntity) {
            applyContextEntity(hcb);
        }
        return hcb;
    }

    private static void apply(HttpTool.HttpClientBuilder hcb, Function<ConfigKey<Boolean>,Boolean> getter) {
        applyIfNonNull(getter.apply(TRUST_ALL), hcb::trustAll);
        applyIfNonNull(getter.apply(TRUST_SELF_SIGNED), hcb::trustSelfSigned);
        applyIfNonNull(getter.apply(LAX_REDIRECT), hcb::laxRedirect);
    }

    private static void applyContextEntity(HttpTool.HttpClientBuilder hcb) {
        BrooklynObject entity = BrooklynTaskTags.getContextEntity(Tasks.current());
        if (entity!=null) apply(hcb, entity.config()::get);
    }

    public static HttpTool.HttpClientBuilder httpClientBuilderDefaultStrict() {
        return httpClientBuilderDefault(false, false);
    }

    private static HttpTool.HttpClientBuilder httpClientBuilderDefault(boolean defaultLaxAndTrusting, boolean lookForContextEntity) {
        HttpTool.HttpClientBuilder hcb = HttpTool.httpClientBuilder();
        if (defaultLaxAndTrusting) {
            hcb.trustAll(true);
            hcb.trustSelfSigned(true);
            hcb.laxRedirect(true);
        }
        if (lookForContextEntity) {
            applyContextEntity(hcb);
        }
        return hcb;
    }

    public static HttpTool.HttpClientBuilder httpClientBuilder(BrooklynObject entity) {
        return httpClientBuilder(entity, DEFAULT_FOR_MGMT_LAX_AND_TRUSTING);
    }

    public static HttpTool.HttpClientBuilder httpClientBuilder(BrooklynObject entity, boolean defaultLaxAndTrusting) {
        HttpTool.HttpClientBuilder hcb = httpClientBuilder(((BrooklynObjectInternal)entity).getManagementContext(), defaultLaxAndTrusting, false);
        apply(hcb, entity.config()::get);
        return hcb;
    }

    public static HttpExecutor newHttpExecutor(BrooklynObject entity) {
        return HttpExecutorImpl.newInstance().withConfig(httpConfigBuilder(entity, false).build());
    }

    public static HttpExecutor newHttpExecutorDefault() {
        return HttpExecutorImpl.newInstance().withConfig(httpConfigBuilderDefault(false, true).build());
    }

    // SslTrustUtils.trustAll and TRUST_ALL -- only used in unsafe methods
    // TrustingSslSocketFactory - only used in unsafe methods
    // HttpTool and HttpTestUtils methods -- only used in tests and check code, methods marked unsafe, not for content
    // HttpTool.TrustAllStrategy -- only used in unsafe methods above and by HttpClientBuilder which is routed above in production code
    // HttpExecutorImpl -- only used with config supplied by above
    // HttpExecutorFactory not set, except in tests; our HttpExecutorFactoryImpl only used in tests
    // HttpConfig.Builder -- all uses routed through here
}
