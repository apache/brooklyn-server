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
package org.apache.brooklyn.rest.resources;

import javax.annotation.Nullable;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ContextResolver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.config.render.RendererHints;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.transform.TaskTransformer;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.rest.util.DefaultExceptionMapper;
import org.apache.brooklyn.rest.util.ManagementContextProvider;
import org.apache.brooklyn.rest.util.json.BrooklynJacksonJsonProvider;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public abstract class AbstractBrooklynRestResource {

    protected @Context UriInfo ui;

    @Context
    private ContextResolver<ManagementContext> mgmt;

    private BrooklynRestResourceUtils brooklynRestResourceUtils;
    private ObjectMapper mapper;

    public ManagementContext mgmt() {
        return Preconditions.checkNotNull(mgmt.getContext(ManagementContext.class), "mgmt");
    }

    public ManagementContextInternal mgmtInternal() {
        return (ManagementContextInternal) mgmt();
    }

    protected synchronized Maybe<ManagementContext> mgmtMaybe() {
        return Maybe.of(mgmt());
    }

    @VisibleForTesting
    public void setManagementContext(ManagementContext managementContext) {
        this.mgmt = new ManagementContextProvider(managementContext);
    }

    public synchronized BrooklynRestResourceUtils brooklyn() {
        if (brooklynRestResourceUtils!=null) return brooklynRestResourceUtils;
        brooklynRestResourceUtils = new BrooklynRestResourceUtils(mgmt());
        return brooklynRestResourceUtils;
    }
    
    /** returns a bad request Response wrapping the given exception */
    protected Response badRequest(Exception e) {
        DefaultExceptionMapper.logExceptionDetailsForDebugging(e);
        return ApiError.of(e).asBadRequestResponseJson();
    }
    
    protected ObjectMapper mapper() {
        return mapper(mgmt());
    }

    protected ObjectMapper mapper(ManagementContext mgmt) {
        if (mapper==null)
            mapper = BrooklynJacksonJsonProvider.findAnyObjectMapper(mgmt);
        return mapper;
    }

    protected RestValueResolver resolving(Object v) {
        return resolving(v, mgmt());
    }

    protected RestValueResolver resolving(Object v, ManagementContext mgmt) {
        return RestValueResolver.resolving(mgmt, v).mapper(mapper(mgmt));
    }

    public static class RestValueResolver {
        final private Object valueToResolve;
        ManagementContext mgmt;
        private @Nullable ObjectMapper mapperN;
        private boolean preferJson;
        private boolean isJerseyReturnValue;

        private @Nullable Entity entity;
        private @Nullable Duration timeout;
        private @Nullable Object rendererHintSource;
        private @Nullable Boolean immediately;

        private @Nullable Boolean raw;
        private @Nullable Boolean useDisplayHints;
        private @Nullable Boolean skipResolution;
        private @Nullable Boolean suppressBecauseSecret;
        private @Nullable Boolean suppressSecrets;

        public static RestValueResolver resolving(ManagementContext mgmt, Object v) { return new RestValueResolver(mgmt, v); }
        
        private RestValueResolver(ManagementContext mgmt, Object v) { this.mgmt = mgmt; valueToResolve = v; }
        
        public RestValueResolver mapper(ObjectMapper mapper) { this.mapperN = mapper; return this; }

        public RestValueResolver newInstanceResolving(Object v) {
            RestValueResolver result = resolving(mgmt, v);
            result.mapperN = mapperN;
            result.preferJson = preferJson;
            result.isJerseyReturnValue = isJerseyReturnValue;

            result.entity = entity;
            result.timeout = timeout;
            result.rendererHintSource = rendererHintSource;
            result.immediately = immediately;

            result.raw = raw;
            result.useDisplayHints = useDisplayHints;
            result.skipResolution = skipResolution;
            result.suppressBecauseSecret = suppressBecauseSecret;
            result.suppressSecrets = suppressSecrets;

            return result;
        }

        public ObjectMapper mapper() {
            if (mapperN==null) mapperN = BrooklynJacksonJsonProvider.findAnyObjectMapper(mgmt);
            return mapperN;
        }
        
        /** whether JSON is the ultimate product; 
         * main effect here is to give null for null if true, else to give empty string 
         * <p>
         * conversion to JSON for complex types is done subsequently (often by the framework)
         * <p>
         * default is true */
        public RestValueResolver preferJson(boolean preferJson) { this.preferJson = preferJson; return this; }
        /** whether an outermost string must be wrapped in quotes, because a String return object is treated as
         * already JSON-encoded
         * <p>
         * default is false */
        public RestValueResolver asJerseyOutermostReturnValue(boolean asJerseyReturnJson) {
            isJerseyReturnValue = asJerseyReturnJson;
            return this;
        }

        @Deprecated // since 1.0
        public RestValueResolver raw(Boolean raw) {
            this.raw = raw;
            return this;
        }
        public RestValueResolver useDisplayHints(Boolean useDisplayHints) {
            this.useDisplayHints = useDisplayHints;
            return this;
        }
        private boolean isUseDisplayHints() {
            if (raw!=null) {
                if (raw) {
                    // explicit non-default value takes precedence
                    // (REST API will not allow 'null')
                    return !raw;
                }
                // otherwise pass through
            }

            if (useDisplayHints!=null) return useDisplayHints;
            return true;
        }

        public boolean isSuppressedBecauseSecret() {
            return Boolean.TRUE.equals(suppressBecauseSecret);
        }

        public RestValueResolver skipResolution(Boolean skipResolution) {
            this.skipResolution = skipResolution;
            return this;
        }
        public RestValueResolver suppressIfSecret(String keyName, Boolean suppressIfSecret) {
            suppressSecrets = suppressIfSecret;

            if (Boolean.TRUE.equals(suppressIfSecret)) {
                if (Sanitizer.IS_SECRET_PREDICATE.apply(keyName)) {
                    suppressBecauseSecret = true;
                }
            } else {
                checkAndGetSecretsSuppressed(mgmt, suppressIfSecret, null);
            }

            return this;
        }

        public RestValueResolver context(Entity entity) { this.entity = entity; return this; }
        public RestValueResolver timeout(Duration timeout) { this.timeout = timeout; return this; }
        public RestValueResolver immediately(boolean immediately) { this.immediately = immediately; return this; }
        public RestValueResolver renderAs(Object rendererHintSource) { this.rendererHintSource = rendererHintSource; return this; }

        public Object resolve() {
            Object valueResult =
                    Boolean.TRUE.equals(skipResolution)
                            ? valueToResolve
                            : getImmediateValue(valueToResolve, entity, immediately, timeout);
            if (valueResult==UNRESOLVED) valueResult = valueToResolve;
            if (rendererHintSource!=null && isUseDisplayHints()) {
                valueResult = RendererHints.applyDisplayValueHintUnchecked(rendererHintSource, valueResult);
            }
            if (Boolean.TRUE.equals(suppressBecauseSecret)) {
                valueResult = suppressAsMinimalizedJson(mapper(), valueResult);

            }
            return getValueForDisplayAfterSecretsCheck(mgmt, mapper(), valueResult, preferJson, isJerseyReturnValue,
                    Boolean.TRUE.equals(suppressSecrets) && !Boolean.TRUE.equals(suppressBecauseSecret), false);
        }

        private static Object UNRESOLVED = "UNRESOLVED".toCharArray();
        
        private static Object getImmediateValue(Object value, @Nullable Entity context, @Nullable Boolean immediately, @Nullable Duration timeout) {
            return Tasks.resolving(value)
                    .as(Object.class)
                    .defaultValue(UNRESOLVED)
                    .deep()
                    .timeout(timeout)
                    .immediately(immediately == null ? false : immediately.booleanValue())
                    .context(context)
                    .swallowExceptions()
                    .get();
        }

        public Object getValueForDisplay(Object value, Boolean preferJson, Boolean isJerseyReturnValue, Boolean suppressNestedSecrets) {
            return getValueForDisplay(mgmt, mapper(), value,
                    preferJson!=null ? preferJson : this.preferJson, isJerseyReturnValue!=null ? isJerseyReturnValue : this.isJerseyReturnValue,
                    suppressNestedSecrets!=null ? suppressNestedSecrets : this.suppressSecrets, false);
        }
        public Object getValueForDisplay(Object value, Boolean preferJson, Boolean isJerseyReturnValue, Boolean suppressNestedSecrets, boolean suppressOutput) {
            return getValueForDisplay(mgmt, mapper(), value,
                    preferJson!=null ? preferJson : this.preferJson, isJerseyReturnValue!=null ? isJerseyReturnValue : this.isJerseyReturnValue,
                    suppressNestedSecrets!=null ? suppressNestedSecrets : this.suppressSecrets, suppressOutput);
        }

        public static Object getValueForDisplay(ManagementContext mgmt, ObjectMapper mapper, Object value, boolean preferJson, boolean isJerseyReturnValue, Boolean suppressNestedSecrets, Boolean suppressOutput) {
            suppressNestedSecrets = checkAndGetSecretsSuppressed(mgmt, suppressNestedSecrets, false);
            return getValueForDisplayAfterSecretsCheck(mgmt, mapper, value, preferJson, isJerseyReturnValue, suppressNestedSecrets,suppressOutput);
        }

        static Object getValueForDisplayAfterSecretsCheck(ManagementContext mgmt, ObjectMapper mapper, Object value, boolean preferJson, boolean isJerseyReturnValue, Boolean suppressNestedSecrets, Boolean suppressOutput) {
            if (preferJson) {
                if (value==null) return null;
                Object result = value;
                // no serialization checks required, with new smart-mapper which does toString
                // (note there is more sophisticated logic in git history however)
                result = value;
                if (result instanceof BrooklynDslDeferredSupplier) {
                    result = result.toString();
                }
                if (isJerseyReturnValue) {
                    if (result instanceof String) {
                        // Jersey does not do json encoding if the return type is a string,
                        // expecting the returner to do the json encoding himself
                        // cf discussion at https://github.com/dropwizard/dropwizard/issues/231
                        result = StringEscapes.JavaStringEscapes.wrapJavaString((String)result);
                    }
                }

                if (suppressNestedSecrets) {
                    if (result==null || Boxing.isPrimitiveOrBoxedObject(result)) {
                        // no action needed
                    } else if (result instanceof CharSequence) {
                        result = Sanitizer.sanitizeMultilineString(result.toString());
                    } else {
                        // go ahead and convert to json and suppress deep
                        try {
                            String resultS = mapper.writeValueAsString(result);
                            result = BeanWithTypeUtils.newSimpleMapper().readValue(resultS, Object.class);
                            if (suppressOutput){
                                result = TaskTransformer.suppressWorkflowOutputs(result);
                            }
                            //the below treats all numbers as doubles
                            //new Gson().fromJson(resultS, Object.class);
                            return Sanitizer.suppressNestedSecretsJson(result, true);
                        } catch (JsonProcessingException e) {
                            throw Exceptions.propagateAnnotated("Cannot serialize REST result", e);
                        }
                    }
                }

                return result;
            } else {
                if (value==null) return "";

                String resultS = value.toString();
                if (suppressNestedSecrets) {
                    if (Sanitizer.IS_SECRET_PREDICATE.apply(resultS)) {
                        return suppressAsMinimalizedJson(mapper, value);
                    }
                }
                return resultS;
            }
        }

        public static Boolean checkAndGetSecretsSuppressed(ManagementContext mgmt, Boolean suppressNestedSecrets, Boolean defaultValue) {
            if (Boolean.TRUE.equals(suppressNestedSecrets)) {
                return true;
            }
            boolean areSecretsAllowed = true; // TODO check in mgmt context if secrets are allowed (change this from static to get mgmt context!)
            if (!areSecretsAllowed) {
                // could throw, but might want API to default to blocking
                // if (Boolean.FALSE.equals(suppressNestedSecrets)) throwWebApplicationException(Response.Status.FORBIDDEN, "Not permitted to prevent suppression of secrets");
                return true;
            }
            boolean isDefaultsSecretSAllowed = true; // TODO check in mgmt context if secrets should default to being shown in API

            if (!isDefaultsSecretSAllowed) return true;
            if (suppressNestedSecrets==null) return defaultValue;
            return suppressNestedSecrets;
        }

        public static String suppressAsMinimalizedJson(ObjectMapper mapper, Object valueResult) {
            try {
                Object resultJ;
                if (valueResult==null) valueResult = ""; // treat null as empty string
                if (valueResult instanceof String) {
                    // don't wrap strings
                    resultJ = valueResult;
                } else {
                    String resultS = mapper.writeValueAsString(valueResult);
                    resultJ = new Gson().fromJson(resultS, Object.class);
                }
                return Sanitizer.suppressJson(resultJ, true);
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }
    }

}
