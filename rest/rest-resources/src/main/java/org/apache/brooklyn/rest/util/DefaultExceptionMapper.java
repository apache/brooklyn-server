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
package org.apache.brooklyn.rest.util;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementClass;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.domain.ApiError.Builder;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.javalang.coerce.ClassCoercionException;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.error.YAMLException;

import com.google.common.annotations.Beta;

@Provider
public class DefaultExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExceptionMapper.class);

    static Set<Class<?>> encounteredUnknownExceptions = MutableSet.of();
    static Set<Object> encounteredExceptionRecords = MutableSet.of();

    @Context
    private ContextResolver<ManagementContext> mgmt;

    @Context
    private ResourceContext resourceContext;

    /**
     * Maps a throwable to a response.
     * <p/>
     * Returns {@link WebApplicationException#getResponse} if the exception is an instance of
     * {@link WebApplicationException}. Otherwise maps known exceptions to responses. If no
     * mapping is found a {@link Status#INTERNAL_SERVER_ERROR} is assumed.
     */
    @Override
    public Response toResponse(Throwable throwable1) {
        String user = Strings.toString(Entitlements.getEntitlementContext());
        if (user==null) user = "<not_logged_in>";

        String path = null;
        try {
            // get path if possible for all requests
            ContainerRequestContext requestContext = resourceContext.getResource(ContainerRequestContext.class);
            path = requestContext.getUriInfo().getPath();
            if (LOG.isTraceEnabled()) {
                LOG.trace("ERROR CONTEXT DETAILS for "+throwable1);
                LOG.trace("url: "+requestContext.getUriInfo());
                LOG.trace("headers: "+requestContext.getHeaders());
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            if (firstEncounter(e)) {
                // include debug trace for everything the first time we get one
                LOG.debug("Unable to resolve path on error "+throwable1+" (logging once): "+e);
            }
        }
        if (path==null) path = "<path_unavailable>";

        // EofException is thrown when the connection is reset,
        // for example when refreshing the browser window.
        // Don't depend on jetty, could be running in other environments as well.
        if (throwable1.getClass().getName().equals("org.eclipse.jetty.io.EofException")) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("REST request {} running as user {} was disconnected, threw: {}",
                        path, user,
                        Exceptions.collapseText(throwable1));
            }
            return null;
        }

        String errorReference = Identifiers.makeRandomId(13);
        Throwable throwable2 = Exceptions.getFirstInteresting(throwable1);
        if (isSevere(throwable2)) {
            LOG.warn("REST request {} running as {} threw severe: {} (ref {})",
                    path, user,
                    Exceptions.collapseText(throwable1), errorReference);
        } else {
            LOG.debug("REST request {} running as {} threw: {} (ref {})",
                    path, user,
                    Exceptions.collapseText(throwable1), errorReference);
        }
        logExceptionDetailsForDebugging(throwable1, errorReference);

        // Some methods will throw this, which gets converted automatically
        if (throwable2 instanceof WebApplicationException) {
            if (throwable2 instanceof ApiError.WebApplicationExceptionWithApiError) {
                ApiError apiError = ((ApiError.WebApplicationExceptionWithApiError)throwable2).getApiError();
                if (!isTraceVisibleToUser()) {
                    if (apiError.getDetails() != null) {
                        LOG.debug("Details of suppressed API error ref " + errorReference + ": " + apiError.getDetails());
                    }
                    return ApiError.builder().message(apiError.getMessage()).details("Reference: " + errorReference).errorCode(apiError.getError()).build().asJsonResponse();
                } else {
                    // include error reference
                    return ApiError.builder().copy(apiError).message(""+apiError.getMessage()+" (Reference: "+errorReference+")").build().asJsonResponse();
                }
            } else {
                WebApplicationException wae = (WebApplicationException) throwable2;
                // could suppress all messages if anything includes unwanted details (but believed not to be the case)
                // note error reference not included, but we'll live with that for this error which isn't used (will still be logged above, just not reported to user)
                return wae.getResponse();
            }
        }

        // The nicest way for methods to provide errors, wrap as this, and the stack trace will be suppressed
        if (throwable2 instanceof UserFacingException) {
            return ApiError.of(throwable2.getMessage()).asBadRequestResponseJson();
        }

        // For everything else, a trace is supplied unless blocked
        
        // Assume ClassCoercionExceptions are caused by TypeCoercions from input parameters gone wrong
        // And IllegalArgumentException for malformed input parameters.
        if (throwable2 instanceof ClassCoercionException || throwable2 instanceof IllegalArgumentException) {
            if (!isTraceVisibleToUser()) {
                return ApiError.of(throwable2.getMessage()).asBadRequestResponseJson();
            }
            return ApiError.of(throwable2).asBadRequestResponseJson();
        }

        // YAML exception 
        if (throwable2 instanceof YAMLException) {
            return ApiError.builder().message(throwable2.getMessage()).details("Reference: "+errorReference).prefixMessage("Invalid YAML").build().asBadRequestResponseJson();
        }

        if (!isTooUninterestingToLogWarn(throwable2)) {
            if (encounteredUnknownExceptions.add( throwable2.getClass() )) {
                LOG.warn("REST call reference "+errorReference+" generated exception type "+throwable2.getClass()+" unrecognized in "+getClass()+" (subsequent occurrences will be logged debug only): " + throwable2, throwable2);
            }
        }

        // Before saying server error, look for a user-facing exception anywhere in the hierarchy
        UserFacingException userFacing = Exceptions.getFirstThrowableOfType(throwable1, UserFacingException.class);
        if (userFacing instanceof UserFacingException) {
            return ApiError.builder().message(userFacing.getMessage()).details("Reference: "+errorReference).build().asBadRequestResponseJson();
        }

        if (!isTraceVisibleToUser()) {
            return ApiError.builder()
                    .message("Internal error. Contact server administrator citing reference " + errorReference +" to consult logs for more details.")
                    .details("Reference: "+errorReference)
                    .build().asResponse(Status.INTERNAL_SERVER_ERROR, MediaType.APPLICATION_JSON_TYPE);
        } else {
            Builder rb = ApiError.builderFromThrowable(Exceptions.collapse(throwable2));
            if (Strings.isBlank(rb.getMessage())) {
                rb.message("Internal error. Contact server administrator citing reference " + errorReference + " to consult logs for more details.");
            }
            rb.details("Reference: "+errorReference);
            return rb.build().asResponse(Status.INTERNAL_SERVER_ERROR, MediaType.APPLICATION_JSON_TYPE);
        }
    }


    public static final ConfigKey<ReturnStackTraceMode> REST_RETURN_STACK_TRACES = ConfigKeys.newConfigKey(ReturnStackTraceMode.class,
            MultiSessionAttributeAdapter.OAB_SERVER_CONFIG_PREFIX+".returnStackTraces",
            "Whether REST requests that have errors can include stack traces; " +
                    "'true' or 'all' to mean always, 'false' or 'none' to mean never, " +
                    "and otherwise 'root' or 'power' to allow users with root or java entitlement",
            ReturnStackTraceMode.ALL);

    boolean isTraceVisibleToUser() {
        ManagementContext m = mgmt.getContext(ManagementContext.class);
        if (m==null) return true;
        try {
            ReturnStackTraceMode mode = m.getConfig().getConfig(REST_RETURN_STACK_TRACES);
            if (mode==null) mode = ReturnStackTraceMode.ALL;
            return mode.checkCurrentUser(m);
        } catch (Exception e) {
            LOG.warn("Error checking user permissions for nested exception; will log and return original exception, with stack traces shown here", e);
            return false;
        }
    }
    public enum ReturnStackTraceMode {
        ALL( (_m,_ec) -> true),
        TRUE( (_m,_ec) -> true),
        NONE( (_m,_ec) -> false),
        FALSE( (_m,_ec) -> false),
        ROOT( (m,ec) -> Entitlements.isEntitled(m.getEntitlementManager(), Entitlements.ROOT, null) ),
        POWER( (m,ec) -> Entitlements.isEntitled(m.getEntitlementManager(), Entitlements.ADD_JAVA, null) ),
        POWERUSER( (m,ec) -> Entitlements.isEntitled(m.getEntitlementManager(), Entitlements.ADD_JAVA, null) ),
        POWER_USER( (m,ec) -> Entitlements.isEntitled(m.getEntitlementManager(), Entitlements.ADD_JAVA, null) ),
        ADDJAVA( (m,ec) -> Entitlements.isEntitled(m.getEntitlementManager(), Entitlements.ADD_JAVA, null) ),
        ADD_JAVA( (m,ec) -> Entitlements.isEntitled(m.getEntitlementManager(), Entitlements.ADD_JAVA, null) ),
        ;
        final BiPredicate<ManagementContext,EntitlementContext> fn;
        ReturnStackTraceMode(BiPredicate<ManagementContext,EntitlementContext> fn) {
            this.fn = fn;
        }

        public boolean checkCurrentUser(ManagementContext m) {
            return m==null ? false : fn.test(m, Entitlements.getEntitlementContext());
        }
    }

    protected boolean isTooUninterestingToLogWarn(Throwable throwable2) {
        // we used to do this which excluded too much
        //return Exceptions.isPrefixBoring(throwable2);

        // for now we do this which probably includes some things we'd like to suppress (eg when starting up) but we can improve subsequently
        return false;
    }

    /** Logs full details at trace, unless it is the first time we've seen this in which case it is debug */
    @Beta
    public static void logExceptionDetailsForDebugging(Throwable t) {
        logExceptionDetailsForDebugging(t, null);
    }
    @Beta
    public static void logExceptionDetailsForDebugging(Throwable t, String errorReference) {
        if (LOG.isDebugEnabled()) {
            if (firstEncounter(t)) {
                // include debug trace for everything the first time we get one
                LOG.debug("Full details of error "+(errorReference!=null ? "ref "+errorReference : "(user "+Entitlements.getEntitlementContext()+")")+": "+t+
                        " (logging debug on first encounter; subsequent instances will be logged trace)", t);
            } else if (LOG.isTraceEnabled()) {
                LOG.trace("Full details of error "+(errorReference!=null ? "ref "+errorReference : "(user "+Entitlements.getEntitlementContext()+")")+": "+t, t);
            }
        }
    }
        
    private static boolean firstEncounter(Throwable t) {
        Set<Object> record = MutableSet.of();
        while (true) {
            record.add(t.getClass());
            if (t.getStackTrace().length>0) {
                if (record.add(t.getStackTrace()[0])) break;
            }
            t = t.getCause();
            if (t==null) break;
        }
        return encounteredExceptionRecords.add(record);
    }

    protected boolean isSevere(Throwable t) {
        // some things, like this, we want more prominent server notice of
        // (the list could be much larger but this is a start)
        if (t instanceof OutOfMemoryError) return true;
        return false;
    }
}
