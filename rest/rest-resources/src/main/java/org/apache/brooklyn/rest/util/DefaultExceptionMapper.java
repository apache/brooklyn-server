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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.domain.ApiError.Builder;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.javalang.coerce.ClassCoercionException;
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
    
    /**
     * Maps a throwable to a response.
     * <p/>
     * Returns {@link WebApplicationException#getResponse} if the exception is an instance of
     * {@link WebApplicationException}. Otherwise maps known exceptions to responses. If no
     * mapping is found a {@link Status#INTERNAL_SERVER_ERROR} is assumed.
     */
    @Override
    public Response toResponse(Throwable throwable1) {
        // EofException is thrown when the connection is reset,
        // for example when refreshing the browser window.
        // Don't depend on jetty, could be running in other environments as well.
        if (throwable1.getClass().getName().equals("org.eclipse.jetty.io.EofException")) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("REST request running as {} was disconnected, threw: {}", Entitlements.getEntitlementContext(), 
                        Exceptions.collapseText(throwable1));
            }
            return null;
        }

        Throwable throwable2 = Exceptions.getFirstInteresting(throwable1);
        if (isSevere(throwable2)) {
            LOG.warn("REST request running as {} threw: {}", Entitlements.getEntitlementContext(), 
                Exceptions.collapseText(throwable1));
        } else {
            LOG.debug("REST request running as {} threw: {}", Entitlements.getEntitlementContext(), 
                Exceptions.collapseText(throwable1));
        }
        logExceptionDetailsForDebugging(throwable1);

        // Some methods will throw this, which gets converted automatically
        if (throwable2 instanceof WebApplicationException) {
            WebApplicationException wae = (WebApplicationException) throwable2;
            return wae.getResponse();
        }

        // The nicest way for methods to provide errors, wrap as this, and the stack trace will be suppressed
        if (throwable2 instanceof UserFacingException) {
            return ApiError.of(throwable2.getMessage()).asBadRequestResponseJson();
        }

        // For everything else, a trace is supplied
        
        // Assume ClassCoercionExceptions are caused by TypeCoercions from input parameters gone wrong
        // And IllegalArgumentException for malformed input parameters.
        if (throwable2 instanceof ClassCoercionException || throwable2 instanceof IllegalArgumentException) {
            return ApiError.of(throwable2).asBadRequestResponseJson();
        }

        // YAML exception 
        if (throwable2 instanceof YAMLException) {
            return ApiError.builder().message(throwable2.getMessage()).prefixMessage("Invalid YAML").build().asBadRequestResponseJson();
        }

        if (!isTooUninterestingToLogWarn(throwable2)) {
            if ( encounteredUnknownExceptions.add( throwable2.getClass() )) {
                LOG.warn("REST call generated exception type "+throwable2.getClass()+" unrecognized in "+getClass()+" (subsequent occurrences will be logged debug only): " + throwable2, throwable2);
            }
        }

        // Before saying server error, look for a user-facing exception anywhere in the hierarchy
        UserFacingException userFacing = Exceptions.getFirstThrowableOfType(throwable1, UserFacingException.class);
        if (userFacing instanceof UserFacingException) {
            return ApiError.of(userFacing.getMessage()).asBadRequestResponseJson();
        }
        
        Builder rb = ApiError.builderFromThrowable(Exceptions.collapse(throwable2));
        if (Strings.isBlank(rb.getMessage()))
            rb.message("Internal error. Contact server administrator to consult logs for more details.");
        return rb.build().asResponse(Status.INTERNAL_SERVER_ERROR, MediaType.APPLICATION_JSON_TYPE);
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
        if (LOG.isDebugEnabled()) {
            if (firstEncounter(t)) {
                // include debug trace for everything the first time we get one
                LOG.debug("Full details of "+Entitlements.getEntitlementContext()+" "+t+" (logging debug on first encounter; subsequent instances will be logged trace)", t);
            } else if (LOG.isTraceEnabled()) {
                LOG.trace("Full details of "+Entitlements.getEntitlementContext()+" "+t, t);
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
