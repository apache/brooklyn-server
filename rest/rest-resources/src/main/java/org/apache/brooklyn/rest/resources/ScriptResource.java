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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.rest.api.ScriptApi;
import org.apache.brooklyn.rest.domain.ScriptExecutionSummary;
import org.apache.brooklyn.rest.util.MultiSessionAttributeAdapter;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.stream.ThreadLocalPrintStream;
import org.apache.brooklyn.util.stream.ThreadLocalPrintStream.OutputCapturingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

public class ScriptResource extends AbstractBrooklynRestResource implements ScriptApi {

    private static final Logger log = LoggerFactory.getLogger(ScriptResource.class);
    
    public static final String USER_DATA_MAP_SESSION_ATTRIBUTE = "brooklyn.script.groovy.user.data";
    public static final String USER_LAST_VALUE_SESSION_ATTRIBUTE = "brooklyn.script.groovy.user.last";
    
    @SuppressWarnings("rawtypes")
    @Override
    public ScriptExecutionSummary groovy(HttpServletRequest request, String script) {
        boolean entitledGS = Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.EXECUTE_GROOVY_SCRIPT, null);
        boolean entitledS = Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.EXECUTE_SCRIPT, null);

        if (!entitledS || !entitledGS) {
            if (entitledGS) log.warn("User '"+Entitlements.getEntitlementContext().user()+"' is entitled to run groovy scripts but not scripts. The two permissions should be equivalent and the former may be removed, blocking access. Either grant permission to execute scripts or remove permission to execute groovy scripts.");
            else throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        }

        log.info("Web REST executing user-supplied script");
        if (log.isDebugEnabled()) {
            log.debug("Web REST user-supplied script contents:\n"+script);
        }
        
        Binding binding = new Binding();
        binding.setVariable("mgmt", mgmt());
        
        MultiSessionAttributeAdapter session = request==null ? null : MultiSessionAttributeAdapter.of(request);
        if (session!=null) {
            Map data = (Map) session.getAttribute(USER_DATA_MAP_SESSION_ATTRIBUTE);
            if (data==null) {
                data = new LinkedHashMap();
                session.setAttribute(USER_DATA_MAP_SESSION_ATTRIBUTE, data);
            }
            binding.setVariable("data", data);

            Object last = session.getAttribute(USER_LAST_VALUE_SESSION_ATTRIBUTE);
            binding.setVariable("last", last);
        }
        
        GroovyShell shell = new GroovyShell(binding);

        OutputCapturingContext stdout = ThreadLocalPrintStream.stdout().captureTee();
        OutputCapturingContext stderr = ThreadLocalPrintStream.stderr().captureTee();

        Object value = null;
        Throwable problem = null;
        try {
            value = shell.evaluate(script);
            if (session!=null)
                session.setAttribute(USER_LAST_VALUE_SESSION_ATTRIBUTE, value);
        } catch (Throwable t) {
            log.warn("Problem in user-supplied script: "+t, t);
            problem = t;
        } finally {
            stdout.end();
            stderr.end();
        }

        if (log.isDebugEnabled()) {
            log.debug("Web REST user-supplied script completed:\n"+
                    (value!=null ? "RESULT: "+value.toString()+"\n" : "")+ 
                    (problem!=null ? "ERROR: "+problem.toString()+"\n" : "")+
                    (!stdout.isEmpty() ? "STDOUT: "+stdout.toString()+"\n" : "")+
                    (!stderr.isEmpty() ? "STDERR: "+stderr.toString()+"\n" : ""));
        }

        // call toString on the result, in case it is not serializable
        return new ScriptExecutionSummary(
                value!=null ? value.toString() : null, 
                problem!=null ? problem.toString() : null,
                stdout.toString(), stderr.toString());
    }
}
