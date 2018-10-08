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
package org.apache.brooklyn.launcher;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.server.BrooklynServiceAttributes;
import org.apache.brooklyn.launcher.config.CustomResourceLocator;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Identifiers;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a {@link WebAppContext} for a web app running in the same Jetty container as
 * the main Brooklyn app.
 */
public class WebAppContextProvider {

    private static final Logger LOG = LoggerFactory.getLogger(WebAppContextProvider.class);

    protected final String pathSpec;
    protected final String warUrl;

    /**
     * @param pathSpec The path at which the war should be served.
     * @param warUrl The url from which the war should be obtained.
     */
    public WebAppContextProvider(String pathSpec, String warUrl) {
        this.warUrl = checkNotNull(warUrl, "warUrl");
        String cleanPathSpec = checkNotNull(pathSpec, "pathSpec");
        while (cleanPathSpec.startsWith("/")) {
            cleanPathSpec = cleanPathSpec.substring(1);
        }
        this.pathSpec = cleanPathSpec;
    }

    /**
     * Serve given WAR at the given pathSpec; if not yet started, it is simply remembered until start;
     * if server already running, the context for this WAR is started.
     * @return the context created and added as a handler (and possibly already started if server is
     * started, so be careful with any changes you make to it!)
     */
    public WebAppContext get(ManagementContext managementContext, Map<String, Object> attributes, boolean ignoreFailures) {
        checkNotNull(managementContext, "managementContext");
        checkNotNull(attributes, "attributes");
        boolean isRoot = pathSpec.isEmpty();

        final WebAppContext context = new WebAppContext();
        // use a unique session ID to prevent interference with other web apps on same server (esp for localhost);
        // it might be better to make this brooklyn-only or base on the management-plane ID;
        // but i think it actually *is* per-server instance, since we don't cache sessions server-side,
        // so i think this is write. [Alex 2015-09]
        context.setInitParameter(SessionManager.__SessionCookieProperty, SessionManager.__DefaultSessionCookie + "_" + "BROOKLYN" + Identifiers.makeRandomId(6));
        context.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
        context.setAttribute(BrooklynServiceAttributes.BROOKLYN_MANAGEMENT_CONTEXT, managementContext);
        for (Map.Entry<String, Object> attributeEntry : attributes.entrySet()) {
            context.setAttribute(attributeEntry.getKey(), attributeEntry.getValue());
        }

        try {
            final CustomResourceLocator locator = new CustomResourceLocator(managementContext.getConfig(), ResourceUtils.create(this));
            final InputStream resource = locator.getResourceFromUrl(warUrl);
            final String warName = isRoot ? "ROOT" : ("embedded-" + pathSpec);
            File tmpWarFile = Os.writeToTempFile(resource, warName, ".war");
            context.setWar(tmpWarFile.getAbsolutePath());
        } catch (Exception e) {
            LOG.warn("Failed to deploy webapp " + pathSpec + " from " + warUrl
                    + (ignoreFailures ? "; launching run without WAR" : " (rethrowing)")
                    + ": " + Exceptions.collapseText(e));
            if (!ignoreFailures) {
                throw new IllegalStateException("Failed to deploy webapp " + pathSpec + " from " + warUrl + ": " + Exceptions.collapseText(e), e);
            }
            LOG.debug("Detail on failure to deploy webapp: " + e, e);
            context.setWar("/dev/null");
        }

        context.setContextPath("/" + pathSpec);
        context.setParentLoaderPriority(true);

        return context;
    }

    public String getPath() {
        return pathSpec;
    }

    public String getWarUrl() {
        return warUrl;
    }

    @Override
    public String toString() {
        final String path = this.pathSpec.isEmpty() ? "/" : this.pathSpec;
        return warUrl + "@" + path;
    }

}
