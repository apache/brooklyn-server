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

import java.io.Closeable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.mgmt.internal.BrooklynShutdownHooks;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.entity.brooklynnode.BrooklynNode;
import org.apache.brooklyn.entity.brooklynnode.LocalBrooklynNode;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.launcher.common.BasicLauncher;
import org.apache.brooklyn.launcher.common.BrooklynPropertiesFactoryHelper;
import org.apache.brooklyn.launcher.config.StopWhichAppsOnShutdown;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.BrooklynUserWithRandomPasswordSecurityProvider;
import org.apache.brooklyn.rest.util.ShutdownHandler;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.FatalRuntimeException;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Example usage is:
 *  * <pre>
 * {@code
 * BrooklynLauncher launcher = BrooklynLauncher.newInstance()
 *     .application(new WebClusterDatabaseExample().appDisplayName("Web-cluster example"))
 *     .location("localhost")
 *     .start();
 * 
 * Entities.dumpInfo(launcher.getApplications());
 * </pre>
 */
public class BrooklynLauncher extends BasicLauncher<BrooklynLauncher> {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynLauncher.class);

    /** Creates a configurable (fluent API) launcher for use starting the web console and Brooklyn applications. */
    public static BrooklynLauncher newInstance() {
        return new BrooklynLauncher();
    }
    
    private boolean startWebApps = true;
    private PortRange port = null;
    private Boolean useHttps = null;
    private InetAddress bindAddress = null;
    private InetAddress publicAddress = null;
    private List<WebAppContextProvider> webApps = new LinkedList<>();
    private Map<String, ?> webconsoleFlags = Maps.newLinkedHashMap();
    private Boolean skipSecurityFilter = null;
    
    private boolean ignoreWebErrors = false;
    
    private StopWhichAppsOnShutdown stopWhichAppsOnShutdown = StopWhichAppsOnShutdown.THESE_IF_NOT_PERSISTED;
    private ShutdownHandler shutdownHandler;
    
    private Function<ManagementContext,Void> customizeManagement = null;
    
    private volatile BrooklynWebServer webServer;

    private String globalBrooklynPropertiesFile = Os.mergePaths(Os.home(), ".brooklyn", "brooklyn.properties");
    private String localBrooklynPropertiesFile;

    public BrooklynServerDetails getServerDetails() {
        if (!isStarted()) throw new IllegalStateException("Cannot retrieve server details until started");
        return new BrooklynServerDetails(webServer, getManagementContext());
    }
    
    /** 
     * Specifies whether the launcher will start the Brooklyn web console 
     * (and any additional webapps specified); default true.
     */
    public BrooklynLauncher webconsole(boolean startWebApps) {
        this.startWebApps = startWebApps;
        return this;
    }

    public BrooklynLauncher installSecurityFilter(Boolean val) {
        this.skipSecurityFilter = val == null ? null : !val;
        return this;
    }

    /** 
     * As {@link #webconsolePort(PortRange)} taking a single port
     */ 
    public BrooklynLauncher webconsolePort(int port) {
        return webconsolePort(PortRanges.fromInteger(port));
    }

    /**
     * As {@link #webconsolePort(PortRange)} taking a string range
     */
    public BrooklynLauncher webconsolePort(String port) {
        if (port==null) return webconsolePort((PortRange)null);
        return webconsolePort(PortRanges.fromString(port));
    }

    /**
     * Specifies the port where the web console (and any additional webapps specified) will listen;
     * default (null) means "8081+" being the first available >= 8081 (or "8443+" for https).
     */ 
    public BrooklynLauncher webconsolePort(PortRange port) {
        this.port = port;
        return this;
    }

    /**
     * Specifies whether the webconsole should use https.
     */ 
    public BrooklynLauncher webconsoleHttps(Boolean useHttps) {
        this.useHttps = useHttps;
        return this;
    }

    /**
     * Specifies the NIC where the web console (and any additional webapps specified) will be bound;
     * default 0.0.0.0, unless no security is specified (e.g. users) in which case it is localhost.
     */ 
    public BrooklynLauncher bindAddress(InetAddress bindAddress) {
        this.bindAddress = bindAddress;
        return this;
    }

    /**
     * Specifies the address that the management context's REST API will be available on. Defaults
     * to {@link #bindAddress} if it is not 0.0.0.0.
     * @see #bindAddress(java.net.InetAddress)
     */
    public BrooklynLauncher publicAddress(InetAddress publicAddress) {
        this.publicAddress = publicAddress;
        return this;
    }

    /**
     * Specifies additional flags to be passed to {@link BrooklynWebServer}.
     */ 
    public BrooklynLauncher webServerFlags(Map<String,?> webServerFlags) {
        this.webconsoleFlags  = webServerFlags;
        return this;
    }

    /** 
     * Specifies an additional webapp to host on the webconsole port.
     * @param contextPath The context path (e.g. "/hello", or equivalently just "hello") where the webapp will be hosted.
     *      "/" will override the brooklyn console webapp.
     * @param warUrl The URL from which the WAR should be loaded, supporting classpath:// protocol in addition to file:// and http(s)://.
     */
    public BrooklynLauncher webapp(String contextPath, String warUrl) {
        webApps.add(new WebAppContextProvider(contextPath, warUrl));
        return this;
    }

    /**
     * @see #webapp(String, String)
     */
    public BrooklynLauncher webapp(WebAppContextProvider contextProvider) {
        webApps.add(contextProvider);
        return this;
    }

    public BrooklynLauncher ignoreWebErrors(boolean ignoreWebErrors) {
        this.ignoreWebErrors = ignoreWebErrors;
        return this;
    }

    public BrooklynLauncher stopWhichAppsOnShutdown(StopWhichAppsOnShutdown stopWhich) {
        this.stopWhichAppsOnShutdown = stopWhich;
        return this;
    }

    public BrooklynLauncher customizeManagement(Function<ManagementContext,Void> customizeManagement) {
        this.customizeManagement = customizeManagement;
        return this;
    }

    public BrooklynLauncher shutdownOnExit(boolean val) {
        LOG.warn("Call to deprecated `shutdownOnExit`", new Throwable("source of deprecated call"));
        stopWhichAppsOnShutdown = StopWhichAppsOnShutdown.THESE_IF_NOT_PERSISTED;
        return this;
    }

    /**
     * A listener to call when the user requests a shutdown (i.e. through the REST API)
     */
    public BrooklynLauncher shutdownHandler(ShutdownHandler shutdownHandler) {
        this.shutdownHandler = shutdownHandler;
        return this;
    }

    @Override
    protected void initManagementContext() {
        setBrooklynPropertiesBuilder(
                new BrooklynPropertiesFactoryHelper(
                        globalBrooklynPropertiesFile,
                        localBrooklynPropertiesFile,
                        getBrooklynProperties())
                .createPropertiesBuilder());

        boolean isManagementContextSet = getManagementContext() != null;

        super.initManagementContext();

        if (!isManagementContextSet) {
            // We created the management context, so we are responsible for terminating it
            BrooklynShutdownHooks.invokeTerminateOnShutdown(getManagementContext());
        }

        if (customizeManagement!=null) {
            customizeManagement.apply(getManagementContext());
        }
    }

    @Override
    protected void startingUp() {
        super.startingUp();

        // Start webapps as soon as mgmt context available -- can use them to detect progress of other processes
        if (startWebApps) {
            try {
                startWebApps();
            } catch (Exception e) {
                handleSubsystemStartupError(ignoreWebErrors, "core web apps", e);
            }
        }
    }

    protected void startWebApps() {
        ManagementContext managementContext = getManagementContext();
        BrooklynProperties brooklynProperties = (BrooklynProperties) managementContext.getConfig();

        // No security options in properties and no command line options overriding.
        if (Boolean.TRUE.equals(skipSecurityFilter) && bindAddress==null) {
            LOG.info("Starting Brooklyn web-console on loopback because security is explicitly disabled and no bind address specified");
            bindAddress = Networking.LOOPBACK;
        } else if (BrooklynWebConfig.hasNoSecurityOptions(managementContext.getConfig())) {
            LOG.info("No security provider options specified. Define a security provider or users to prevent a random password being created and logged.");
            
            if (bindAddress==null) {
                LOG.info("Starting Brooklyn web-console with passwordless access on localhost and protected access from any other interfaces (no bind address specified)");
            } else {
                if (Arrays.equals(new byte[] { 127, 0, 0, 1 }, bindAddress.getAddress())) { 
                    LOG.info("Starting Brooklyn web-console with passwordless access on localhost");
                } else if (Arrays.equals(new byte[] { 0, 0, 0, 0 }, bindAddress.getAddress())) { 
                    LOG.info("Starting Brooklyn web-console with passwordless access on localhost and random password (logged) required from any other interfaces");
                } else { 
                    LOG.info("Starting Brooklyn web-console with passwordless access on localhost (if permitted) and random password (logged) required from any other interfaces");
                }
            }
            brooklynProperties.put(
                    BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE,
                    new BrooklynUserWithRandomPasswordSecurityProvider(managementContext));
        } else {
            LOG.debug("Starting Brooklyn using security properties: "+brooklynProperties.submap(ConfigPredicates.nameStartsWith(BrooklynWebConfig.BASE_NAME_SECURITY)).asMapWithStringKeys());
        }
        if (bindAddress == null) bindAddress = Networking.ANY_NIC;

        LOG.debug("Starting Brooklyn web-console with bindAddress "+bindAddress+" and properties "+brooklynProperties);
        try {
            webServer = new BrooklynWebServer(webconsoleFlags, managementContext);
            webServer.setBindAddress(bindAddress);
            webServer.setPublicAddress(publicAddress);
            if (port!=null) webServer.setPort(port);
            if (useHttps!=null) webServer.setHttpsEnabled(useHttps);
            webServer.setShutdownHandler(shutdownHandler);
            webServer.putAttributes(brooklynProperties);
            webServer.skipSecurity(Boolean.TRUE.equals(skipSecurityFilter));
            for (WebAppContextProvider webapp : webApps) {
                webServer.addWar(webapp);
            }
            webServer.start();

        } catch (Exception e) {
            LOG.warn("Failed to start Brooklyn web-console (rethrowing): " + Exceptions.collapseText(e));
            throw new FatalRuntimeException("Failed to start Brooklyn web-console: " + Exceptions.collapseText(e), e);
        }
    }

    @Override
    protected void startBrooklynNode() {
        if (webServer == null || !startWebApps) {
            LOG.info("Skipping BrooklynNode entity creation, BrooklynWebServer not running");
            return;
        }
        super.startBrooklynNode();
    }

    protected EntitySpec<LocalBrooklynNode> customizeBrooklynNodeSpec(EntitySpec<LocalBrooklynNode> brooklynNodeSpec) {
        return brooklynNodeSpec
                .configure(SoftwareProcess.RUN_DIR, System.getenv("ROOT"))
                .configure(SoftwareProcess.INSTALL_DIR, System.getenv("BROOKLYN_HOME"))
                .configure(BrooklynNode.ENABLED_HTTP_PROTOCOLS, ImmutableList.of(webServer.getHttpsEnabled() ? "https" : "http"))
                .configure(webServer.getHttpsEnabled() ? BrooklynNode.HTTPS_PORT : BrooklynNode.HTTP_PORT, PortRanges.fromInteger(webServer.getActualPort()))
                .configure(BrooklynNode.WEB_CONSOLE_BIND_ADDRESS, bindAddress)
                .configure(BrooklynNode.WEB_CONSOLE_PUBLIC_ADDRESS, publicAddress)
                .configure(BrooklynNode.NO_WEB_CONSOLE_AUTHENTICATION, Boolean.TRUE.equals(skipSecurityFilter));
    }

    protected void startApps() {
        if ((stopWhichAppsOnShutdown==StopWhichAppsOnShutdown.ALL) ||
            (stopWhichAppsOnShutdown==StopWhichAppsOnShutdown.ALL_IF_NOT_PERSISTED && getPersistMode()==PersistMode.DISABLED)) {
            BrooklynShutdownHooks.invokeStopAppsOnShutdown(getManagementContext());
        }

        for (Application app : getApplications()) {
            if (app instanceof Startable) {

                if ((stopWhichAppsOnShutdown==StopWhichAppsOnShutdown.THESE) || 
                    (stopWhichAppsOnShutdown==StopWhichAppsOnShutdown.THESE_IF_NOT_PERSISTED && getPersistMode()==PersistMode.DISABLED)) {
                    BrooklynShutdownHooks.invokeStopOnShutdown(app);
                }
            }
        }
        super.startApps();
    }


    /**
     * Terminates this launch, but does <em>not</em> stop the applications (i.e. external processes
     * are left running, etc). However, by terminating the management console the brooklyn applications
     * become unusable.
     */
    public void terminate() {
        if (!isStarted()) return; // no-op

        if (webServer != null) {
            try {
                webServer.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping web-server; continuing with termination", e);
            }
        }

        ManagementContext managementContext = getManagementContext();

        // TODO Do we want to do this as part of managementContext.terminate, so after other threads are terminated etc?
        // Otherwise the app can change between this persist and the terminate.
        if (getPersistMode() != PersistMode.DISABLED) {
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                if (managementContext.getHighAvailabilityManager().getPersister() != null) {
                    managementContext.getHighAvailabilityManager().getPersister().waitForWritesCompleted(Duration.TEN_SECONDS);
                }
                managementContext.getRebindManager().waitForPendingComplete(Duration.TEN_SECONDS, true);
                LOG.info("Finished waiting for persist; took "+Time.makeTimeStringRounded(stopwatch));
            } catch (RuntimeInterruptedException e) {
                Thread.currentThread().interrupt(); // keep going with shutdown
                LOG.warn("Persistence interrupted during shutdown: "+e, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // keep going with shutdown
                LOG.warn("Persistence interrupted during shutdown: "+e, e);
            } catch (TimeoutException e) {
                LOG.warn("Timeout after 10 seconds waiting for persistence to write all data; continuing");
            }
        }

        if (managementContext instanceof ManagementContextInternal) {
            ((ManagementContextInternal)managementContext).terminate();
        }

        for (Location loc : getLocations()) {
            if (loc instanceof Closeable) {
                Streams.closeQuietly((Closeable)loc);
            }
        }
    }

    public BrooklynLauncher globalBrooklynPropertiesFile(String file) {
        globalBrooklynPropertiesFile = file;
        return this;
    }

    public BrooklynLauncher localBrooklynPropertiesFile(String file) {
        localBrooklynPropertiesFile = file;
        return this;
    }

}
