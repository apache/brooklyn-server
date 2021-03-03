/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.launcher.osgi;

import com.google.common.base.Stopwatch;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.BrooklynVersionService;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.BrooklynShutdownHooks;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.typereg.BrooklynCatalogBundleResolver;
import org.apache.brooklyn.launcher.common.BasicLauncher;
import org.apache.brooklyn.launcher.common.BrooklynPropertiesFactoryHelper;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.BrooklynUserWithRandomPasswordSecurityProvider;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.javalang.Threads;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.osgi.framework.*;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.startlevel.FrameworkStartLevel;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * Initializer for brooklyn-core when running in an OSGi environment.
 */
public class OsgiLauncherImpl extends BasicLauncher<OsgiLauncherImpl> implements OsgiLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(OsgiLauncherImpl.class);
    public static final String BROOKLYN_CONFIG_PID = "brooklyn";

    /** Takes a JSON representation or comma separated list of OSGi filter expressions (cf {@link BundleContext#getServiceReferences(String, String)},
     * where each filter must be satisfied before the catalog will be initialized.
     * <p>
     * For example:
     * <code>(&(osgi.service.blueprint.compname=customBundleResolver))</code> to wait for a service registered via a blueprint with component name 'customBundleResolver', or
     * or
     * <code>["(&(osgi.service.blueprint.compname=customBundleResolver))","(&(osgi.service.blueprint.compname=customTypePlanTransformer))"]
     * to wait for two services registered, 'customBundleResolver' and 'customTypePlanTransformer'</code>.
     * <p>
     * This can be required where some catalog or rebind items depend on services installed during startup.
     * */
    public static final String BROOKLYN_OSGI_DEPENDENCIES_SERVICES_FILTERS = "brooklyn.osgi.dependencies.services.filters";
    public static final String BROOKLYN_OSGI_DEPENDENCIES_SERVICES_TIMEOUT = "brooklyn.osgi.dependencies.services.timeout";
    public static final String BROOKLYN_OSGI_STARTLEVEL_POSTINIT = "brooklyn.osgi.startlevel.postinit";

    private Object reloadLock = new Object();

    private BrooklynVersionService brooklynVersion;

    private String globalBrooklynProperties;
    private String localBrooklynProperties;
    private String defaultCatalogLocation;

    private ConfigurationAdmin configAdmin;
    private ConfigSupplier configSupplier;


    @Override
    public OsgiLauncherImpl startPartOne() {
        // make sure brooklyn-core bundle is started
        brooklynVersion.getVersion();

        Configuration brooklynConfig = getConfiguration(BROOKLYN_CONFIG_PID);
        // Note that this doesn't check whether the files exist, just that there are potential alternative sources for configuration.
        if (brooklynConfig == null && Strings.isEmpty(globalBrooklynProperties) && Strings.isEmpty(localBrooklynProperties)) {
            LOG.warn("Config Admin PID '" + BROOKLYN_CONFIG_PID + "' not found, not using external configuration. Create a brooklyn.cfg file in etc folder.");
        }
        configSupplier = new ConfigSupplier(brooklynConfig);
        BrooklynPropertiesFactoryHelper helper = new BrooklynPropertiesFactoryHelper(
                globalBrooklynProperties, localBrooklynProperties, configSupplier);
        setBrooklynPropertiesBuilder(helper.createPropertiesBuilder());
        return super.startPartOne();
    }

    private Configuration getConfiguration(String configPid) {
        String filter = '(' + Constants.SERVICE_PID + '=' + configPid + ')';
        Configuration[] configs;
        try {
            configs = configAdmin.listConfigurations(filter);
        } catch (InvalidSyntaxException | IOException e) {
            throw Exceptions.propagate(e);
        }
        if (configs != null && configs.length > 0) {
            return configs[0];
        } else {
            return null;
        }
    }

    // init-method can't find the start method for some reason, provide an alternative.
    @Override
    public void initOsgi() {
        synchronized (reloadLock) {
            final Stopwatch startupTimer = Stopwatch.createStarted();
            BrooklynShutdownHooks.resetShutdownFlag();
            LOG.debug("OsgiLauncher init, catalog "+defaultCatalogLocation);
            catalogInitialization(new CatalogInitialization(String.format("file:%s", defaultCatalogLocation)));
            startPartOne();
            startupTimer.stop();
            LOG.info("Brooklyn initialisation (part one) complete after {}", startupTimer.toString());
        }
    }

    Thread fallbackThread = null;
    @Override
    public void startOsgi() {
        final Bundle bundle = FrameworkUtil.getBundle(this.getClass());

        Framework f = (Framework) bundle.getBundleContext().getBundle(0);
        int startLevel = f.adapt(FrameworkStartLevel.class).getStartLevel();

        if (areServiceDependenciesReady(bundle, bundle + " bundle activation")) {
            LOG.debug("Starting OSGi catalog/rebind (no service dependencies or all already satisfied, on bundle activation)");
            doStartOsgiAfterBundlesRefreshed();

        } else {
            ServiceListener sl[] = { null };


            sl[0] = new ServiceListener() {
                @Override
                public void serviceChanged(ServiceEvent event) {
                    if (areServiceDependenciesReady(bundle, "ServiceEvent[" + event.getServiceReference() + " / " + event.getType() + " - " + event.getSource() + "]")) {
                        LOG.debug("Starting OSGi catalog/rebind - all service dependencies satisfied");
                        bundle.getBundleContext().removeServiceListener(sl[0]);
                        if (fallbackThread!=null) {
                            fallbackThread.interrupt();
                        }
                        new Thread(() -> {
                            // do this in a new thread as it takes a while, shouldn't run in service listener thread
                            doStartOsgiAfterBundlesRefreshed();
                        }).start();
                    }
                }
            };

            Duration timeout;
            try {
                timeout = Duration.parse((String) getBrooklynProperties().getConfig(BROOKLYN_OSGI_DEPENDENCIES_SERVICES_TIMEOUT));
            } catch (Exception e) {
                throw Exceptions.propagateAnnotated("Invalid duration specified for '"+BROOKLYN_OSGI_DEPENDENCIES_SERVICES_TIMEOUT+"'", e);
            }
            fallbackThread = new Thread(() -> {
                CountdownTimer timer = timeout==null ? null : CountdownTimer.newInstanceStarted(timeout);
                try {
                    if (timeout!=null) {
                        LOG.debug("Service dependencies timeout detected as " + timeout + "; will start catalog/rebind after that delay if service dependencies not fulfilled sooner");
                    } else {
                        LOG.debug("No timeout specified for '"+BROOKLYN_OSGI_DEPENDENCIES_SERVICES_TIMEOUT+"'; will wait indefinitely for service dependencies");
                    }
                    int iteration = 0;
                    do {
                        if (iteration>0) {
                            LOG.debug("Still waiting on service dependencies, " + iteration+"m so far" +
                                    (timer!=null ? ", "+timer.getDurationRemaining()+" remaining until timeout" :  " (will wait indefinitely)"));
                        }
                        Duration wait = Duration.ONE_MINUTE;
                        if (timer != null && timer.getDurationRemaining().isShorterThan(wait)) {
                            wait = timer.getDurationRemaining();
                        }
                        Time.sleep(wait);
                        iteration++;
                    } while (timer==null || timer.isNotExpired());
                } catch (RuntimeInterruptedException e) {
                    // normal - thread aborted probably because service dependencies fulfilled
                    LOG.debug("OsgiLauncher fallback thread interrupted, probably because service dependencies fulfilled in another thread where OSGi catalog/rebind start should be occurring (or due to shutdown)");
                    Thread.interrupted();
                    return;
                } finally {
                    fallbackThread = null;
                }

                LOG.warn("Starting OSGi catalog/rebind due to timeout waiting for service dependencies");
                bundle.getBundleContext().removeServiceListener(sl[0]);
                new Thread(() -> {
                    // do this in a new thread so it isn't interrupted if fallback[0] is interrupted
                    if (!doStartOsgiAfterBundlesRefreshed()) {
                        LOG.debug("Did not start OSGi after timeout; already started");
                    }
                }).start();
            });

            bundle.getBundleContext().addServiceListener(sl[0]);
            fallbackThread.start();
        }
    }

    private boolean areServiceDependenciesReady(Bundle bundle, String context) {
        Object deps = getBrooklynProperties().getConfig(BROOKLYN_OSGI_DEPENDENCIES_SERVICES_FILTERS);
        if (deps!=null) {
            List<String> items = JavaStringEscapes.unwrapJsonishListStringIfPossible(deps.toString());
            LOG.debug("OSGi catalog/rebind service dependency check, on " + context + ": " + items);

            for (String item: items) {
                ServiceReference r1[];
                try {
                    r1 = bundle.getBundleContext().getServiceReferences((String)null, item);
                } catch (Exception e) {
                    throw Exceptions.propagateAnnotated("Error getting service references satisfying '"+item+"'", e);
                }

                if (r1 == null || r1.length == 0) {
                    LOG.debug("OSGi catalog/rebind blocked, service dependency not yet fulfilled (will keep listening for services): '" + item + "'");
                    return false;
                }
            }

            return true;


        } else {
            LOG.debug("No service dependencies specified, on " + context);
            return true;

        }
    }

    AtomicBoolean startedOsgiAfterBundlesRefreshed = new AtomicBoolean();
    private boolean doStartOsgiAfterBundlesRefreshed() {
        if (startedOsgiAfterBundlesRefreshed.getAndSet(true)) {
            LOG.debug("OSGi catalog/rebind already started when invoked a second time", new Throwable("Trace for unexpected redundant OSGi catalog/rebind start"));
            return false;
        }

        doStartOsgi();

        Object newStartLevelS = null;
        try {
            newStartLevelS = getBrooklynProperties().getConfig(BROOKLYN_OSGI_STARTLEVEL_POSTINIT);
            if (newStartLevelS==null || Strings.isBlank(""+newStartLevelS)) {
                LOG.debug("No change required to OSGi start-level after OSGi catalog/rebind ("+BROOKLYN_OSGI_STARTLEVEL_POSTINIT+" unset)");

            } else {
                FrameworkStartLevel fsl = FrameworkUtil.getBundle(this.getClass()).getBundleContext().getBundle(0).adapt(FrameworkStartLevel.class);

                int newStartLevel = TypeCoercions.coerce(newStartLevelS, Integer.class);
                if (fsl.getStartLevel()<newStartLevel) {
                    LOG.debug("Changing OSGi start-level to "+newStartLevelS+" (from "+fsl+") after OSGi catalog/rebind");
                    fsl.setStartLevel(newStartLevel);

                } else {
                    LOG.debug("No change required to OSGi start-level after OSGi catalog/rebind (currently "+fsl.getStartLevel()+", "+BROOKLYN_OSGI_STARTLEVEL_POSTINIT+"="+newStartLevelS+" required)");

                }
            }
        } catch (Exception e) {
            LOG.error("Error handling post-init start level: "+e, e);
        }

        return true;
    }

    private void doStartOsgi() {
        synchronized (reloadLock) {
            final Stopwatch startupTimer = Stopwatch.createStarted();
            LOG.debug("OsgiLauncher catalog/rebind running initialization (part two)");
            startPartTwo();
            startupTimer.stop();
            LOG.info("Brooklyn initialization (part two) complete after {}", startupTimer.toString());
        }
    }

    @Override
    public void destroyOsgi() {
        LOG.debug("Notified of system shutdown, calling shutdown hooks");
        Threads.runShutdownHooks();

        Thread t = fallbackThread;
        if (t!=null) {
            LOG.debug("Notified of system shutdown, cancelling service dependencies fallback thread");
            t.interrupt();
        }
    }

    @Override
    protected void startingUp() {
        super.startingUp();
        ManagementContext managementContext = getManagementContext();
        BrooklynProperties brooklynProperties = (BrooklynProperties) managementContext.getConfig();
        if (BrooklynWebConfig.hasNoSecurityOptions(brooklynProperties)) {
            LOG.info("No security provider options specified. Define a security provider or users to prevent a random password being created and logged.");
            // Deprecated in 0.11.0. Add to release notes and remove in next release.
            brooklynProperties.put(
                    BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE,
                    new BrooklynUserWithRandomPasswordSecurityProvider(managementContext));
            managementContext.getScratchpad().put(
                    BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE,
                    new BrooklynUserWithRandomPasswordSecurityProvider(managementContext));
        }
    }

    public void updateProperties(Map<?, ?> props) {
        synchronized (reloadLock) {
            LOG.info("Updating brooklyn config because of config admin changes.");
            configSupplier.update(props);
            getManagementContext().reloadBrooklynProperties();
        }
    }

    public void setBrooklynVersion(BrooklynVersionService brooklynVersion) {
        this.brooklynVersion = brooklynVersion;
    }

    public void setPersistenceLocation(@Nullable String persistenceLocationSpec) {
        persistenceLocation(persistenceLocationSpec);
    }


    public void setBrooklynProperties(BrooklynProperties brooklynProperties) {
        brooklynProperties(brooklynProperties);
    }

    public void setIgnorePersistenceErrors(boolean ignorePersistenceErrors) {
        ignorePersistenceErrors(ignorePersistenceErrors);
    }

    public void setIgnoreCatalogErrors(boolean ignoreCatalogErrors) {
        ignoreCatalogErrors(ignoreCatalogErrors);
    }

    public void setIgnoreAppErrors(boolean ignoreAppErrors) {
        ignoreAppErrors(ignoreAppErrors);
    }

    public void setPersistMode(PersistMode persistMode) {
        persistMode(persistMode);
    }

    public void setHighAvailabilityMode(HighAvailabilityMode highAvailabilityMode) {
        highAvailabilityMode(highAvailabilityMode);
    }

    public void setPersistenceDir(@Nullable String persistenceDir) {
        persistenceDir(persistenceDir);
    }

    public void setPersistPeriod(String persistPeriod) {
        persistPeriod(Duration.parse(persistPeriod));
    }

    public void setHaHeartbeatTimeout(String val) {
        haHeartbeatTimeout(Duration.parse(val));
    }

    public void setStartBrooklynNode(boolean val) {
        startBrooklynNode(val);
    }

    public void setHaHeartbeatPeriod(String val) {
        haHeartbeatPeriod(Duration.parse(val));
    }

    public void setCopyPersistedState(String destinationDir) {
        copyPersistedState(destinationDir);
    }

    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = configAdmin;
    }

    public void setGlobalBrooklynProperties(String globalBrooklynProperties) {
        this.globalBrooklynProperties = globalBrooklynProperties;
    }

    public void setLocalBrooklynProperties(String localBrooklynProperties) {
        this.localBrooklynProperties = localBrooklynProperties;
    }

    public void setDefaultCatalogLocation(String defaultCatalogLocation) {
        this.defaultCatalogLocation = defaultCatalogLocation;
    }
}
