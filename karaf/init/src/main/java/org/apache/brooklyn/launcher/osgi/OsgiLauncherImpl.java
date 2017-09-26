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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.BrooklynVersionService;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.BrooklynShutdownHooks;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.launcher.common.BasicLauncher;
import org.apache.brooklyn.launcher.common.BrooklynPropertiesFactoryHelper;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.BrooklynUserWithRandomPasswordSecurityProvider;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Threads;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
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

        Configuration brooklynConfig = getConfiguration(configAdmin, BROOKLYN_CONFIG_PID);
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

    private Configuration getConfiguration(ConfigurationAdmin configAdmin, String brooklynConfigPid) {
        String filter = '(' + Constants.SERVICE_PID + '=' + brooklynConfigPid + ')';
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

    @Override
    public void startOsgi() {
        synchronized (reloadLock) {
            final Stopwatch startupTimer = Stopwatch.createStarted();
            LOG.debug("OsgiLauncher start");
            startPartTwo();
            startupTimer.stop();
            LOG.info("Brooklyn initialisation (part two) complete after {}", startupTimer.toString());
        }
    }

    @Override
    public void destroyOsgi() {
        LOG.debug("Notified of system shutdown, calling shutdown hooks");
        Threads.runShutdownHooks();
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
