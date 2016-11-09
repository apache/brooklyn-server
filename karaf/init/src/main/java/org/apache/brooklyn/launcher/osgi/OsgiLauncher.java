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

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.BrooklynVersionService;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.internal.BrooklynProperties;
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

/**
 * Initializer for brooklyn-core when running in an OSGi environment.
 */
public class OsgiLauncher extends BasicLauncher<OsgiLauncher> {

    private static final Logger LOG = LoggerFactory.getLogger(OsgiLauncher.class);
    private static final String DEFAULT_CATALOG_BOM = "file:etc/default.catalog.bom";
    public static final String BROOKLYN_CONFIG_PID = "brooklyn";
    
    private Object reloadLock = new Object();

    private BrooklynVersionService brooklynVersion;

    private String globalBrooklynProperties;
    private String localBrooklynProperties;

    private Integer port;

    private ConfigurationAdmin configAdmin;
    private ConfigSupplier configSupplier;


    @Override
    public OsgiLauncher start() {
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
        return super.start();
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

    // Called by blueprint container
    // init-method can't find the start method for some reason, provide an alternative
    public void init() {
        synchronized (reloadLock) {
            LOG.debug("OsgiLauncher init");
            catalogInitialization(new CatalogInitialization(DEFAULT_CATALOG_BOM, false, null, false));
            start();
        }
    }

    // Called by blueprint container
    public void destroy() {
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
            brooklynProperties.put(
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


    public void setBrooklynProperties(BrooklynProperties brooklynProperties){
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

    public void setPort(Integer port) {
        this.port = port;
    }

}
