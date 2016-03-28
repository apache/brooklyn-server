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

import javax.annotation.Nullable;

import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.BrooklynVersionService;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.launcher.common.BasicLauncher;
import org.apache.brooklyn.util.javalang.Threads;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializer for brooklyn-core when running in an OSGi environment.
 *
 * Temporarily here; should be totally contained in blueprint beans' init-methods.
 */
public class OsgiLauncher extends BasicLauncher<OsgiLauncher> {
    private static final Logger log = LoggerFactory.getLogger(OsgiLauncher.class);

    private BrooklynVersionService brooklynVersion;

    @Override
    public OsgiLauncher start() {
        // make sure brooklyn-core bundle is started
        brooklynVersion.getVersion();

        return super.start();
    }

    // Called by blueprint container
    // init-method can't find the start method for some reason, provide an alternative
    public void init() {
        start();
    }

    // Called by blueprint container
    public void destroy() {
        log.debug("Notified of system shutdown, calling shutdown hooks");
        Threads.runShutdownHooks();
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

}
