/*
 * Copyright 2015 The Apache Software Foundation.
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
package org.apache.brooklyn.util.core.osgi;

import java.io.File;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.io.FileUtil;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 *
 * @author Ciprian Ciubotariu <cheepeero@gmx.net>
 */
public class OsgiTestBase {

    private static final Logger log = LoggerFactory.getLogger(OsgiTestBase.class);

    public static final String BROOKLYN_OSGI_TEST_A_0_1_0_PATH = OsgiTestResources.BROOKLYN_OSGI_TEST_A_0_1_0_PATH;
    public static final String BROOKLYN_OSGI_TEST_A_0_1_0_URL = "classpath:"+BROOKLYN_OSGI_TEST_A_0_1_0_PATH;

    protected Bundle install(String url) {
        try {
            return Osgis.install(framework, url);
        } catch (Exception e) {
            throw new IllegalStateException("test resources not available; may be an IDE issue, so try a mvn rebuild of this project", e);
        }
    }

    protected Bundle installFromClasspath(String resourceName) {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), resourceName);
        try {
            return Osgis.install(framework, String.format("classpath:%s", resourceName));
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    protected Framework framework = null;
    private File storageTempDir;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        storageTempDir = Os.newTempDir("osgi-standalone");
        framework = Osgis.getFramework(storageTempDir.getAbsolutePath(), true);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        tearDownOsgiFramework(framework, storageTempDir);
    }

    public static void tearDownOsgiFramework(Framework framework, File storageTempDir) {
        Osgis.ungetFramework(framework);
        FileUtil.deleteDirectory(storageTempDir);
    }

    public static void preinstallLibrariesLowLevelToPreventCatalogBomParsing(ManagementContext mgmt, String ...libraries) {
        // catalog BOM CAMP syntax not available in core; need to pre-install
        // to prevent Brooklyn from installing BOMs in those libraries
        for (String lib: libraries) {
            // install libs manually to prevent catalog BOM loading
            // (could do OsgiManager.installDeferredStart also, then just ignore the start)
            try {
                Osgis.install(((ManagementContextInternal)mgmt).getOsgiManager().get().getFramework(), lib);
            } catch (BundleException e) {
                throw Exceptions.propagate(e);
            }
        }
    }

}
