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
import java.io.IOException;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.apache.commons.io.FileUtils;
import org.osgi.framework.BundleException;
import org.osgi.framework.launch.Framework;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 *
 * @author Ciprian Ciubotariu <cheepeero@gmx.net>
 */
public class OsgiTestBase {

    protected Framework framework = null;
    private File storageTempDir;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        storageTempDir = Os.newTempDir("osgi-standalone");
        framework = Osgis.getFramework(storageTempDir.getAbsolutePath(), true);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws BundleException, IOException, InterruptedException {
        tearDownOsgiFramework(framework, storageTempDir);
    }

    public static void tearDownOsgiFramework(Framework framework, File storageTempDir) throws BundleException, InterruptedException, IOException {
        Osgis.ungetFramework(framework);
        framework = null;
        if (storageTempDir != null) {
            FileUtils.deleteDirectory(storageTempDir);
            storageTempDir = null;
        }
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
