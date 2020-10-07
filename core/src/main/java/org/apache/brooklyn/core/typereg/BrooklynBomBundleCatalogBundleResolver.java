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
package org.apache.brooklyn.core.typereg;

import java.io.InputStream;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.mgmt.ha.BrooklynBomOsgiArchiveInstaller;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrooklynBomBundleCatalogBundleResolver extends AbstractCatalogBundleResolver {

    private static Logger LOG = LoggerFactory.getLogger(BrooklynBomBundleCatalogBundleResolver.class);

    public static final String FORMAT = "brooklyn-bom-bundle";

    public BrooklynBomBundleCatalogBundleResolver() {
        super(FORMAT, "Brooklyn catalog.bom ZIP", "ZIP with a catalog.bom and/or an OSGi manifest " +
                "(if just an OSGi manifest, types will not be added but the bundle will be installed)");
    }

    @Override
    public BrooklynBomBundleCatalogBundleResolver withManagementContext(ManagementContext mgmt) {
        return (BrooklynBomBundleCatalogBundleResolver) super.withManagementContext(mgmt);
    }

    // TODO

    @Override
    protected double scoreForNullFormat(InputStream f) {
        return 0;
    }

    @Override
    public ReferenceWithError<OsgiBundleInstallationResult> install(InputStream input, BundleInstallationOptions options) {
        LOG.debug("Installing bundle from stream - known details: "+options.knownBundleMetadata);

        BrooklynBomOsgiArchiveInstaller installer = new BrooklynBomOsgiArchiveInstaller(
                ((ManagementContextInternal)mgmt).getOsgiManager().get(),
                options.knownBundleMetadata, input);
        installer.setStart(options.start);
        installer.setCatalogBomText(FORMAT, null);
        installer.setLoadCatalogBom(options.loadCatalogBom);
        installer.setForce(options.forceUpdateOfNonSnapshots);

        installer.setDeferredStart(options.deferredStart);
        installer.setValidateTypes(options.validateTypes);

        return installer.install();
    }

}
