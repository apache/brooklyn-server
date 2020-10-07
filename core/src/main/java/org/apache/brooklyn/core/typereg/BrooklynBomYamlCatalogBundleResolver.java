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

import java.io.*;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrooklynBomYamlCatalogBundleResolver extends AbstractCatalogBundleResolver {

    private static Logger LOG = LoggerFactory.getLogger(BrooklynBomYamlCatalogBundleResolver.class);

    public static final String FORMAT = "brooklyn-bom-yaml";

    public BrooklynBomYamlCatalogBundleResolver() {
        super(FORMAT, "Brooklyn catalog.bom YAML", "YAML map with a single brooklyn.catalog key");
    }

    // TODO

    @Override
    protected double scoreForNullFormat(InputStream f) {
        return 0;
    }

    @Override
    public ReferenceWithError<OsgiBundleInstallationResult> install(InputStream input, BundleInstallationOptions options) {
        String yaml = Streams.readFullyString(input);
        Map<?, ?> cm = BasicBrooklynCatalog.getCatalogMetadata(yaml);

        if (cm == null) {
            throw new IllegalStateException("No catalog meta data supplied. brooklyn.catalog must be specified");
        }

        VersionedName vn = BasicBrooklynCatalog.getVersionedName(cm, false);
        if (vn == null) {
            // for better legacy compatibiity, if id specified at root use that
            String id = (String) cm.get("id");
            if (Strings.isNonBlank(id)) {
                vn = VersionedName.fromString(id);
            }
            vn = new VersionedName(vn != null && Strings.isNonBlank(vn.getSymbolicName()) ? vn.getSymbolicName() : "brooklyn-catalog-bom-" + Identifiers.makeRandomId(8),
                    vn != null && vn.getVersionString() != null ? vn.getVersionString() : ConfigUtils.getFirstAs(cm, String.class, "version").or(BasicBrooklynCatalog.NO_VERSION));
        }
        LOG.debug("Wrapping supplied BOM as " + vn);
        Manifest mf = new Manifest();
        mf.getMainAttributes().putValue(Constants.BUNDLE_SYMBOLICNAME, vn.getSymbolicName());
        mf.getMainAttributes().putValue(Constants.BUNDLE_VERSION, vn.getOsgiVersionString());
        mf.getMainAttributes().putValue(Constants.BUNDLE_MANIFESTVERSION, "2");
        mf.getMainAttributes().putValue(Attributes.Name.MANIFEST_VERSION.toString(), BasicBrooklynCatalog.OSGI_MANIFEST_VERSION_VALUE);
        mf.getMainAttributes().putValue(BasicBrooklynCatalog.BROOKLYN_WRAPPED_BOM_BUNDLE, Boolean.TRUE.toString());

        BundleMaker bm = new BundleMaker(mgmt);
        File bf = bm.createTempBundle(vn.getSymbolicName(), mf, MutableMap.of(
                new ZipEntry(BasicBrooklynCatalog.CATALOG_BOM), (InputStream) new ByteArrayInputStream(yaml.getBytes())));

        OsgiBundleInstallationResult result;
        try {
            result = ((ManagementContextInternal)mgmt).getOsgiManager().get().installBrooklynBomBundle(
                    new BasicManagedBundle(vn.getSymbolicName(), vn.getVersionString(), null, null), new FileInputStream(bf), true, true, options.forceUpdateOfNonSnapshots).get();
        } catch (FileNotFoundException e) {
            throw Exceptions.propagate(e);
        } finally {
            bf.delete();
        }
        if (result.getCode().isError()) {
            // rollback done by install call above
            throw new IllegalStateException(result.getMessage());
        }
        ((BasicBrooklynCatalog)mgmt.getCatalog()).uninstallEmptyWrapperBundles();
        return ReferenceWithError.newInstanceWithoutError(result);
    }
}
