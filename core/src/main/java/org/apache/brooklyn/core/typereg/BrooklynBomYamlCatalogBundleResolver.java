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
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.InputStreamSource;
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

    @Override
    protected double scoreForNullFormat(Supplier<InputStream> f) {
        try (FileTypeDetector detector = new FileTypeDetector(f)) {
            // positive but very small, so we can get errors
            if (!detector.isPrintableText()) return 0.001;
            if (!detector.isYaml()) return 0.01;
            Object yaml = detector.getYaml().get();
            if (yaml instanceof Iterable) {
                Iterator yi = ((Iterable) yaml).iterator();
                if (!yi.hasNext()) return 0;
                Object yo = yi.next();
                if (yi.hasNext()) {
                    // multiple documents
                    return 0.01;
                }
                if (yo instanceof Map) {
                    if (((Map<?, ?>) yo).containsKey("brooklyn.catalog")) return 0.9;
                    return 0.1;
                } else {
                    return 0.01;
                }
            }
            // expected an iterable
            return 0.01;
        }
    }

    @Override
    public ReferenceWithError<OsgiBundleInstallationResult> install(Supplier<InputStream> input, BundleInstallationOptions options) {
        try (FileTypeDetector detector = new FileTypeDetector(input)) {
            // throw if not valid yaml
            detector.getYaml().get();
        }

        if (options==null) options = new BundleInstallationOptions();

        String yaml = Streams.readFullyString(input.get());
        Map<?, ?> cm = BasicBrooklynCatalog.getCatalogMetadata(yaml);

        if (cm == null) {
            throw new IllegalStateException("No catalog meta data supplied. brooklyn.catalog must be specified");
        }

        VersionedName vn = BasicBrooklynCatalog.getVersionedName(cm, false);
        if (vn == null) {
            vn = new VersionedName("brooklyn-catalog-bom-" + Identifiers.makeRandomId(8), (String)null);
        }
        if (Strings.isBlank(vn.getVersionString())) {
            vn = new VersionedName(vn.getSymbolicName(),
                    ConfigUtils.getFirstAs(cm, String.class, "version").or(BasicBrooklynCatalog.NO_VERSION));
        }
        LOG.debug("Wrapping supplied BOM as " + vn);
        Manifest mf = new Manifest();
        mf.getMainAttributes().putValue(Constants.BUNDLE_SYMBOLICNAME, vn.getSymbolicName());
        mf.getMainAttributes().putValue(Constants.BUNDLE_VERSION, vn.getOsgiVersionString());
        mf.getMainAttributes().putValue(Constants.BUNDLE_MANIFESTVERSION, "2");
        mf.getMainAttributes().putValue(Attributes.Name.MANIFEST_VERSION.toString(), BasicBrooklynCatalog.OSGI_MANIFEST_VERSION_VALUE);
        mf.getMainAttributes().putValue(BasicBrooklynCatalog.BROOKLYN_WRAPPED_BOM_BUNDLE, Boolean.TRUE.toString());

        Object impliedHeaders = cm.get(BasicBrooklynCatalog.CATALOG_OSGI_WRAP_HEADERS);
        if (impliedHeaders instanceof Map) {
            ((Map<?, ?>) impliedHeaders).forEach((k,v)->{
                mf.getMainAttributes().putValue(Strings.toString(k), Strings.toString(v));
            });
        } else if (impliedHeaders!=null) {
            throw new IllegalStateException("Must contain map of OSGi headers to insert in "+BasicBrooklynCatalog.CATALOG_OSGI_WRAP_HEADERS);
        }

        BundleMaker bm = new BundleMaker(mgmt);
        File bf = bm.createTempBundle(vn.getSymbolicName(), mf, MutableMap.of(
                new ZipEntry(BasicBrooklynCatalog.CATALOG_BOM), (InputStream) new ByteArrayInputStream(yaml.getBytes())));

        OsgiBundleInstallationResult result;
        try {

            BasicManagedBundle basicManagedBundle = new BasicManagedBundle(vn.getSymbolicName(), vn.getVersionString(),
                    null, BrooklynBomBundleCatalogBundleResolver.FORMAT,
                    null, null, options.getDeleteable());
            // if the submitted blueprint contains tags, we set them on the bundle, so they can be picked up and used to tag the plan.
            if( cm.containsKey("tags") && cm.get("tags") instanceof Iterable) {
                basicManagedBundle.tags().addTags((Iterable<?>)cm.get("tags"));
            }
            // Store the bundleIconUrl as an ICON_URL tag
            Maybe<String> bundleIconUrl = ConfigUtils.getFirstAs(cm, String.class, "bundleIconUrl");
            if (bundleIconUrl.isPresentAndNonNull()) {
                basicManagedBundle.tags().addTag(BrooklynTags.newIconUrlTag(bundleIconUrl.get()));
            }
            result = ((ManagementContextInternal)mgmt).getOsgiManager().get().installBrooklynBomBundle(
                    basicManagedBundle, InputStreamSource.of("ZIP generated for "+vn+": "+bf, bf), options.isStart(), options.isLoadCatalogBom(), options.isForceUpdateOfNonSnapshots(),
                    options.isValidateTypes(), options.isDeferredStart()).get();
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
