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
package org.apache.brooklyn.core;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.rt.felix.ManifestHelper;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.OsgiUtil;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.VersionComparator;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Wraps the version of Brooklyn.
 * <p>
 * Also retrieves the SHA-1 from any OSGi bundle, and checks that the maven and osgi versions match.
 */
public class BrooklynVersion implements BrooklynVersionService {

    private static final Logger log = LoggerFactory.getLogger(BrooklynVersion.class);

    private static final String MVN_VERSION_RESOURCE_FILE = "META-INF/maven/org.apache.brooklyn/brooklyn-core/pom.properties";
    private static final String MANIFEST_PATH = "META-INF/MANIFEST.MF";
    private static final String BROOKLYN_CORE_SYMBOLIC_NAME = "org.apache.brooklyn.core";

    private static final String MVN_VERSION_PROPERTY_NAME = "version";
    private static final String OSGI_VERSION_PROPERTY_NAME = Attributes.Name.IMPLEMENTATION_VERSION.toString();
    private static final String OSGI_SHA1_PROPERTY_NAME = "Implementation-SHA-1";
    // may be useful:
//    private static final String OSGI_BRANCH_PROPERTY_NAME = "Implementation-Branch";

    private final static String VERSION_FROM_STATIC = "1.1.0"; // BROOKLYN_VERSION
    private static final AtomicReference<Boolean> IS_DEV_ENV = new AtomicReference<Boolean>();

    private static final String BROOKLYN_FEATURE_PREFIX = "Brooklyn-Feature-";

    public static final BrooklynVersion INSTANCE = new BrooklynVersion();

    private final Properties versionProperties = new Properties();

    private BrooklynVersion() {
        // we read the maven pom metadata and osgi metadata and make sure it's sensible
        // everything is put into a single map for now (good enough, but should be cleaned up)
        readPropertiesFromMavenResource(BrooklynVersion.class.getClassLoader());
        readPropertiesFromOsgiResource();
        // TODO there is also build-metadata.properties used in ServerResource /v1/server/version endpoint
        // see comments on that about folding it into this class instead

        checkVersions();
    }

    @Override
    public void checkVersions() {
        String mvnVersion = getVersionFromMavenProperties();
        if (mvnVersion != null && !VERSION_FROM_STATIC.equals(mvnVersion)) {
            throw new IllegalStateException("Version error: maven " + mvnVersion + " / code " + VERSION_FROM_STATIC);
        }

        String osgiVersion = versionProperties.getProperty(OSGI_VERSION_PROPERTY_NAME);
        // TODO does the OSGi version include other slightly differ gubbins/style ?
        if (osgiVersion != null && !VERSION_FROM_STATIC.equals(osgiVersion)) {
            throw new IllegalStateException("Version error: osgi " + osgiVersion + " / code " + VERSION_FROM_STATIC);
        }
    }

    @Override
    @Nullable
    public String getVersionFromMavenProperties() {
        return versionProperties.getProperty(MVN_VERSION_PROPERTY_NAME);
    }

    @Override
    @Nullable
    public String getVersionFromOsgiManifest() {
        return versionProperties.getProperty(OSGI_VERSION_PROPERTY_NAME);
    }

    @Override
    @Nullable
    /** SHA1 of the last commit to brooklyn at the time this build was made.
     * For SNAPSHOT builds of course there may have been further non-committed changes. */
    public String getSha1FromOsgiManifest() {
        return versionProperties.getProperty(OSGI_SHA1_PROPERTY_NAME);
    }

    @Override
    public String getVersion() {
        return VERSION_FROM_STATIC;
    }

    @Override
    public boolean isSnapshot() {
        return VersionComparator.isSnapshot(getVersion());
    }

    private void readPropertiesFromMavenResource(ClassLoader resourceLoader) {
        InputStream versionStream = null;
        try {
            versionStream = resourceLoader.getResourceAsStream(MVN_VERSION_RESOURCE_FILE);
            if (versionStream == null) {
                if (isDevelopmentEnvironment()) {
                    // allowed for dev env
                    log.trace("No maven resource file " + MVN_VERSION_RESOURCE_FILE + " available");
                } else {
                    log.warn("No maven resource file " + MVN_VERSION_RESOURCE_FILE + " available");
                }
                return;
            }
            versionProperties.load(checkNotNull(versionStream));
        } catch (IOException e) {
            log.warn("Error reading maven resource file " + MVN_VERSION_RESOURCE_FILE + ": " + e, e);
        } finally {
            Streams.closeQuietly(versionStream);
        }
    }

    /**
     * Reads main attributes properties from brooklyn-core's bundle manifest.
     */
    private void readPropertiesFromOsgiResource() {
        if (OsgiUtil.isBrooklynInsideFramework()) {
            Dictionary<String, String> headers = FrameworkUtil.getBundle(BrooklynVersion.class).getHeaders();
            for (Enumeration<String> keys = headers.keys(); keys.hasMoreElements();) {
                String key = keys.nextElement();
                versionProperties.put(key, headers.get(key));
            }
        } else {
            Enumeration<URL> paths;
            try {
                paths = BrooklynVersion.class.getClassLoader().getResources(MANIFEST_PATH);
            } catch (IOException e) {
                // shouldn't happen
                throw Exceptions.propagate(e);
            }
            while (paths.hasMoreElements()) {
                URL u = paths.nextElement();
                InputStream us = null;
                try {
                    us = u.openStream();
                    ManifestHelper mh = ManifestHelper.forManifest(us);
                    if (BROOKLYN_CORE_SYMBOLIC_NAME.equals(mh.getSymbolicName())) {
                        Attributes attrs = mh.getManifest().getMainAttributes();
                        for (Object key : attrs.keySet()) {
                            // key is an Attribute.Name; toString converts to string
                            versionProperties.put(key.toString(), attrs.getValue(key.toString()));
                        }
                        return;
                    }
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    log.debug("Skipping unsupported OSGi manifest in " + u + " when determining Brooklyn version properties: " + e);
                } finally {
                    Streams.closeQuietly(us);
                }
            }
            if (isDevelopmentEnvironment()) {
                // allowed for dev env
                log.trace("No OSGi manifest available to determine version properties");
            } else {
                log.warn("No OSGi manifest available to determine version properties");
            }
        }
    }

    /**
     * Returns whether this is a Brooklyn dev environment,
     * specifically core/target/classes/ is on the classpath for the org.apache.brooklyn.core project.
     * <p/>
     * In a packaged or library build of Brooklyn (normal usage) this should return false,
     * and all OSGi components should be available.
     * <p/>
     * There is no longer any way to force this,
     * such as the old BrooklynDevelopmentMode class;
     * but that could easily be added if required (eg as a system property).
     */
    public static boolean isDevelopmentEnvironment() {
        Boolean isDevEnv = IS_DEV_ENV.get();
        if (isDevEnv != null) return isDevEnv;
        synchronized (IS_DEV_ENV) {
            isDevEnv = computeIsDevelopmentEnvironment();
            IS_DEV_ENV.set(isDevEnv);
            return isDevEnv;
        }
    }

    private static boolean computeIsDevelopmentEnvironment() {
        Enumeration<URL> paths;
        try {
            paths = BrooklynVersion.class.getClassLoader().getResources("org/apache/brooklyn/core/BrooklynVersion.class");
        } catch (IOException e) {
            // shouldn't happen
            throw Exceptions.propagate(e);
        }
        while (paths.hasMoreElements()) {
            URL u = paths.nextElement();
            // running fram a classes directory (including coverage-classes for cobertura) triggers dev env
            if (u.getPath().endsWith("org/apache/brooklyn/core/BrooklynVersion.class") && "file".equals(u.getProtocol()) && !u.toString().contains("!")) {
                try {
                    log.debug("Brooklyn dev/src environment detected: BrooklynVersion class is at: " + u);
                    return true;
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    log.warn("Error reading manifest to determine whether this is a development environment: " + e, e);
                }
            }
        }
        return false;
    }

    @Override
    public void logSummary() {
        log.debug("Brooklyn version " + getVersion() + " (git SHA1 " + getSha1FromOsgiManifest() + ")");
    }

    public static String get() {
        return INSTANCE.getVersion();
    }

    public static BrooklynVersion getInstance() {
        return INSTANCE;
    }

    /**
     * @param mgmt The context to search for features.
     * @return An iterable containing all features found in the management context's classpath and catalogue.
     */
    public static Iterable<BrooklynFeature> getFeatures(ManagementContext mgmt) {
        if (OsgiUtil.isBrooklynInsideFramework()) {
            List<Bundle> bundles = Lists.newArrayList(
                    FrameworkUtil.getBundle(BrooklynVersion.class)
                            .getBundleContext()
                            .getBundles()
            );

            Maybe<OsgiManager> osgi = ((ManagementContextInternal)mgmt).getOsgiManager();
            if (osgi.isPresentAndNonNull()) {
                for (ManagedBundle b: osgi.get().getManagedBundles().values()) {
                    Maybe<Bundle> osgiBundle = osgi.get().findBundle(b);
                    if (osgiBundle.isPresentAndNonNull()) {
                        bundles.add(osgiBundle.get());
                    }                    
                }
                // TODO remove when everything uses osgi bundles tracked by brooklyn above
                for (RegisteredType t: mgmt.getTypeRegistry().getAll()) {
                    for (OsgiBundleWithUrl catalogBundle : t.getLibraries()) {
                        Maybe<Bundle> osgiBundle = osgi.get().findBundle(catalogBundle);
                        if (osgiBundle.isPresentAndNonNull()) {
                            bundles.add(osgiBundle.get());
                        }
                    }
                }
            }

            // Set over list in case a bundle is reported more than once (e.g. from classpath and from OSGi).
            // Not sure of validity of this approach over just reporting duplicates.
            ImmutableSet.Builder<BrooklynFeature> features = ImmutableSet.builder();
            for(Bundle bundle : bundles) {
                Optional<BrooklynFeature> fs = BrooklynFeature.newFeature(bundle.getHeaders());
                if (fs.isPresent()) {
                    features.add(fs.get());
                }
            }
            return features.build();
        } else {
            Set<URL> manifests = MutableSet.copyOf(ResourceUtils.create(mgmt).getResources(MANIFEST_PATH));

            Maybe<OsgiManager> osgi = ((ManagementContextInternal)mgmt).getOsgiManager();
            if (osgi.isPresentAndNonNull()) {
                // get manifests for all bundles installed
                Iterables.addAll(manifests, 
                    osgi.get().getResources(MANIFEST_PATH, osgi.get().getManagedBundles().values()) );
            }
            
            // TODO remove when everything uses osgi bundles tracked by brooklyn above
            for (RegisteredType t: mgmt.getTypeRegistry().getAll()) {
                OsgiBrooklynClassLoadingContext osgiContext = new OsgiBrooklynClassLoadingContext(mgmt, t.getId(), t.getLibraries());
                Iterables.addAll(manifests, osgiContext.getResources(MANIFEST_PATH));
            }
            
            // Set over list in case a bundle is reported more than once (e.g. from classpath and from OSGi).
            // Not sure of validity of this approach over just reporting duplicates.
            ImmutableSet.Builder<BrooklynFeature> features = ImmutableSet.builder();
            for (URL manifest : manifests) {
                ManifestHelper mh = null;
                try {
                    mh = ManifestHelper.forManifest(manifest);
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    log.debug("Error reading OSGi manifest from " + manifest + " when determining version properties: " + e, e);
                }
                if (mh == null) continue;
                Attributes attrs = mh.getManifest().getMainAttributes();
                Optional<BrooklynFeature> fs = BrooklynFeature.newFeature(attrs);
                if (fs.isPresent()) {
                    features.add(fs.get());
                }
            }
            return features.build();
        }
    }

    public static class BrooklynFeature {
        private final String name;
        private final String symbolicName;
        private final String version;
        private final String lastModified;
        private final Map<String, String> additionalData;

        BrooklynFeature(String name, String symbolicName, String version, String lastModified, Map<String, String> additionalData) {
            this.symbolicName = checkNotNull(symbolicName, "symbolicName");
            this.name = name;
            this.version = version;
            this.lastModified = lastModified;
            this.additionalData = ImmutableMap.copyOf(additionalData);
        }

        private static Optional<BrooklynFeature> newFeature(Attributes attrs) {
            // unfortunately Attributes is a Map<Object,Object>
            Dictionary<String,String> headers = new Hashtable<>();
            for (Map.Entry<Object, Object> entry : attrs.entrySet()) {
                headers.put(entry.getKey().toString(), entry.getValue().toString());
            }
            return newFeature(headers);
        }

        private static void ifHeaderAddKey(Dictionary<String, String> headers, String header, Map<String, String> additionalData, String key) {
            if (!additionalData.containsKey(key)) {
                String v = headers.get(header);
                if (v!=null && !v.toLowerCase().equals("unknown")) {
                    additionalData.put(key, headers.get(header));
                }
            }
        }

        /** @return Present if any attribute name begins with {@link #BROOKLYN_FEATURE_PREFIX}, absent otherwise. */
        private static Optional<BrooklynFeature> newFeature(Dictionary<String,String> headers) {
            Map<String, String> additionalData = Maps.newHashMap();
            for (Enumeration<String> keys = headers.keys(); keys.hasMoreElements();) {
                String key = keys.nextElement();
                if (key.startsWith(BROOKLYN_FEATURE_PREFIX)) {
                    String value = headers.get(key);
                    if (!Strings.isBlank(value)) {
                        additionalData.put(key, value);
                    }
                }
            }
            if (additionalData.isEmpty()) {
                return Optional.absent();
            }

            // Name is special cased as it a useful way to indicate a feature without
            String nameKey = BROOKLYN_FEATURE_PREFIX + "Name";
            String name = Optional.fromNullable(additionalData.remove(nameKey))
                    .or(Optional.fromNullable(Constants.BUNDLE_NAME))
                    .or(headers.get(Constants.BUNDLE_SYMBOLICNAME));

            ifHeaderAddKey(headers, "Implementation-SHA-1", additionalData, "buildSha1");
            ifHeaderAddKey(headers, "Implementation-Branch", additionalData, "buildBranch");

            return Optional.of(new BrooklynFeature(
                    name,
                    headers.get(Constants.BUNDLE_SYMBOLICNAME),
                    headers.get(Constants.BUNDLE_VERSION),
                    headers.get("Bnd-LastModified"),
                    additionalData));
        }

        public String getLastModified() {
            return lastModified;
        }

        public String getName() {
            return name;
        }

        public String getSymbolicName() {
            return symbolicName;
        }

        public String getVersion() {
            return version;
        }

        /** @return an unmodifiable map */
        public Map<String, String> getAdditionalData() {
            return additionalData;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + symbolicName + (version != null ? ":" + version : "") + "}";
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(symbolicName, version);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            BrooklynFeature that = (BrooklynFeature) other;
            if (!symbolicName.equals(that.symbolicName)) {
                return false;
            } else if (version != null ? !version.equals(that.version) : that.version != null) {
                return false;
            }
            return true;
        }
    }

    public static String getOsgiVersion() {
        return BrooklynVersionSyntax.toValidOsgiVersion(get());
    }
}
