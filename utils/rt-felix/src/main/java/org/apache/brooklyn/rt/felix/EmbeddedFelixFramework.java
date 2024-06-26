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
package org.apache.brooklyn.rt.felix;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.reflect.ClassPath;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.osgi.OsgiUtils;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.felix.framework.FrameworkFactory;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functions for starting an Apache Felix OSGi framework inside a non-OSGi Brooklyn distro.
 * 
 * @author Ciprian Ciubotariu <cheepeero@gmx.net>
 */
public class EmbeddedFelixFramework {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedFelixFramework.class);

    private static final String EXTENSION_PROTOCOL = "system";
    private static final String MANIFEST_PATH = "META-INF/MANIFEST.MF";
    private static final Set<String> SYSTEM_BUNDLES = MutableSet.of();

    // set here to avoid importing core, only needed for tests
    private static final String BROOKLYN_VERSION = "1.2.0-SNAPSHOT";
    private static final String BROOKLYN_VERSION_OSGI_ROUGH = BROOKLYN_VERSION.replaceFirst("-.*", "");

    private static final Set<URL> CANDIDATE_BOOT_BUNDLES;
    
    static {
        try {
            CANDIDATE_BOOT_BUNDLES = MutableSet.copyOf(Collections.list(
                EmbeddedFelixFramework.class.getClassLoader().getResources(MANIFEST_PATH))).asUnmodifiable();
        } catch (Exception e) {
            // should never happen; weird classloading problem
            throw Exceptions.propagate(e);
        }
    }

    // -------- creating

    /*
     * loading framework factory and starting framework based on:
     * http://felix.apache.org/documentation/subprojects/apache-felix-framework/apache-felix-framework-launching-and-embedding.html
     */

    public static FrameworkFactory newFrameworkFactory() {
        URL url = EmbeddedFelixFramework.class.getClassLoader().getResource(
                "META-INF/services/org.osgi.framework.launch.FrameworkFactory");
        if (url != null) {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
                try {
                    for (String s = br.readLine(); s != null; s = br.readLine()) {
                        s = s.trim();
                        // load the first non-empty, non-commented line
                        if ((s.length() > 0) && (s.charAt(0) != '#')) {
                            return (FrameworkFactory) Class.forName(s).newInstance();
                        }
                    }
                } finally {
                    if (br != null) br.close();
                }
            } catch (Exception e) {
                // class creation exceptions are not interesting to caller...
                throw Exceptions.propagate(e);
            }
        }
        throw new IllegalStateException("Could not find framework factory.");
    }

    public static Framework newFrameworkStarted(String felixCacheDir, boolean clean, Map<?,?> extraStartupConfig) {
        Map<Object,Object> cfg = MutableMap.copyOf(extraStartupConfig);
        if (clean) cfg.put(Constants.FRAMEWORK_STORAGE_CLEAN, "onFirstInit");
        if (felixCacheDir!=null) cfg.put(Constants.FRAMEWORK_STORAGE, felixCacheDir);
        cfg.put(Constants.FRAMEWORK_BSNVERSION, Constants.FRAMEWORK_BSNVERSION_MULTIPLE);

        if (CANDIDATE_BOOT_BUNDLES.stream().noneMatch(url -> url.toString().contains("brooklyn-core"))) {
            // if not running brooklyn-core from a jar, for osgi deps to work we need to make the system bundle export brooklyn packages;
            // mainly for tests to work; everything else should be running from jars with manifest.mf

            try {
                // spring has: PathMatchingResourcePatternResolver; we use guava's ClassPath
                Set<String> brooklynPackages = ClassPath.from(EmbeddedFelixFramework.class.getClassLoader()).getTopLevelClasses()
                                .stream().map(c -> c.getPackageName())
                                .filter(n -> n.startsWith("org.apache.brooklyn.")).collect(Collectors.toSet());
                LOG.info("Embedded felix OSGi system running without brooklyn-core JAR; manually adding brooklyn packages ("+brooklynPackages.size()+") to system bundle exports");
                cfg.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, brooklynPackages.stream().map(p -> p + ";version=\""+BROOKLYN_VERSION_OSGI_ROUGH+"\"").collect(Collectors.joining(",")));
            } catch (Exception e) {
                throw Exceptions.propagateAnnotated("Unable to set up embedded felix framework with packages inferred", e);
            }
        }

        FrameworkFactory factory = newFrameworkFactory();

        Stopwatch timer = Stopwatch.createStarted();
        Framework framework = factory.newFramework(cfg);
        try {
            framework.init();
            installBootBundles(framework);
            framework.start();
        } catch (Exception e) {
            // framework bundle start exceptions are not interesting to caller...
            throw Exceptions.propagate(e);
        }
        LOG.debug("System bundles are: "+SYSTEM_BUNDLES);
        LOG.debug("OSGi framework started in " + Duration.of(timer));
        return framework;
    }

    public static void stopFramework(Framework framework) throws RuntimeException {
        try {
            if (framework != null) {
                framework.stop();
                framework.waitForStop(0);
            }
        } catch (BundleException | InterruptedException e) {
            throw Exceptions.propagate(e);
        }
    }

    /* --- helper functions */

    private static void installBootBundles(Framework framework) {
        Stopwatch timer = Stopwatch.createStarted();
        LOG.debug("Installing OSGi boot bundles from "+EmbeddedFelixFramework.class.getClassLoader()+"...");
        
        Iterator<URL> resources = CANDIDATE_BOOT_BUNDLES.iterator();
        // previously we evaluated this each time, but lately (discovered in 2019,
        // possibly the case for a long time before) it seems to grow, accessing ad hoc dirs
        // in cache/* made by tests, which get deleted, logging lots of errors.
        // so now we statically populate it at load time.
        
        BundleContext bundleContext = framework.getBundleContext();
        Map<String, Bundle> installedBundles = getInstalledBundlesById(bundleContext);
        while (resources.hasNext()) {
            URL url = resources.next();
            ReferenceWithError<?> installResult = installExtensionBundle(bundleContext, url, installedBundles, OsgiUtils.getVersionedId(framework));
            if (installResult.hasError() && !installResult.masksErrorIfPresent()) {
                // these are just candiate boot bundles used in testing so forgive errors and warnings
                if (LOG.isTraceEnabled()) LOG.trace("Unable to install manifest from "+url+": "+installResult.getError(), installResult.getError());
            } else {
                Object result = installResult.getWithoutError();
                if (result instanceof Bundle) {
                    String v = OsgiUtils.getVersionedId( (Bundle)result );
                    SYSTEM_BUNDLES.add(v);
                    if (installResult.hasError()) {
                        if (LOG.isTraceEnabled()) LOG.trace(installResult.getError().getMessage()+(result!=null ? " ("+result+"/"+v+")" : ""));
                    } else {
                        if (LOG.isTraceEnabled()) LOG.trace("Installed "+v+" from "+url);
                    }
                } else if (installResult.hasError()) {
                    LOG.trace(installResult.getError().getMessage());
                }
            }
        }
        if (LOG.isTraceEnabled()) LOG.trace("Installed OSGi boot bundles in "+Time.makeTimeStringRounded(timer)+": "+Arrays.asList(framework.getBundleContext().getBundles()));
    }

    private static Map<String, Bundle> getInstalledBundlesById(BundleContext bundleContext) {
        Map<String, Bundle> installedBundles = new HashMap<String, Bundle>();
        Bundle[] bundles = bundleContext.getBundles();
        for (Bundle b : bundles) {
            installedBundles.put(OsgiUtils.getVersionedId(b), b);
        }
        return installedBundles;
    }

    /** Wraps the bundle if successful or already installed, wraps TRUE if it's the system entry,
     * wraps null if the bundle is already installed from somewhere else;
     * in all these cases <i>masking</i> an explanatory error if already installed or it's the system entry.
     * <p>
     * Returns an instance wrapping null and <i>throwing</i> an error if the bundle could not be installed.
     */
    private static ReferenceWithError<?> installExtensionBundle(BundleContext bundleContext, URL manifestUrl, Map<String, Bundle> installedBundles, String frameworkVersionedId) {
        //ignore http://felix.extensions:9/ system entry
        if("felix.extensions".equals(manifestUrl.getHost()))
            return ReferenceWithError.newInstanceMaskingError(null, new IllegalArgumentException("Skipping install of internal extension bundle from "+manifestUrl));

        try {
            Manifest manifest = readManifest(manifestUrl);
            if (!isValidBundle(manifest))
                return ReferenceWithError.newInstanceMaskingError(null, new IllegalArgumentException("Resource at "+manifestUrl+" is not an OSGi bundle manifest"));

            String versionedId = OsgiUtils.getVersionedId(manifest);
            URL bundleUrl = OsgiUtils.getContainerUrl(manifestUrl, MANIFEST_PATH);

            Bundle existingBundle = installedBundles.get(versionedId);
            if (existingBundle != null) {
                if (!bundleUrl.equals(existingBundle.getLocation()) &&
                        //the framework bundle is always pre-installed, don't display duplicate info
                        !versionedId.equals(frameworkVersionedId)) {
                    return ReferenceWithError.newInstanceMaskingError(null, new IllegalArgumentException("Bundle "+versionedId+" (from manifest " + manifestUrl + ") is already installed, from " + existingBundle.getLocation()));
                }
                return ReferenceWithError.newInstanceMaskingError(existingBundle, new IllegalArgumentException("Bundle "+versionedId+" from manifest " + manifestUrl + " is already installed"));
            }

            byte[] jar = buildExtensionBundle(manifest);
            if (LOG.isTraceEnabled()) LOG.trace("Installing boot bundle " + bundleUrl);
            //mark the bundle as extension so we can detect it later using the "system:" protocol
            //(since we cannot access BundleImpl.isExtension)
            Bundle newBundle = bundleContext.installBundle(EXTENSION_PROTOCOL + ":" + bundleUrl.toString(), new ByteArrayInputStream(jar));
            installedBundles.put(versionedId, newBundle);
            return ReferenceWithError.newInstanceWithoutError(newBundle);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            return ReferenceWithError.newInstanceThrowingError(null,
                new IllegalStateException("Problem installing extension bundle " + manifestUrl + ": "+e, e));
        }
    }

    private static Manifest readManifest(URL manifestUrl) throws IOException {
        Manifest manifest;
        InputStream in = null;
        try {
            in = manifestUrl.openStream();
            manifest = new Manifest(in);
        } finally {
            if (in != null) {
                try {in.close();}
                catch (Exception e) {};
            }
        }
        return manifest;
    }

    private static byte[] buildExtensionBundle(Manifest manifest) throws IOException {
        Attributes atts = manifest.getMainAttributes();

        //the following properties are invalid in extension bundles
        atts.remove(new Attributes.Name(Constants.IMPORT_PACKAGE));
        atts.remove(new Attributes.Name(Constants.REQUIRE_BUNDLE));
        atts.remove(new Attributes.Name(Constants.BUNDLE_NATIVECODE));
        atts.remove(new Attributes.Name(Constants.DYNAMICIMPORT_PACKAGE));
        atts.remove(new Attributes.Name(Constants.BUNDLE_ACTIVATOR));

        //mark as extension bundle
        atts.putValue(Constants.FRAGMENT_HOST, "system.bundle; extension:=framework");

        //create the jar containing the manifest
        ByteArrayOutputStream jar = new ByteArrayOutputStream();
        JarOutputStream out = new JarOutputStream(jar, manifest);
        out.close();
        return jar.toByteArray();
    }

    private static boolean isValidBundle(Manifest manifest) {
        Attributes atts = manifest.getMainAttributes();
        return atts.containsKey(new Attributes.Name(Constants.BUNDLE_MANIFESTVERSION));
    }

    public static boolean isExtensionBundle(Bundle bundle) {
        String location = bundle.getLocation();
        return location != null && 
                EXTENSION_PROTOCOL.equals(Urls.getProtocol(location));
    }

}
