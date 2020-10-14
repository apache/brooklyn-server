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

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.function.Supplier;
import org.apache.brooklyn.api.framework.FrameworkLookup;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.mgmt.ha.BrooklynBomOsgiArchiveInstaller;
import org.apache.brooklyn.core.mgmt.ha.BrooklynBomOsgiArchiveInstaller.PrepareInstallResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.typereg.BrooklynCatalogBundleResolver.BundleInstallationOptions;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.stream.InputStreamSource;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrooklynCatalogBundleResolvers {

    private static Logger LOG = LoggerFactory.getLogger(BrooklynCatalogBundleResolvers.class);

    private static Collection<BrooklynCatalogBundleResolver> getAll() {
        return ImmutableList.copyOf(FrameworkLookup.lookupAll(BrooklynCatalogBundleResolver.class));
    }

    private static Collection<Class<? extends BrooklynCatalogBundleResolver>> OVERRIDE;
    @SafeVarargs
    @VisibleForTesting
    public synchronized static void forceAvailable(Class<? extends BrooklynCatalogBundleResolver> ...classes) {
        OVERRIDE = Arrays.asList(classes);
    }
    public synchronized static void clearForced() {
        OVERRIDE = null;
    }

    public static Collection<BrooklynCatalogBundleResolver> all(ManagementContext mgmt) {
        // TODO cache these in the TypeRegistry, looking for new ones periodically or supplying a way to register them
        Collection<Class<? extends BrooklynCatalogBundleResolver>> override = OVERRIDE;
        Collection<BrooklynCatalogBundleResolver> result = new ArrayList<>();
        if (override!=null) {
            for (Class<? extends BrooklynCatalogBundleResolver> o1: override) {
                try {
                    result.add(o1.newInstance());
                } catch (Exception e) {
                    Exceptions.propagate(e);
                }
            }
        } else {
            result.addAll(getAll());
        }
        for(BrooklynCatalogBundleResolver t : result) {
            t.setManagementContext(mgmt);
        }
        return result;
    }

    /** returns a list of {@link BrooklynCatalogBundleResolver} instances for this {@link ManagementContext}
     * which may be able to handle the given bundle; the list is sorted with highest-score transformer first */
    @Beta
    public static List<BrooklynCatalogBundleResolver> forBundle(ManagementContext mgmt, Supplier<InputStream> input,
                                                                BrooklynCatalogBundleResolver.BundleInstallationOptions options) {
        Multimap<Double,BrooklynCatalogBundleResolver> byScoreMulti = TreeMultimap.create(Comparator.reverseOrder(), (r1, r2) -> 0);
        Collection<BrooklynCatalogBundleResolver> resolvers = all(mgmt);
        for (BrooklynCatalogBundleResolver transformer : resolvers) {
            double score = transformer.scoreForBundle(options==null ? null : options.format, input);
            if (LOG.isTraceEnabled()) {
                LOG.trace("SCORE for '" + input + "' at " + transformer + ": " + score);
            }
            if (score>0) byScoreMulti.put(score, transformer);
        }
        return ImmutableList.copyOf(Iterables.concat(byScoreMulti.values()));
    }

    public static ReferenceWithError<OsgiBundleInstallationResult> install(ManagementContext mgmt, Supplier<InputStream> input,
                                                                           BundleInstallationOptions options) {
        LOG.debug("Installing bundle {} / {}", input, (options==null ? null : options.knownBundleMetadata));
        File fileToDelete = null;
        if (input==null && options.knownBundleMetadata==null) {
            return ReferenceWithError.newInstanceThrowingError(null, new IllegalArgumentException("Bundle contents or reference must be supplied"));
        }
        if (input==null && options.knownBundleMetadata!=null) {
            // installing to brooklyn a bundle already installed to OSGi
            PrepareInstallResult prepareResult = BrooklynBomOsgiArchiveInstaller.prepareInstall(mgmt, options.knownBundleMetadata, null, input, options.forceUpdateOfNonSnapshots, null);
            if (prepareResult.resultObject != null) {
                return ReferenceWithError.newInstanceWithoutError(prepareResult.resultObject);
            }

            fileToDelete = prepareResult.zipFile;
            if (prepareResult.zipFile != null) {
                input = InputStreamSource.of(prepareResult.zipFile.getName(), prepareResult.zipFile);
            } else {
                return ReferenceWithError.newInstanceThrowingError(null, new IllegalArgumentException("Bundle contents or known reference must be supplied; " +
                        options.knownBundleMetadata + " not known"));
            }
        }

        OsgiBundleInstallationResult firstResult = null;
        try {
            List<BrooklynCatalogBundleResolver> resolvers = forBundle(mgmt, input, options);
            Collection<String> resolversWhoDontSupport = new ArrayList<String>();
            Collection<Exception> failuresFromResolvers = new ArrayList<Exception>();
            for (BrooklynCatalogBundleResolver t : resolvers) {
                try {
                    ReferenceWithError<OsgiBundleInstallationResult> result = t.install(input, options);
                    if (result == null) {
                        resolversWhoDontSupport.add(t.getFormatCode() + " (returned null)");
                        continue;
                    }
                    if (firstResult == null) {
                        // return the first result for more info
                        firstResult = result.getWithoutError();
                    }
                    result.get();  // assert there is no error
                    LOG.debug("Installed bundle {} / {}: {}: {}", input, (options==null ? null : options.knownBundleMetadata), result.get().getCode(), result.get().getMessage());
                    return result;
                } catch (@SuppressWarnings("deprecation") UnsupportedCatalogBundleException e) {
                    resolversWhoDontSupport.add(t.getFormatCode() +
                            (Strings.isNonBlank(e.getMessage()) ? " (" + e.getMessage() + ")" : ""));
                } catch (Throwable e) {
                    Exceptions.propagateIfFatal(e);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Resolver for " + t.getFormatCode() + " gave an error creating this plan (may retry): " + e, e);
                    }
                    failuresFromResolvers.add(new PropagatedRuntimeException(
                            (t.getFormatCode() + " bundle installation error") + ": " +
                                    Exceptions.collapseText(e), e));
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Failure resolving bundle; returning summary failure, but for reference "
                        + "potentially applicable resolvers were " + resolvers + ", "
                        + "available ones are " + MutableList.builder().addAll(all(mgmt)).build() + "; "
                        + "failures: " + failuresFromResolvers+"; "
                        + "unsupported by: "+resolversWhoDontSupport);
            }

            // failed
            Exception exception;
            if (!failuresFromResolvers.isEmpty()) {
                // at least one thought he could do it
                exception = failuresFromResolvers.size() == 1 ? Exceptions.create(null, failuresFromResolvers) :
                        Exceptions.create("All applicable bundle resolvers failed", failuresFromResolvers);
            } else {
                String prefix = Strings.isBlank(options.format) ? "Invalid bundle" : "Invalid '"+options.format+"' bundle";
                if (resolvers.isEmpty()) {
                    exception = new UnsupportedTypePlanException(prefix + "; format could not be recognized, none of the available resolvers " + all(mgmt) + " support it");
                } else {
                    exception = new UnsupportedTypePlanException(prefix + "; potentially applicable resolvers " + resolvers + " do not support it, and other available resolvers " +
//                    // the removeAll call below won't work until "all" caches it
//                    MutableList.builder().addAll(all(mgmt)).removeAll(transformers).build()+" "+
                            "do not accept it");
                }
            }
            return ReferenceWithError.newInstanceThrowingError(firstResult, exception);
        } catch (Exception e) {
            return ReferenceWithError.newInstanceThrowingError(firstResult, e);
        } finally {
            if (fileToDelete!=null) {
                fileToDelete.delete();
            }
        }
    }

}
