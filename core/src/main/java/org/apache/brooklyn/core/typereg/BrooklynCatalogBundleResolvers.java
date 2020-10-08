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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
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
        Multimap<Double,BrooklynCatalogBundleResolver> byScoreMulti = ArrayListMultimap.create();
        Collection<BrooklynCatalogBundleResolver> transformers = all(mgmt);
        for (BrooklynCatalogBundleResolver transformer : transformers) {
            double score = transformer.scoreForBundle(options.format, input);
            if (LOG.isTraceEnabled()) {
                LOG.trace("SCORE for '" + input + "' at " + transformer + ": " + score);
            }
            if (score>0) byScoreMulti.put(score, transformer);
        }
        Map<Double, Collection<BrooklynCatalogBundleResolver>> tree = new TreeMap<Double, Collection<BrooklynCatalogBundleResolver>>(byScoreMulti.asMap());
        List<Collection<BrooklynCatalogBundleResolver>> highestFirst = new ArrayList<Collection<BrooklynCatalogBundleResolver>>(tree.values());
        Collections.reverse(highestFirst);
        return ImmutableList.copyOf(Iterables.concat(highestFirst));
    }

    public static ReferenceWithError<OsgiBundleInstallationResult> install(ManagementContext mgmt, Supplier<InputStream> input,
                                                                           BundleInstallationOptions options) {
        File fileToDelete = null;
        if (input==null  && options.knownBundleMetadata==null) {
            return ReferenceWithError.newInstanceThrowingError(null, new IllegalArgumentException("Bundle contents or reference must be supplied"));
        }
//            if (options.knownBundleMetadata.getOsgiUniqueUrl()==null) {
//                return ReferenceWithError.newInstanceThrowingError(null, new IllegalArgumentException("Bundle contents or reference with URL must be supplied"));
//            }
        PrepareInstallResult prepareResult = BrooklynBomOsgiArchiveInstaller.prepareInstall(mgmt, options.knownBundleMetadata, input, options.forceUpdateOfNonSnapshots);
        if (prepareResult.alreadyInstalledResult!=null) {
            return ReferenceWithError.newInstanceWithoutError(prepareResult.alreadyInstalledResult);
        }

        fileToDelete = prepareResult.zipFile;
        //we don't need it put into a file, but that's what we used to do so keep doing it that way;
        //and again for backwards compatibility, prefer the zip file that prepareInstall gives us,
        //although it would feel better not to, unless needed
        //if (input==null) {
            if (prepareResult.zipFile!=null) {
                input = InputStreamSource.of(prepareResult.zipFile.getName(), prepareResult.zipFile);
            } else {
                return ReferenceWithError.newInstanceThrowingError(null, new IllegalArgumentException("Bundle contents or knwon reference must be supplied; "+
                        options.knownBundleMetadata+" not known"));
            }
        //}

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
                    result.get();  // assert there is no error
                    return result;
                } catch (@SuppressWarnings("deprecation") UnsupportedCatalogBundleException e) {
                    resolversWhoDontSupport.add(t.getFormatCode() +
                            (Strings.isNonBlank(e.getMessage()) ? " (" + e.getMessage() + ")" : ""));
                } catch (Throwable e) {
                    Exceptions.propagateIfFatal(e);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Transformer for " + t.getFormatCode() + " gave an error creating this plan (may retry): " + e, e);
                    }
                    failuresFromResolvers.add(new PropagatedRuntimeException(
                            (t.getFormatCode() + " bundle installation error") + ": " +
                                    Exceptions.collapseText(e), e));
                }
            }

            // failed
            Exception result;
            if (!failuresFromResolvers.isEmpty()) {
                // at least one thought he could do it
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failure transforming plan; returning summary failure, but for reference "
                            + "potentially application transformers were " + resolvers + ", "
                            + "available ones are " + MutableList.builder().addAll(all(mgmt))
                            .build() + "; "
                            + "failures: " + failuresFromResolvers);
                }
                result = failuresFromResolvers.size() == 1 ? Exceptions.create(null, failuresFromResolvers) :
                        Exceptions.create("All plan transformers failed", failuresFromResolvers);
            } else {
                if (resolvers.isEmpty()) {
                    result = new UnsupportedTypePlanException("Invalid plan; format could not be recognized, none of the available resolvers " + all(mgmt) + " support it");
                } else {
                    result = new UnsupportedTypePlanException("Invalid plan; potentially applicable resolvers " + resolvers + " do not support it, " +
                            "and other available resolvers do not accept it");
                }
            }
            return ReferenceWithError.newInstanceThrowingError(null, result);
        } finally {
            if (fileToDelete!=null) {
                fileToDelete.delete();
            }
        }
    }

}
