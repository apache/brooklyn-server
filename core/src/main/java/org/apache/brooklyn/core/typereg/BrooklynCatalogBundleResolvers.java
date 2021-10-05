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
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.brooklyn.api.framework.FrameworkLookup;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.ha.BrooklynBomOsgiArchiveInstaller;
import org.apache.brooklyn.core.mgmt.ha.BrooklynBomOsgiArchiveInstaller.PrepareInstallResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.typereg.BrooklynCatalogBundleResolver.BundleInstallationOptions;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
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
    public static Map<BrooklynCatalogBundleResolver,Double> forBundleWithScore(ManagementContext mgmt, Supplier<InputStream> input,
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
        return byScoreMulti.entries().stream().collect(MutableMap::new, (map,entry)->map.put(entry.getValue(), entry.getKey()), (x,y)->{ /* should not be used in sequential stream */ throw new IllegalStateException(); });
    }

    public static List<BrooklynCatalogBundleResolver> forBundle(ManagementContext mgmt, Supplier<InputStream> input,
                                                                         BrooklynCatalogBundleResolver.BundleInstallationOptions options) {
        return MutableList.copyOf( forBundleWithScore(mgmt, input, options).keySet() );
    }

    public static ReferenceWithError<OsgiBundleInstallationResult> install(ManagementContext mgmt, Supplier<InputStream> input,
                                                                           BundleInstallationOptions options) {
        LOG.debug("Installing bundle {} / {} for {}", input, (options==null ? null : options.knownBundleMetadata), Entitlements.getEntitlementContextUser());
        File fileToDelete = null;
        if (input==null && options.knownBundleMetadata==null) {
            return ReferenceWithError.newInstanceThrowingError(null, new IllegalArgumentException("Bundle contents or reference must be supplied"));
        }
        if (input==null && options.knownBundleMetadata!=null) {
            try {
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
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                return ReferenceWithError.newInstanceThrowingError(null, e);
            }
        }

        OsgiBundleInstallationResult firstResult = null;
        try {
            Map<BrooklynCatalogBundleResolver, Double> resolvers = forBundleWithScore(mgmt, input, options);
            Collection<String> resolversWhoDontSupport = new ArrayList<String>();
            Map<BrooklynCatalogBundleResolver, Exception> failuresFromResolvers = MutableMap.of();
            Double highestFailedScore = null;
            for (Entry<BrooklynCatalogBundleResolver,Double> ti : resolvers.entrySet()) {
                BrooklynCatalogBundleResolver t = ti.getKey();
                try {
                    ReferenceWithError<OsgiBundleInstallationResult> result = t.install(input, options);
                    if (result == null) {
                        resolversWhoDontSupport.add(t.getFormatCode() + " (returned null)");
                        continue;
                    }
                    if (firstResult == null) {
                        // capture the first result for more info; but do not return, as it is subsumed in error messages, and is not the most useful error
                        firstResult = result.getWithoutError();
                    }
                    result.get();  // assert there is no error
                    LOG.debug("Installed bundle {} / {} for {}: {}: {}", input, (options==null ? null : options.knownBundleMetadata), Entitlements.getEntitlementContextUser(), result.get().getCode(), result.get().getMessage());
                    if (highestFailedScore!=null) {
                        if (highestFailedScore > 0.9 && (ti.getValue() == null || highestFailedScore > ti.getValue() + 0.1)) {
                            // if there was an error from a high scoring resolver and a lower-scoring resolver accepted it, log a warning
                            LOG.warn("Bundle {} was installed by fallback resolver {} because preferred resolver(s) reported issues: {} / {} (scores {})",
                                    t, result.get().getMetadata(), resolversWhoDontSupport, failuresFromResolvers, resolvers);
                        } else {
                            // if there was an error from a high scoring resolver and a lower-scoring resolver accepted it, log a warning
                            LOG.debug("Bundle {} was installed by resolver {} after other resolver(s) reported issues: {} / {} (scores {})",
                                    t, result.get().getMetadata(), resolversWhoDontSupport, failuresFromResolvers, resolvers);
                        }
                    }
                    return result;
                } catch (@SuppressWarnings("deprecation") UnsupportedCatalogBundleException e) {
                    resolversWhoDontSupport.add(t.getFormatCode() +
                            (Strings.isNonBlank(e.getMessage()) ? " (" + e.getMessage() + ")" : ""));
                } catch (Throwable e) {
                    Exceptions.propagateIfFatal(e);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Resolver for " + t.getFormatCode() + " gave an error creating this plan (may retry): " + e, e);
                    }
                    failuresFromResolvers.put(t, new PropagatedRuntimeException(
                            (t.getFormatCode() + " bundle installation error") + ": " +
                                    Exceptions.collapseText(e), e));
                }
                if (highestFailedScore==null) highestFailedScore = ti.getValue();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Failure resolving bundle; returning summary failure, but for reference "
                        + "potentially applicable resolvers were " + resolvers + " "
                        + "(available ones are " + MutableList.builder().addAll(all(mgmt)).build() + ")"
                        + (failuresFromResolvers.isEmpty() ? "" : "; failures: " + failuresFromResolvers)
                        + (resolversWhoDontSupport.isEmpty() ? "" : "; unsupported by: "+resolversWhoDontSupport)
                        + (firstResult==null ? "" : "; error result: "+firstResult));
            }

            // failed
            Exception exception;
            if (!failuresFromResolvers.isEmpty()) {
                // at least one thought he could do it
                Double score = resolvers.get(failuresFromResolvers.keySet().iterator().next());
                double minScore = score==null ? 0 : score/2;
                // ignore those which are < 1/2 the best score; they're probably items of last resort
                List<Exception> interestingFailures = failuresFromResolvers.entrySet().stream().filter(entry -> Maybe.ofDisallowingNull(resolvers.get(entry.getKey())).or(0d) > minScore).map(entry -> entry.getValue()).collect(Collectors.toList());
                exception = interestingFailures.size() == 1 ? Exceptions.create(null, interestingFailures) :
                        Exceptions.create("All applicable bundle resolvers failed", interestingFailures);
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
            return ReferenceWithError.newInstanceThrowingError(null, exception);
        } catch (Exception e) {
            return ReferenceWithError.newInstanceThrowingError(null, e);
        } finally {
            if (fileToDelete!=null) {
                fileToDelete.delete();
            }
        }
    }

}
