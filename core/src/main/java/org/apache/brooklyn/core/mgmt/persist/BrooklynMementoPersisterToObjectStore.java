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
package org.apache.brooklyn.core.mgmt.persist;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.xml.xpath.XPathConstants;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.rebind.PersistenceExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.mgmt.rebind.mementos.*;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.mgmt.classloading.BrooklynClassLoadingContextSequential;
import org.apache.brooklyn.core.mgmt.classloading.ClassLoaderFromBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore.StoreObjectAccessor;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore.StoreObjectAccessorWithLock;
import org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializer.XmlMementoSerializerBuilder;
import org.apache.brooklyn.core.mgmt.rebind.PeriodicDeltaChangeListener;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl;
import org.apache.brooklyn.core.mgmt.rebind.dto.BrooklynMementoImpl;
import org.apache.brooklyn.core.mgmt.rebind.dto.BrooklynMementoManifestImpl;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.xstream.XmlUtil;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/** Implementation of the {@link BrooklynMementoPersister} backed by a pluggable
 * {@link PersistenceObjectStore} such as a file system or a jclouds object store */
public class BrooklynMementoPersisterToObjectStore implements BrooklynMementoPersister {

    // TODO Crazy amount of duplication between handling entity, location, policy, enricher + feed;
    // Need to remove that duplication.

    // TODO Should stop() take a timeout, and shutdown the executor gracefully?
    
    private static final Logger LOG = LoggerFactory.getLogger(BrooklynMementoPersisterToObjectStore.class);
    public static final String PLANE_ID_FILE_NAME = "planeId";


    public static final ConfigKey<Integer> PERSISTER_MAX_THREAD_POOL_SIZE = ConfigKeys.newIntegerConfigKey(
            "persister.threadpool.maxSize",
            "Maximum number of concurrent operations for persistence (reads/writes/deletes of *different* objects)", 
            10);

    public static final ConfigKey<Integer> PERSISTER_MAX_SERIALIZATION_ATTEMPTS = ConfigKeys.newIntegerConfigKey(
            "persister.maxSerializationAttempts",
            "Maximum number of attempts to serialize a memento (e.g. if first attempts fail because of concurrent modifications of an entity)", 
            5);

    private final PersistenceObjectStore objectStore;
    private final MementoSerializer<Object> serializerWithStandardClassLoader;

    private final Map<String, StoreObjectAccessorWithLock> writers = new LinkedHashMap<String, PersistenceObjectStore.StoreObjectAccessorWithLock>();

    private ListeningExecutorService executor;

    private volatile boolean writesAllowed = false;
    private volatile boolean writesShuttingDown = false;
    private StringConfigMap brooklynProperties;
    private ManagementContext mgmt = null;
    
    private List<Delta> queuedDeltas = new CopyOnWriteArrayList<BrooklynMementoPersister.Delta>();
    
    /**
     * Lock used on writes (checkpoint + delta) so that {@link #waitForWritesCompleted(Duration)} can block
     * for any concurrent call to complete.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Set<String> lastErrors = MutableSet.of();

    public BrooklynMementoPersisterToObjectStore(PersistenceObjectStore objectStore, ManagementContext mgmt) {
        this(objectStore, mgmt, mgmt.getCatalogClassLoader());
    }
    
    public BrooklynMementoPersisterToObjectStore(PersistenceObjectStore objectStore, ManagementContext mgmt, ClassLoader classLoader) {
        this(objectStore, ((ManagementContextInternal)mgmt).getBrooklynProperties(), classLoader);
        this.mgmt = mgmt;
    }
    
    /** @deprecated since 0.12.0 use constructor taking management context */
    @Deprecated
    public BrooklynMementoPersisterToObjectStore(PersistenceObjectStore objectStore, StringConfigMap brooklynProperties, ClassLoader classLoader) {
        this.objectStore = checkNotNull(objectStore, "objectStore");
        this.brooklynProperties = brooklynProperties;
        
        int maxSerializationAttempts = brooklynProperties.getConfig(PERSISTER_MAX_SERIALIZATION_ATTEMPTS);
        MementoSerializer<Object> rawSerializer = XmlMementoSerializerBuilder.from(brooklynProperties)
                .withBrooklynDeserializingClassRenames()
                .withClassLoader(classLoader).build();
        this.serializerWithStandardClassLoader = new RetryingMementoSerializer<Object>(rawSerializer, maxSerializationAttempts);

        objectStore.createSubPath("entities");
        objectStore.createSubPath("locations");
        objectStore.createSubPath("policies");
        objectStore.createSubPath("enrichers");
        objectStore.createSubPath("feeds");
        objectStore.createSubPath("catalog");

        // FIXME does it belong here or to ManagementPlaneSyncRecordPersisterToObjectStore ?
        objectStore.createSubPath("plane");
        
        resetExecutor();
    }

    private Integer maxThreadPoolSize() {
        return brooklynProperties.getConfig(PERSISTER_MAX_THREAD_POOL_SIZE);
    }

    public MementoSerializer<Object> getMementoSerializer() {
        return getSerializerWithStandardClassLoader();
    }
    
    protected MementoSerializer<Object> getSerializerWithStandardClassLoader() {
        return serializerWithStandardClassLoader;
    }
    
    protected MementoSerializer<Object> getSerializerWithCustomClassLoader(LookupContext lookupContext, BrooklynObjectType type, String objectId) {
        ClassLoader cl = getCustomClassLoaderForBrooklynObject(lookupContext, type, objectId);
        if (cl==null) return serializerWithStandardClassLoader;
        return getSerializerWithCustomClassLoader(lookupContext, cl);
    }
    
    protected MementoSerializer<Object> getSerializerWithCustomClassLoader(LookupContext lookupContext, ClassLoader classLoader) {
        int maxSerializationAttempts = brooklynProperties.getConfig(PERSISTER_MAX_SERIALIZATION_ATTEMPTS);
        MementoSerializer<Object> rawSerializer = XmlMementoSerializerBuilder.from(brooklynProperties)
                .withBrooklynDeserializingClassRenames()
                .withClassLoader(classLoader).build();
        MementoSerializer<Object> result = new RetryingMementoSerializer<Object>(rawSerializer, maxSerializationAttempts);
        result.setLookupContext(lookupContext);
        return result;
    }
    
    @Nullable protected ClassLoader getCustomClassLoaderForBrooklynObject(LookupContext lookupContext,
                                                                          BrooklynObjectType type, String objectId) {
        BrooklynObject item = lookupContext.peek(type, objectId);
        String catalogItemId = (item == null) ? null : item.getCatalogItemId();
        // TODO enrichers etc aren't yet known -- would need to backtrack to the entity to get them from bundles
        if (catalogItemId == null) {
            return null;
        }
        // See RebindIteration.BrooklynObjectInstantiator.load(), for handling where catalog item is missing;
        // similar logic here.
        final ManagementContext managementContext = lookupContext.lookupManagementContext();
        RegisteredType catalogItem = managementContext.getTypeRegistry().get(catalogItemId);
        if (catalogItem == null) {
            // will happen on rebind if our execution should have ended
            if (Thread.interrupted()) {
                LOG.debug("Aborting (probably old) rebind iteration");
                throw new RuntimeInterruptedException("Rebind iteration cancelled");
            }

            // might come here for other reasons too
            LOG.debug("Unable to load registered type "+catalogItemId
                +" for custom class loader of " + type + " " + objectId + "; will use default class loader");
            return null;
        } else {
            final BrooklynClassLoadingContextSequential ctx = new BrooklynClassLoadingContextSequential(managementContext);
            ctx.add(CatalogUtils.newClassLoadingContextForCatalogItems(managementContext,
                    item.getCatalogItemId(), item.getCatalogItemIdSearchPath()));
            return ClassLoaderFromBrooklynClassLoadingContext.of(ctx);
        }
    }
    
    @Override public void enableWriteAccess() {
        writesAllowed = true;
    }
    
    @Override
    public void disableWriteAccess(boolean graceful) {
        writesShuttingDown = true;
        try {
            writesAllowed = false;
            // a very long timeout to ensure we don't lose state. 
            // If persisting thousands of entities over slow network to Object Store, could take minutes.
            waitForWritesCompleted(Duration.ONE_HOUR);
            
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } finally {
            writesShuttingDown = false;
        }
    }
    
    @Override 
    public void stop(boolean graceful) {
        disableWriteAccess(graceful);
        stopExecutor(graceful);
    }

    @Override
    public void reset() {
        resetExecutor();
    }

    public void resetExecutor() {
        stopExecutor(false);
        executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(maxThreadPoolSize(), new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
                // Note: Thread name referenced in logback-includes' ThreadNameDiscriminator
                return new Thread(r, "brooklyn-persister");
            }}));
    }

    public void stopExecutor(boolean graceful) {
        if (executor != null) {
            if (graceful) {
                executor.shutdown();
                try {
                    // should be quick because we've just turned off writes, waiting for their completion
                    executor.awaitTermination(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            } else {
                executor.shutdownNow();
            }
        }
    }

    public PersistenceObjectStore getObjectStore() {
        return objectStore;
    }

    protected StoreObjectAccessorWithLock getWriter(String path) {
        String id = path.substring(path.lastIndexOf('/')+1);
        synchronized (writers) {
            StoreObjectAccessorWithLock writer = writers.get(id);
            if (writer == null) {
                writer = new StoreObjectAccessorLocking( objectStore.newAccessor(path) );
                writers.put(id, writer);
            }
            return writer;
        }
    }

    private Map<String,String> makeIdSubPathMap(Iterable<String> subPathLists) {
        Map<String,String> result = MutableMap.of();
        for (String subpath: subPathLists) {
            String id = subpath;
            id = id.substring(id.lastIndexOf('/')+1);
            id = id.substring(id.lastIndexOf('\\')+1);
            // assumes id is the filename; should work even if not, as id is later read from xpath
            // but you'll get warnings (and possibility of loss if there is a collision)
            result.put(id, subpath);
        }
        return result;
    }
    
    protected BrooklynMementoRawData listMementoSubPathsAsData(final RebindExceptionHandler exceptionHandler) {
        final BrooklynMementoRawData.Builder subPathDataBuilder = BrooklynMementoRawData.builder();

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
                subPathDataBuilder.putAll(type, makeIdSubPathMap(objectStore.listContentsWithSubPath(type.getSubPathName())));
            }
            
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            exceptionHandler.onLoadMementoFailed(BrooklynObjectType.UNKNOWN, "Failed to list files", e);
            throw new IllegalStateException("Failed to list memento files in "+objectStore, e);
        }

        BrooklynMementoRawData subPathData = subPathDataBuilder.build();
        LOG.debug("Loaded rebind lists; took {}: {} entities, {} locations, {} policies, {} enrichers, {} feeds, {} catalog items, {} bundles; from {}", new Object[]{
            Time.makeTimeStringRounded(stopwatch),
            subPathData.getEntities().size(), subPathData.getLocations().size(), subPathData.getPolicies().size(), subPathData.getEnrichers().size(), 
            subPathData.getFeeds().size(), subPathData.getCatalogItems().size(), subPathData.getBundles().size(),
            objectStore.getSummaryName() });
        
        return subPathData;
    }
    
    @Override
    public BrooklynMementoRawData loadMementoRawData(final RebindExceptionHandler exceptionHandler) {
        BrooklynMementoRawData subPathData = listMementoSubPathsAsData(exceptionHandler);
        
        final BrooklynMementoRawData.Builder builder = BrooklynMementoRawData.builder();
        
        Visitor loaderVisitor = new Visitor() {
            @Override
            public void visit(BrooklynObjectType type, String id, String contentsSubpath) throws Exception {
                if (type == BrooklynObjectType.MANAGED_BUNDLE && id.endsWith(".jar")) {
                    // don't visit jar files directly; someone else will read them
                    return;
                }
                
                String contents = null;
                try {
                    contents = read(contentsSubpath);
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    exceptionHandler.onLoadMementoFailed(type, "memento "+id+" read error", e);
                }
                if (contents==null) {
                    LOG.warn("No contents for "+contentsSubpath+" in persistence store; ignoring");

                } else {
                    String xmlId = (String) XmlUtil.xpathHandlingIllegalChars(contents, "/" + type.toCamelCase() + "/id");
                    String xmlUrl = getXmlValue(contents, "/" + type.toCamelCase() + "/url").orNull();
                    String xmlJavaType = getXmlValue(contents, "/" + type.toCamelCase() + "/type").orNull();
                    String summary = MutableList.<String>of(contentsSubpath, type.toCamelCase()).appendIfNotNull(xmlId).appendIfNotNull(xmlUrl).appendIfNotNull(xmlJavaType).stream().collect(Collectors.joining(" / "));

                    String safeXmlId = Strings.makeValidFilename(xmlId);
                    if (!Objects.equal(id, safeXmlId)) {
                        LOG.warn("ID mismatch on " + summary + ": xml id is " + safeXmlId);
                    }

                    boolean include;
                    if (type == BrooklynObjectType.MANAGED_BUNDLE) {
                        // could R/W to cache space directly, rather than memory copy then extra file copy
                        byte[] jarData = readBytes(contentsSubpath + ".jar");
                        include = (jarData != null);
                        if (!include) {
                            LOG.warn("No JAR data for "+summary+"; assuming deprecated and not meant to be installed, so ignoring");
                            //throw new IllegalStateException("No bundle data for " + contentsSubpath+".jar; contents:\n"+contents);
                        } else {
                            LOG.debug("Including bundle "+summary+", with jar data size "+jarData.length);
                            builder.bundleJar(id, ByteSource.wrap(jarData));
                        }
                    } else {
                        LOG.debug("Including item "+summary);
                        include = true;
                    }
                    if (include) {
                        builder.put(type, xmlId, contents);
                    }
                }
            }
        };

        Stopwatch stopwatch = Stopwatch.createStarted();

        builder.planeId(Strings.emptyToNull(read(PLANE_ID_FILE_NAME)));
        visitMemento("loading raw", subPathData, loaderVisitor, exceptionHandler);
        
        BrooklynMementoRawData result = builder.build();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded rebind raw data; took {}; {} entities, {} locations, {} policies, {} enrichers, {} feeds, {} catalog items, {} bundles, from {}", new Object[]{
                     Time.makeTimeStringRounded(stopwatch.elapsed(TimeUnit.MILLISECONDS)), result.getEntities().size(), 
                     result.getLocations().size(), result.getPolicies().size(), result.getEnrichers().size(),
                     result.getFeeds().size(), result.getCatalogItems().size(), result.getBundles().size(),
                     objectStore.getSummaryName() });
        }

        return result;
    }

    private Maybe<String> getXmlValue(String xml, String path) {
        try {
            return Maybe.ofDisallowingNull((String) XmlUtil.xpathHandlingIllegalChars(xml, path));
        } catch (Exception e) { Exceptions.propagateIfFatal(e); /* otherwise ignore if no url */ }
        return Maybe.absent();
    }

    private static class XPathHelper {
        private String contents;
        private String prefix;

        public XPathHelper(String contents, String prefix) {
            this.contents = contents;
            this.prefix = prefix;
        }

        private String get(String innerPath) {
            return (String) XmlUtil.xpathHandlingIllegalChars(contents, prefix+innerPath);
        }
        private List<String> getStringList(String innerPath) {
            List<String> result = MutableList.of();
            final NodeList nodeList =
                (NodeList) XmlUtil.xpathHandlingIllegalChars(contents, prefix + innerPath + "//string", XPathConstants.NODESET);
            for(int c = 0 ; c < nodeList.getLength() ; c++) {
                result.add(nodeList.item(c).getFirstChild().getNodeValue());
            }
            return result;
        }
    }


    @Override
    public BrooklynMementoManifest loadMementoManifest(BrooklynMementoRawData mementoDataR,
                                                       final RebindExceptionHandler exceptionHandler) throws IOException {
        final BrooklynMementoRawData mementoData = mementoDataR==null ? loadMementoRawData(exceptionHandler) : mementoDataR;
        
        final BrooklynMementoManifestImpl.Builder builder = BrooklynMementoManifestImpl.builder();

        builder.planeId(mementoData.getPlaneId());

        Visitor visitor = new Visitor() {
            @Override
            public void visit(BrooklynObjectType type, String objectId, final String contents) throws Exception {
                XPathHelper x = new XPathHelper(contents, "/"+type.toCamelCase()+"/");
                switch (type) {
                    case ENTITY:
                        builder.entity(x.get("id"), x.get("type"), Strings.emptyToNull(x.get("parent")),
                            Strings.emptyToNull(x.get("catalogItemId")),
                            x.getStringList("searchPath"));
                        break;
                    case LOCATION:
                    case POLICY:
                    case ENRICHER:
                    case FEED:
                        builder.putType(type, x.get("id"), x.get("type"));
                        break;
                    case CATALOG_ITEM:
                        try {
                            CatalogItemMemento memento = (CatalogItemMemento) getSerializerWithStandardClassLoader().fromString(contents);
                            if (memento == null) {
                                LOG.warn("No "+type.toCamelCase()+"-memento deserialized from " + objectId + "; ignoring and continuing");
                            } else {
                                builder.catalogItem(memento);
                            }
                        } catch (Exception e) {
                            exceptionHandler.onLoadMementoFailed(type, "catalog memento "+objectId+" early catalog deserialization error", e);
                        }
                        break;
                    case MANAGED_BUNDLE:
                        try {
                            ManagedBundleMemento memento = (ManagedBundleMemento) getSerializerWithStandardClassLoader().fromString(contents);
                            builder.bundle( memento );
                            memento.setJarContent(mementoData.getBundleJars().get(objectId));
                        } catch (Exception e) {
                            exceptionHandler.onLoadMementoFailed(type, "bundle memento "+objectId+" early catalog deserialization error", e);
                        }
                        break;
                        
                    default:
                        throw new IllegalStateException("Unexpected brooklyn type: "+type);
                }
            }
        };

        Stopwatch stopwatch = Stopwatch.createStarted();

        visitMemento("manifests", mementoData, visitor, exceptionHandler);
        
        BrooklynMementoManifest result = builder.build();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded rebind manifests; took {}: {} entities, {} locations, {} policies, {} enrichers, {} feeds, {} catalog items, {} bundles; from {}", new Object[]{
                     Time.makeTimeStringRounded(stopwatch), 
                     result.getEntityIdToManifest().size(), result.getLocationIdToType().size(), 
                     result.getPolicyIdToType().size(), result.getEnricherIdToType().size(), result.getFeedIdToType().size(), 
                     result.getCatalogItemMementos().size(), result.getBundles().size(),
                     objectStore.getSummaryName() });
        }

        return result;
    }
    
    @Override
    public BrooklynMemento loadMemento(BrooklynMementoRawData mementoData, final LookupContext lookupContext, final RebindExceptionHandler exceptionHandler) throws IOException {
        LOG.debug("Loading mementos");

        if (mementoData==null)
            mementoData = loadMementoRawData(exceptionHandler);

        Stopwatch stopwatch = Stopwatch.createStarted();

        final BrooklynMementoImpl.Builder builder = BrooklynMementoImpl.builder();

        builder.planeId(mementoData.getPlaneId());

        Visitor visitor = new Visitor() {
            @Override
            public void visit(BrooklynObjectType type, String objectId, String contents) throws Exception {
                try {
                    Memento memento;
                    try {
                        lookupContext.pushContextDescription(""+type.toString().toLowerCase()+" "+objectId);
                        memento = (Memento) getSerializerWithCustomClassLoader(lookupContext, type, objectId).fromString(contents);
                    } finally {
                        lookupContext.popContextDescription();
                    }
                    if (memento == null) {
                        LOG.warn("No "+type.toCamelCase()+"-memento deserialized from " + objectId + "; ignoring and continuing");
                    } else {
                        builder.memento(memento);
                    }
                } catch (Exception e) {
                    exceptionHandler.onLoadMementoFailed(type, "memento "+objectId+" ("+type+") deserialization error", e);
                }
            }

        };

        // TODO not convinced this is single threaded on reads; maybe should get a new one each time?
        getSerializerWithStandardClassLoader().setLookupContext(lookupContext);
        try {
            visitMemento("deserialization", mementoData, visitor, exceptionHandler);
        } finally {
            getSerializerWithStandardClassLoader().unsetLookupContext();
        }

        BrooklynMemento result = builder.build();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded rebind mementos; took {}: {} entities, {} locations, {} policies, {} enrichers, {} feeds, {} catalog items, {} bundles, from {}", new Object[]{
                      Time.makeTimeStringRounded(stopwatch.elapsed(TimeUnit.MILLISECONDS)), result.getEntityIds().size(), 
                      result.getLocationIds().size(), result.getPolicyIds().size(), result.getEnricherIds().size(), result.getManagedBundleIds().size(),
                      result.getFeedIds().size(), result.getCatalogItemIds().size(),
                      objectStore.getSummaryName() });
        }
        
        return result;
    }
    
    protected interface Visitor {
        public void visit(BrooklynObjectType type, String id, String contents) throws Exception;
    }
    
    protected void visitMemento(final String phase, final BrooklynMementoRawData rawData, final Visitor visitor, final RebindExceptionHandler exceptionHandler) {
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        
        class VisitorWrapper implements Runnable {
            private final BrooklynObjectType type;
            private final Map.Entry<String,String> objectIdAndData;
            public VisitorWrapper(BrooklynObjectType type, Map.Entry<String,String> objectIdAndData) {
                this.type = type;
                this.objectIdAndData = objectIdAndData;
            }
            @Override
            public void run() {
                try {
                    try {
                        visitor.visit(type, objectIdAndData.getKey(), objectIdAndData.getValue());
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        if (Thread.currentThread().isInterrupted()) {
                            throw new RuntimeInterruptedException("Interruption discovered", e);
                        }
                        exceptionHandler.onLoadMementoFailed(type, "memento " + objectIdAndData.getKey() + " " + phase + " error", e);
                    }
                } catch (RuntimeInterruptedException e) {
                    LOG.debug("Ending persistence on interruption, probably cancelled when server about to transition: "+e);
                }
            }
        }
        
        for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
            for (final Map.Entry<String,String> entry : rawData.getObjectsOfType(type).entrySet()) {
                futures.add(executor.submit(new VisitorWrapper(type, entry)));
            }
        }

        try {
            // Wait for all, failing fast if any exceptions.
            Futures.allAsList(futures).get();
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            
            List<Exception> exceptions = Lists.newArrayList();
            
            for (ListenableFuture<?> future : futures) {
                if (future.isDone()) {
                    try {
                        future.get();
                    } catch (InterruptedException|RuntimeInterruptedException e2) {
                        throw Exceptions.propagate(e2);
                    } catch (ExecutionException e2) {
                        LOG.warn("Problem loading memento ("+phase+"): "+e2, e2);
                        exceptions.add(e2);
                    }
                    future.cancel(true);
                }
            }
            if (exceptions.isEmpty()) {
                throw Exceptions.propagate(e);
            } else {
                // Normally there should be at lesat one failure; otherwise all.get() would not have failed.
                throw new CompoundRuntimeException("Problem loading mementos ("+phase+")", exceptions);
            }
        }
    }

    protected void checkWritesAllowed() {
        if (!writesAllowed && !writesShuttingDown) {
            throw new IllegalStateException("Writes not allowed in "+this);
        }
    }

    @Override
    public Set<String> getLastErrors() {
        return lastErrors;
    }

    /** See {@link BrooklynPersistenceUtils} for conveniences for using this method. */
    @Override
    @Beta
    public boolean checkpoint(BrooklynMementoRawData newMemento, PersistenceExceptionHandler exceptionHandler, String context, @Nullable RebindManager contextDetails) {
        checkWritesAllowed();
        try {
            lock.writeLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
        
        try {
            if (LOG.isDebugEnabled()) {
                if (contextDetails!=null && !contextDetails.hasPending()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Checkpointing memento for {}", context);
                    }
                } else {
                    LOG.debug("Checkpointing memento for {}", context);
                }
            }

            exceptionHandler.clearRecentErrors();
            objectStore.prepareForMasterUse();
            
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<ListenableFuture<?>> futures = Lists.newArrayList();

            futures.add(asyncUpdatePlaneId(newMemento.getPlaneId(), exceptionHandler));
            for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
                for (Map.Entry<String, String> entry : newMemento.getObjectsOfType(type).entrySet()) {
                    addPersistContentIfManagedBundle(type, false, entry.getKey(), entry.getValue(), futures, exceptionHandler, contextDetails);
                    futures.add(asyncPersist(type.getSubPathName(), type, entry.getKey(), entry.getValue(), exceptionHandler));
                }
            }
            
            try {
                // Wait for all the tasks to complete or fail, rather than aborting on the first failure.
                // But then propagate failure if any fail. (hence the two calls).
                Futures.successfulAsList(futures).get();
                Futures.allAsList(futures).get();
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
            if (LOG.isDebugEnabled()) LOG.debug("Checkpointed memento in {} (for {})", Time.makeTimeStringRounded(stopwatch), context);
        } finally {
            lastErrors = exceptionHandler.getRecentErrors();
            lock.writeLock().unlock();
        }
        return lastErrors.isEmpty();
    }

    @Override
    public boolean delta(Delta delta, PersistenceExceptionHandler exceptionHandler) {
        checkWritesAllowed();
        Set<String> theseErrors = MutableSet.of();

        while (!queuedDeltas.isEmpty()) {
            Delta extraDelta = queuedDeltas.remove(0);
            theseErrors.addAll( doDelta(extraDelta, exceptionHandler, true) );
        }

        theseErrors.addAll( doDelta(delta, exceptionHandler, false) );
        lastErrors = theseErrors;
        return theseErrors.isEmpty();
    }
    
    protected Set<String> doDelta(Delta delta, PersistenceExceptionHandler exceptionHandler, boolean previouslyQueued) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Set<String> theseLastErrors = deltaImpl(delta, exceptionHandler);
        
        if (LOG.isDebugEnabled()) LOG.debug("Checkpointed "+(previouslyQueued ? "previously queued " : "")+"delta of memento in {}: "
                + "updated {} entities, {} locations, {} policies, {} enrichers, {} catalog items, {} bundles; "
                + "removed {} entities, {} locations, {} policies, {} enrichers, {} catalog items, {} bundles"
                + (theseLastErrors.isEmpty() ? "" : "; "+theseLastErrors.size()+" errors: "+theseLastErrors),
                    new Object[] {Time.makeTimeStringRounded(stopwatch),
                        delta.entities().size(), delta.locations().size(), delta.policies().size(), delta.enrichers().size(), delta.catalogItems().size(), delta.bundles().size(),
                        delta.removedEntityIds().size(), delta.removedLocationIds().size(), delta.removedPolicyIds().size(), delta.removedEnricherIds().size(), delta.removedCatalogItemIds().size(), delta.removedBundleIds().size()});

        return theseLastErrors;
    }
    
    @Override
    public void queueDelta(Delta delta) {
        queuedDeltas.add(delta);
    }
    
    /**
     * Concurrent calls will queue-up (the lock is "fair", which means an "approximately arrival-order policy").
     * Current usage is with the {@link PeriodicDeltaChangeListener} so we expect only one call at a time.
     * 
     * TODO Longer term, if we care more about concurrent calls we could merge the queued deltas so that we
     * don't do unnecessary repeated writes of an entity.
     */
    private Set<String> deltaImpl(Delta delta, PersistenceExceptionHandler exceptionHandler) {
        try {
            lock.writeLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
        try {
            exceptionHandler.clearRecentErrors();
            objectStore.prepareForMasterUse();

            List<ListenableFuture<?>> futures = Lists.newArrayList();
            
            Set<String> deletedIds = MutableSet.of();
            for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
                deletedIds.addAll(delta.getRemovedIdsOfType(type));
            }
            
            if (delta.planeId() != null) {
                futures.add(asyncUpdatePlaneId(delta.planeId(), exceptionHandler));
            }
            for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
                for (Memento item : delta.getObjectsOfType(type)) {
                    if (!deletedIds.contains(item.getId())) {
                        addPersistContentIfManagedBundle(type, true, item.getId(), ""+item.getCatalogItemId()+"/"+item.getDisplayName(), futures, exceptionHandler, null);
                        futures.add(asyncPersist(type.getSubPathName(), item, exceptionHandler));
                    }
                }
            }
            for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
                for (String id : delta.getRemovedIdsOfType(type)) {
                    futures.add(asyncDelete(type.getSubPathName(), id, exceptionHandler));
                    if (type==BrooklynObjectType.MANAGED_BUNDLE) {
                        futures.add(asyncDelete(type.getSubPathName(), id+".jar", exceptionHandler));
                    }
                }
            }
            
            try {
                // Wait for all the tasks to complete or fail, rather than aborting on the first failure.
                // But then propagate failure if any fail. (hence the two calls).
                Futures.successfulAsList(futures).get();
                Futures.allAsList(futures).get();
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
            
        } finally {
            lastErrors = exceptionHandler.getRecentErrors();
            lock.writeLock().unlock();
        }
        return lastErrors;
    }

    private void addPersistContentIfManagedBundle(final BrooklynObjectType type, final boolean isDelta, final String id, final String summaryOrContents, List<ListenableFuture<?>> futures, final PersistenceExceptionHandler exceptionHandler, final @Nullable RebindManager deltaContext) {
        if (type==BrooklynObjectType.MANAGED_BUNDLE) {
            if (mgmt==null) {
                throw new IllegalStateException("Cannot persist bundles without a management context");
            }
            final ManagedBundle mb = ((ManagementContextInternal)mgmt).getOsgiManager().get().getManagedBundles().get(id);
            LOG.debug("Persisting managed bundle "+id+": "+mb+" - "+summaryOrContents);
            if (mb==null) {
                if (deltaContext!=null && deltaContext instanceof RebindManagerImpl && ((RebindManagerImpl)deltaContext).isBundleIdUnmanaged(id)) {
                    // known to happen if we add then remove something, because it is still listed
                    LOG.trace("Skipipng absent managed bundle for added and removed bundle {}; ignoring (probably uninstalled or reinstalled with another OSGi ID; see debug log for contents)", id);
                } else {
                    LOG.warn("Cannot find managed bundle for added bundle {}; ignoring (probably uninstalled or reinstalled with another OSGi ID; see debug log for contents)", id);
                }
                return;
            }
            
            if (mb instanceof BasicManagedBundle) {
                if (!isDelta || ((BasicManagedBundle)mb).getPersistenceNeeded()) {
                    futures.add( executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            if (isDelta && !((BasicManagedBundle)mb).getPersistenceNeeded()) {
                                // someone else persisted this (race)
                                return;
                            }
                            if (!isBundleExcludedFromPersistence(mb)) {
                                persist(type.getSubPathName(), type, id + ".jar", com.google.common.io.Files.asByteSource(
                                        ((ManagementContextInternal) mgmt).getOsgiManager().get().getBundleFile(mb)), exceptionHandler);
                            }
                            ((BasicManagedBundle)mb).setPersistenceNeeded(false);
                        } }) );
                }
            }
        }
    }

    private boolean isBundleExcludedFromPersistence(ManagedBundle mb) {
        return ((ManagementContextInternal)mgmt).getOsgiManager().get().isExcludedFromPersistence(mb);
    }

    @Override
    public void waitForWritesCompleted(Duration timeout) throws InterruptedException, TimeoutException {
        boolean locked = lock.readLock().tryLock(timeout.toMillisecondsRoundingUp(), TimeUnit.MILLISECONDS);
        if (locked) {
            ImmutableSet<StoreObjectAccessorWithLock> wc;
            synchronized (writers) {
                wc = ImmutableSet.copyOf(writers.values());
            }
            lock.readLock().unlock();
            
            // Belt-and-braces: the lock above should be enough to ensure no outstanding writes, because
            // each writer is now synchronous.
            for (StoreObjectAccessorWithLock writer : wc) {
                writer.waitForCurrentWrites(timeout);
            }
        } else {
            throw new TimeoutException("Timeout waiting for writes to "+objectStore);
        }
    }

    @Override
    public boolean isWriting() {
        boolean locked;
        try {
            locked = lock.readLock().tryLock(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
        if (locked) {
            ImmutableSet<StoreObjectAccessorWithLock> wc;
            synchronized (writers) {
                wc = ImmutableSet.copyOf(writers.values());
            }
            lock.readLock().unlock();
            
            for (StoreObjectAccessorWithLock writer : wc) {
                if (writer.isWriting()) {
                    return true;
                }
            }
            
            return false;
        } else {
            return true;
        }
    }

    private String read(String subPath) {
        StoreObjectAccessor objectAccessor = objectStore.newAccessor(subPath);
        return objectAccessor.get();
    }

    private byte[] readBytes(String subPath) {
        StoreObjectAccessor objectAccessor = objectStore.newAccessor(subPath);
        return objectAccessor.getBytes();
    }

    private void persist(String subPath, Memento memento, PersistenceExceptionHandler exceptionHandler) {
        try {
            checkMementoForProblemsAndWarn(memento);
            getWriter(getPath(subPath, memento.getId())).put(getSerializerWithStandardClassLoader().toString(memento));

        } catch (Exception e) {
            exceptionHandler.onPersistMementoFailed(memento, e);
        }
    }

    private boolean isKnownNotManagedActive(BrooklynObject bo) {
        return bo!=null && ((bo instanceof Entity && !Entities.isManagedActive((Entity) bo)) || (bo instanceof Location && !Locations.isManaged((Location) bo)));
    }

    private void checkMementoForProblemsAndWarn(Memento memento) {
        // warn if there appears to be a dangling reference
        MutableList<String> dependenciesToConfirmExistence = MutableList.of();
        if (memento instanceof EntityMemento || memento instanceof LocationMemento) dependenciesToConfirmExistence.appendIfNotNull( ((TreeNode) memento).getParent() );
        // NOTE: adjuncts on entities could be relevant for warning also, but they don't persist a reference to their entity and can't so easily be looked up without it; so far this hasn't been an issue
        Maybe<BrooklynObject> me = null;
        for (String id : dependenciesToConfirmExistence) {
            BrooklynObject bo = mgmt.lookup(id);
            if (bo == null) {
                if (me == null) me = Maybe.ofAllowingNull(mgmt.lookup(memento.getId()));
                if (me.isPresentAndNonNull() && isKnownNotManagedActive(me.get())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Persistence dependency incomplete with " + memento.getType() + " " + memento.getId() + "; "+me.get()+" is being unmanaged, and dependency " + id + "(" + bo + ") is not known; likely the former will be unpersisted shortly also but persisting it for now as requested");
                    }
                } else if (me.get()==null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Persistence dependency incomplete with " + memento.getType() + " " + memento.getId() + "; "+me.get()+" is not known to mgmt context, and dependency " + id + "(" + bo + ") is not known; likely the former will be unpersisted shortly also but persisting it for now as requested");
                    }
                } else {
                    // almost definitely a problem, as the descendants should be unmanaged by the time the parent is removed altogether from lookup tables
                    LOG.warn("Persistence dependency problem with " + memento.getType() + " " + memento.getId() + "; dependency " + id + " not found when persisting the former, potential race in creation/deletion which may prevent rebind");
                }
            }

            if (isKnownNotManagedActive(bo)) {
                // common to do partial persistence when deleting a tree, so this is not worrisome
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Persistence dependency incomplete with " + memento.getType() + " " + memento.getId() + "; dependency " + id + "(" + bo + ") is being unmanaged; likely the former will be unmanaged and unpersisted shortly but persisting it for now as requested");
                }
            }
        }
    }

    private void persist(String subPath, BrooklynObjectType type, String id, String content, PersistenceExceptionHandler exceptionHandler) {
        try {
            if (content==null) LOG.warn("Null content for "+type+" "+id);
            getWriter(getPath(subPath, id)).put(content);
        } catch (Exception e) {
            exceptionHandler.onPersistRawMementoFailed(type, id, e);
        }
    }
    
    private void persist(String subPath, BrooklynObjectType type, String id, ByteSource content, PersistenceExceptionHandler exceptionHandler) {
        try {
            getWriter(getPath(subPath, id)).put(content);
        } catch (Exception e) {
            exceptionHandler.onPersistRawMementoFailed(type, id, e);
        }
    }
    
    private void delete(String subPath, String id, PersistenceExceptionHandler exceptionHandler) {
        try {
            StoreObjectAccessorWithLock w = getWriter(getPath(subPath, id));
            w.delete();
            synchronized (writers) {
                writers.remove(id);
            }
        } catch (Exception e) {
            exceptionHandler.onDeleteMementoFailed(id, e);
        }
    }

    private void updatePlaneId(String planeId, PersistenceExceptionHandler exceptionHandler) {
        try {
            if (planeId==null) {
                // can happen during initial backup creation; if happens any other time, there's a problem!
                LOG.debug("Null content for planeId; not updating at server");
                return;
            }

            String persistedPlaneId = read(PLANE_ID_FILE_NAME);
            if (persistedPlaneId == null) {
                getWriter(PLANE_ID_FILE_NAME).put(planeId);
            } else if(!persistedPlaneId.equals(planeId)) {
                throw new IllegalStateException("Persisted planeId found (" + persistedPlaneId + ") but instance planeId is different (" + planeId + ")");
            }
        } catch (Exception e) {
            exceptionHandler.onUpdatePlaneIdFailed(planeId, e);
        }
    }

    private ListenableFuture<?> asyncPersist(final String subPath, final Memento memento, final PersistenceExceptionHandler exceptionHandler) {
        return executor.submit(new Runnable() {
            @Override
            public void run() {
                persist(subPath, memento, exceptionHandler);
            }});
    }

    private ListenableFuture<?> asyncPersist(final String subPath, final BrooklynObjectType type, final String id, final String content, final PersistenceExceptionHandler exceptionHandler) {
        return executor.submit(new Runnable() {
            @Override
            public void run() {
                persist(subPath, type, id, content, exceptionHandler);
            }});
    }

    private ListenableFuture<?> asyncDelete(final String subPath, final String id, final PersistenceExceptionHandler exceptionHandler) {
        return executor.submit(new Runnable() {
            @Override
            public void run() {
                delete(subPath, id, exceptionHandler);
            }});
    }
    
    private ListenableFuture<?> asyncUpdatePlaneId(final String planeId, final PersistenceExceptionHandler exceptionHandler) {
        return executor.submit(new Runnable() {
            @Override
            public void run() {
                updatePlaneId(planeId, exceptionHandler);
            }});
    }

    private String getPath(String subPath, String id) {
        return subPath+"/"+Strings.makeValidFilename(id);
    }

    @Override
    public String getBackingStoreDescription() {
        return getObjectStore().getSummaryName();
    }

}
