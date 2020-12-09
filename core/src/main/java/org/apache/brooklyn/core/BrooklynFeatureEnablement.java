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

import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.util.core.internal.ssh.ShellTool;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

/**
 * For enabling/disabling experimental features.
 * Feature enablement can be set in a number of ways, with the following precedence (most important first):
 * <ol>
 *   <li>Explicit call to {@link #setEnablement(String, boolean)} (or to {@link #enable(String)} or {@link #disable(String)})).
 *   <li>Java system properties
 *   <li>Brooklyn properties (passed using {@link #init(BrooklynProperties)})
 *   <li>Defaults set explicitly by calling {@link #setDefault(String, boolean)}
 *   <li>Hard-coded defaults
 * </ol>
 * <p>
 * For example, start Brooklyn with {@code -Dbrooklyn.executionManager.renameThreads=true}
 */
@Beta
public class BrooklynFeatureEnablement {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynFeatureEnablement.class);

    public static final String FEATURE_PROPERTY_PREFIX = "brooklyn.experimental.feature";
    
    public static final String FEATURE_POLICY_PERSISTENCE_PROPERTY = FEATURE_PROPERTY_PREFIX+".policyPersistence";
    
    public static final String FEATURE_ENRICHER_PERSISTENCE_PROPERTY = FEATURE_PROPERTY_PREFIX+".enricherPersistence";

    public static final String FEATURE_FEED_PERSISTENCE_PROPERTY = FEATURE_PROPERTY_PREFIX+".feedPersistence";
    
    /**
     * When persisting an entity that changes, whether to persist its adjuncts and locations 
     * (i.e. its enrichers, policies, feeds and locations).
     * 
     * This was previously the default behaviour, which meant that (legacy) java-based Brooklyn objects
     * (e.g. entities, locations, policies, enrichers or feeds) could get away with bad practices and 
     * still be persisted. For example, they could change 'state' without telling the listener, and hope
     * that the entity they were attached to would soon persist (thus piggy-backing off it).
     */
    public static final String FEATURE_REFERENCED_OBJECTS_REPERSISTENCE_PROPERTY = FEATURE_PROPERTY_PREFIX+".referencedObjectsRepersistence";
    
    /** whether feeds are automatically registered when set on entities, so that they are persisted */
    public static final String FEATURE_FEED_REGISTRATION_PROPERTY = FEATURE_PROPERTY_PREFIX+".feedRegistration";

    public static final String FEATURE_CORS_CXF_PROPERTY = FEATURE_PROPERTY_PREFIX + ".corsCxfFeature";

    public static final String FEATURE_BUNDLE_PERSISTENCE_PROPERTY = FEATURE_PROPERTY_PREFIX+".bundlePersistence";
    public static final String FEATURE_CATALOG_PERSISTENCE_PROPERTY = FEATURE_PROPERTY_PREFIX+".catalogPersistence";
    
    /** whether the default standby mode is {@link HighAvailabilityMode#HOT_STANDBY} or falling back to the traditional
     * {@link HighAvailabilityMode#STANDBY} */
    public static final String FEATURE_DEFAULT_STANDBY_IS_HOT_PROPERTY = FEATURE_PROPERTY_PREFIX+".defaultStandbyIsHot";

    /** whether $brooklyn:entitySpec blocks persist a {@link DeferredSupplier} containing YAML or the resolved {@link EntitySpec}; 
     * the former allows upgrades and deferred resolution of other flags, whereas the latter is the traditional 
     * behaviour (prior to 2017-11) which resolves the {@link EntitySpec} earlier and persists that */  
    public static final String FEATURE_PERSIST_ENTITY_SPEC_AS_SUPPLIER = FEATURE_PROPERTY_PREFIX+".persist.entitySpecAsSupplier";

    /**
     * Renaming threads can really helps with debugging etc; however it's a massive performance hit (2x)
     * <p>
     * We get 55000 tasks per sec with this off, 28k/s with this on.
     * <p>
     * Defaults to false if system property is not set.
     */
    public static final String FEATURE_RENAME_THREADS = "brooklyn.executionManager.renameThreads";

    /**
     * Add a jitter to the startup of tasks for testing concurrency code.
     * Use {@code brooklyn.executionManager.jitterThreads.maxDelay} to tune the maximum time task
     * startup gets delayed in milliseconds. The actual time will be a random value between [0, maxDelay).
     * Default is 200 milliseconds.
     */
    public static final String FEATURE_JITTER_THREADS = "brooklyn.executionManager.jitterThreads";

    /**
     * When rebinding to state created from very old versions, the catalogItemId properties will be missing which
     * results in errors when OSGi bundles are used. When enabled the code tries to infer the catalogItemId from
     * <ul>
     *   <li> parent entities
     *   <li> catalog items matching the type that needs to be deserialized
     *   <li> iterating through all catalog items and checking if they can provide the needed type
     * </ul>
     */
    public static final String FEATURE_BACKWARDS_COMPATIBILITY_INFER_CATALOG_ITEM_ON_REBIND = "brooklyn.backwardCompatibility.feature.inferCatalogItemOnRebind";
    
    /**
     * When rebinding, an entity could reference a catalog item that no longer exists. This option 
     * will automatically update the catalog item reference to what is inferred as the most 
     * suitable catalog symbolicName:version.
     */
    public static final String FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND = "brooklyn.quickfix.fixDanglingCatalogItemOnRebind";
    
    /**
     * When executing over ssh, whether to support the "async exec" approach, or only the classic approach.
     * 
     * If this feature is disabled, then even if the {@link ShellTool#PROP_EXEC_ASYNC} is configured it
     * will still use the classic ssh approach.
     */
    public static final String FEATURE_SSH_ASYNC_EXEC = FEATURE_PROPERTY_PREFIX+".ssh.asyncExec";

    public static final String FEATURE_VALIDATE_LOCATION_SSH_KEYS = "brooklyn.validate.locationSshKeys";
    
    /**
     * In previous versions, reparenting was not allowed. This feature restores that behaviour.
     */
    public static final String FEATURE_DISALLOW_REPARENTING = "brooklyn.disallowReparenting";

    /**
     * Values explicitly set by Java calls.
     */
    private static final Map<String, Boolean> FEATURE_ENABLEMENTS = Maps.newLinkedHashMap();

    /**
     * Values set from brooklyn.properties
     */
    private static final Map<String, Boolean> FEATURE_ENABLEMENTS_PROPERTIES = Maps.newLinkedHashMap();

    /**
     * Defaults (e.g. set by the static block's call to {@link #setDefaults()}, or by downstream projects
     * calling {@link #setDefault(String, boolean)}).
     */
    private static final Map<String, Boolean> FEATURE_ENABLEMENT_DEFAULTS = Maps.newLinkedHashMap();

    private static final Object MUTEX = new Object();

    static void setDefaults() {
        // Idea is here one can put experimental features that are *enabled* by default, but 
        // that can be turned off via system properties, or vice versa.
        // Typically this is useful where a feature is deemed risky!
        
        setDefault(FEATURE_POLICY_PERSISTENCE_PROPERTY, true);
        setDefault(FEATURE_ENRICHER_PERSISTENCE_PROPERTY, true);
        setDefault(FEATURE_FEED_PERSISTENCE_PROPERTY, true);
        setDefault(FEATURE_FEED_REGISTRATION_PROPERTY, false);
        setDefault(FEATURE_BUNDLE_PERSISTENCE_PROPERTY, true);
        setDefault(FEATURE_CATALOG_PERSISTENCE_PROPERTY, true);
        setDefault(FEATURE_REFERENCED_OBJECTS_REPERSISTENCE_PROPERTY, false);
        setDefault(FEATURE_DEFAULT_STANDBY_IS_HOT_PROPERTY, false);
        setDefault(FEATURE_PERSIST_ENTITY_SPEC_AS_SUPPLIER, true);
        setDefault(FEATURE_RENAME_THREADS, false);
        setDefault(FEATURE_JITTER_THREADS, false);
        setDefault(FEATURE_BACKWARDS_COMPATIBILITY_INFER_CATALOG_ITEM_ON_REBIND, false);
        setDefault(FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND, false);
        setDefault(FEATURE_SSH_ASYNC_EXEC, false);
        setDefault(FEATURE_VALIDATE_LOCATION_SSH_KEYS, true);
    }
    
    static {
        setDefaults();
    }
    
    public static boolean isEnabled(String property) {
        synchronized (MUTEX) {
            if (FEATURE_ENABLEMENTS.containsKey(property)) {
                return FEATURE_ENABLEMENTS.get(property);
            } else if (System.getProperty(property) != null) {
                String rawVal = System.getProperty(property);
                return Boolean.parseBoolean(rawVal);
            } else if (FEATURE_ENABLEMENTS_PROPERTIES.containsKey(property)) {
                return FEATURE_ENABLEMENTS_PROPERTIES.get(property);
            } else if (FEATURE_ENABLEMENT_DEFAULTS.containsKey(property)) {
                return FEATURE_ENABLEMENT_DEFAULTS.get(property);
            } else {
                return false;
            }
        }
    }

    /**
     * Initialises the feature-enablement from brooklyn properties. For each
     * property, prefer a system-property if present; otherwise use the value 
     * from brooklyn properties.
     */
    public static void init(BrooklynProperties props) {
        boolean found = false;
        synchronized (MUTEX) {
            for (Map.Entry<String, Object> entry : props.asMapWithStringKeys().entrySet()) {
                String property = entry.getKey();
                if (property.startsWith(FEATURE_PROPERTY_PREFIX)) {
                    found = true;
                    Boolean oldVal = isEnabled(property);
                    boolean val = Boolean.parseBoolean(""+entry.getValue());
                    FEATURE_ENABLEMENTS_PROPERTIES.put(property, val);
                    Boolean newVal = isEnabled(property);
                    
                    if (Objects.equal(oldVal, newVal)) {
                        LOG.debug("Enablement of "+property+" set to "+val+" from brooklyn properties (no-op as continues to resolve to "+oldVal+")");
                    } else {
                        LOG.debug("Enablement of "+property+" set to "+val+" from brooklyn properties (resolved value previously "+oldVal+")");
                    }
                }
            }
            if (!found) {
                LOG.debug("Init feature enablement did nothing, as no settings in brooklyn properties");
            }
        }
    }
    
    public static boolean enable(String property) {
        return setEnablement(property, true);
    }
    
    public static boolean disable(String property) {
        return setEnablement(property, false);
    }
    
    public static boolean setEnablement(String property, boolean val) {
        synchronized (MUTEX) {
            boolean oldVal = isEnabled(property);
            FEATURE_ENABLEMENTS.put(property, val);
            
            if (val == oldVal) {
                LOG.debug("Enablement of "+property+" set to explicit "+val+" (no-op as resolved to same value previously)");
            } else {
                LOG.debug("Enablement of "+property+" set to explicit "+val+" (previously resolved to "+oldVal+")");
            }
            return oldVal;
        }
    }
    
    public static void setDefault(String property, boolean val) {
        synchronized (MUTEX) {
            Boolean oldDefaultVal = FEATURE_ENABLEMENT_DEFAULTS.get(property);
            FEATURE_ENABLEMENT_DEFAULTS.put(property, val);
            if (oldDefaultVal != null) {
                if (oldDefaultVal.equals(val)) {
                    LOG.debug("Default enablement of "+property+" set to "+val+" (no-op as same as previous default value)");
                } else {
                    LOG.debug("Default enablement of "+property+" set to "+val+" (overwriting previous default of "+oldDefaultVal+")");
                }
            } else {
                LOG.debug("Default enablement of "+property+" set to "+val);
            }
        }
    }

    @VisibleForTesting
    public static void clearCache() {
        synchronized (MUTEX) {
            FEATURE_ENABLEMENTS.clear();
            FEATURE_ENABLEMENTS_PROPERTIES.clear();
            FEATURE_ENABLEMENT_DEFAULTS.clear();
            setDefaults();
        }
    }
}
