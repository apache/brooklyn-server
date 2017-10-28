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
package org.apache.brooklyn.core.server;

import static org.apache.brooklyn.core.config.ConfigKeys.newStringConfigKey;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.usage.ManagementNodeStateListener;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

/** Config keys for the brooklyn server */
public class BrooklynServerConfig {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(BrooklynServerConfig.class);

    /**
     * Provided for setting; consumers should use {@link #getMgmtBaseDir(ManagementContext)}
     */
    public static final ConfigKey<String> MGMT_BASE_DIR = newStringConfigKey(
            "brooklyn.base.dir", "Directory for reading and writing all brooklyn server data", 
            Os.fromHome(".brooklyn"));
    
    @Deprecated /** @deprecated since 0.7.0 use BrooklynServerConfig routines */
    // copied here so we don't have back-ref to BrooklynConfigKeys
    public static final ConfigKey<String> BROOKLYN_DATA_DIR = newStringConfigKey(
            "brooklyn.datadir", "Directory for writing all brooklyn data");

    /**
     * Provided for setting; consumers should query the management context persistence subsystem
     * for the actual target, or use {@link BrooklynServerPaths#newMainPersistencePathResolver(ManagementContext)}
     * if trying to resolve the value
     */
    public static final ConfigKey<String> PERSISTENCE_DIR = newStringConfigKey(
        "brooklyn.persistence.dir", 
        "Directory or container name for writing persisted state");

    public static final ConfigKey<String> PERSISTENCE_LOCATION_SPEC = newStringConfigKey(
        "brooklyn.persistence.location.spec", 
        "Optional location spec string for an object store (e.g. jclouds:swift:URL) where persisted state should be kept; "
        + "if blank or not supplied, the file system is used"); 

    public static final ConfigKey<String> PERSISTENCE_BACKUPS_DIR = newStringConfigKey(
        "brooklyn.persistence.backups.dir", 
        "Directory or container name for writing backups of persisted state; "
        + "defaults to 'backups' inside the default persistence directory");
    
    public static final ConfigKey<String> PERSISTENCE_BACKUPS_LOCATION_SPEC = newStringConfigKey(
        "brooklyn.persistence.backups.location.spec", 
        "Location spec string for an object store (e.g. jclouds:swift:URL) where backups of persisted state should be kept; "
        + "defaults to the local file system");
    
    public static final ConfigKey<Boolean> PERSISTENCE_BACKUPS_REQUIRED_ON_PROMOTION =
        ConfigKeys.newBooleanConfigKey("brooklyn.persistence.backups.required.promotion",
            "Whether a backup should be made of the persisted state from the persistence location to the backup location on node promotion, "
            + "before any writes from this node", true);
    
    public static final ConfigKey<Boolean> PERSISTENCE_BACKUPS_REQUIRED_ON_DEMOTION =
        ConfigKeys.newBooleanConfigKey("brooklyn.persistence.backups.required.promotion",
            "Whether a backup of in-memory state should be made to the backup persistence location on node demotion, "
            + "in case other nodes might write conflicting state", true);

    /** @deprecated since 0.7.0, use {@link #PERSISTENCE_BACKUPS_ON_PROMOTION} and {@link #PERSISTENCE_BACKUPS_ON_DEMOTION},
     * which allow using a different target location and are supported on more environments (and now default to true) */
    @Deprecated
    public static final ConfigKey<Boolean> PERSISTENCE_BACKUPS_REQUIRED =
        ConfigKeys.newBooleanConfigKey("brooklyn.persistence.backups.required",
            "Whether a backup should always be made of the persistence directory; "
            + "if true, it will fail if this operation is not permitted (e.g. jclouds-based cloud object stores); "
            + "if false, the persistence store will be overwritten with changes (but files not removed if they are unreadable); "
            + "if null or not set, the legacy beahviour of creating backups where possible (e.g. file system) is currently used; "
            + "this key is DEPRECATED in favor of promotion and demotion specific flags now defaulting to true");

    @SuppressWarnings("serial")
    public static final ConfigKey<List<ManagementNodeStateListener>> MANAGEMENT_NODE_STATE_LISTENERS = ConfigKeys.newConfigKey(
            new TypeToken<List<ManagementNodeStateListener>>() {},
            "brooklyn.managementNodeState.listeners",
            "Optional list of ManagementNodeStateListener instances",
            ImmutableList.<ManagementNodeStateListener>of());

    public static final ConfigKey<Duration> MANAGEMENT_NODE_STATE_LISTENER_TERMINATION_TIMEOUT = ConfigKeys.newConfigKey(
            Duration.class,
            "brooklyn.managementNodeState.listeners.timeout",
            "Timeout on termination, to wait for queue of management-node-state listener events to be processed",
            Duration.TEN_SECONDS);

    public static final ConfigKey<String> BROOKLYN_CATALOG_URL = ConfigKeys.newStringConfigKey("brooklyn.catalog.url",
        "The URL of a custom catalog.bom to load");

    /** string used in places where the management node ID is needed to resolve a path */
    public static final String MANAGEMENT_NODE_ID_PROPERTY = "brooklyn.mgmt.node.id";
    
    public static final ConfigKey<Boolean> USE_OSGI = ConfigKeys.newBooleanConfigKey("brooklyn.osgi.enabled",
        "Whether OSGi is enabled, defaulting to true", true);
    public static final ConfigKey<String> OSGI_CACHE_DIR = ConfigKeys.newStringConfigKey("brooklyn.osgi.cache.dir",
        "Directory to use for OSGi cache, potentially including Freemarker template variables "
        + "${"+MGMT_BASE_DIR.getName()+"} (which is the default for relative paths), "
        + "${"+Os.TmpDirFinder.BROOKLYN_OS_TMPDIR_PROPERTY+"} if it should be in the tmp dir space,  "
        + "and ${"+MANAGEMENT_NODE_ID_PROPERTY+"} to include the management node ID (recommended if running multiple OSGi paths)",
        "osgi/cache/${"+MANAGEMENT_NODE_ID_PROPERTY+"}/");
    public static final ConfigKey<Boolean> OSGI_CACHE_CLEAN = ConfigKeys.newBooleanConfigKey("brooklyn.osgi.cache.clean",
        "Whether to delete the OSGi directory before and after use; if unset, it will delete if the node ID forms part of the cache dir path (which by default it does) to avoid file leaks");

    public static final ConfigKey<String> PERSIST_MANAGED_BUNDLE_WHITELIST_REGEX = ConfigKeys.newStringConfigKey(
            "brooklyn.persistence.bundle.whitelist",
            "Regex for bundle symbolic names explicitly allowed to be persisted (taking precedence over blacklist); "
                    + "managed bundles will by default be peristed if not blacklisted; "
                    + "they do not need to be explicitly whitelisted.",
            null);
    
    public static final ConfigKey<String> PERSIST_MANAGED_BUNDLE_BLACKLIST_REGEX = ConfigKeys.newStringConfigKey(
            "brooklyn.persistence.bundle.blacklist",
            "Regex for bundle symbolic names explicitly excluded from persistence (but whitelist takes precedence); "
                    + "if not explicitly blacklisted, managed bundles will by default be peristed",
            "org\\.apache\\.brooklyn\\..*");

    /** @see BrooklynServerPaths#getMgmtBaseDir(ManagementContext) */
    public static String getMgmtBaseDir(ManagementContext mgmt) {
        return BrooklynServerPaths.getMgmtBaseDir(mgmt);
    }
    /** @see BrooklynServerPaths#getMgmtBaseDir(ManagementContext) */
    public static String getMgmtBaseDir(StringConfigMap brooklynProperties) {
        return BrooklynServerPaths.getMgmtBaseDir(brooklynProperties);
    }
    /** @see BrooklynServerPaths#getMgmtBaseDir(ManagementContext) */
    public static String getMgmtBaseDir(Map<String,?> brooklynProperties) {
        return BrooklynServerPaths.getMgmtBaseDir(brooklynProperties);
    }
    
    /**
     * @return {@link ManagementContext#getManagementNodeUri()}, located in this utility class for convenience.
     */
    public static Maybe<URI> getBrooklynWebUri(ManagementContext mgmt) {
        return mgmt.getManagementNodeUri();
    }
    
}
