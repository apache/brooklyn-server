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
package org.apache.brooklyn.core.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.StringFunctions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.CharMatcher;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import groovy.lang.Closure;

/**
 * TODO methods in this class are not thread safe.
 * intention is that they are set during startup and not modified thereafter.
 */
@SuppressWarnings("rawtypes")
public class BrooklynPropertiesImpl implements BrooklynProperties {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynPropertiesImpl.class);

    public static class Factory {
        /** creates a new empty {@link BrooklynPropertiesImpl} */
        public static BrooklynPropertiesImpl newEmpty() {
            return new BrooklynPropertiesImpl();
        }

        /** creates a new {@link BrooklynPropertiesImpl} with contents loaded 
         * from the usual places, including *.properties files and environment variables */
        public static BrooklynPropertiesImpl newDefault() {
            return new Builder(true).build();
        }

        public static Builder builderDefault() {
            return new Builder(true);
        }

        public static Builder builderEmpty() {
            return new Builder(false);
        }

        public static class Builder {
            private String defaultLocationMetadataUrl;
            private String globalLocationMetadataFile = null;
            private String globalPropertiesFile = null;
            private String localPropertiesFile = null;
            private BrooklynPropertiesImpl originalProperties = null;

            /** @deprecated since 0.7.0 use static methods in {@link Factory} to create */
            @Deprecated
            public Builder() {
                this(true);
            }

            private Builder(boolean setGlobalFileDefaults) {
                resetDefaultLocationMetadataUrl();
                if (setGlobalFileDefaults) {
                    resetGlobalFiles();
                }
            }

            public Builder resetDefaultLocationMetadataUrl() {
                defaultLocationMetadataUrl = "classpath://brooklyn/location-metadata.properties";
                return this;
            }
            public Builder resetGlobalFiles() {
                defaultLocationMetadataUrl = "classpath://brooklyn/location-metadata.properties";
                globalLocationMetadataFile = Os.mergePaths(Os.home(), ".brooklyn", "location-metadata.properties");
                globalPropertiesFile = Os.mergePaths(Os.home(), ".brooklyn", "brooklyn.properties");
                return this;
            }

            /**
             * Creates a Builder that when built, will return the BrooklynProperties passed to this constructor
             */
            private Builder(BrooklynPropertiesImpl originalProperties) {
                this.originalProperties = new BrooklynPropertiesImpl().addFromMap(originalProperties.asMapWithStringKeys());
            }

            /**
             * The URL of a default location-metadata.properties (for meta-data about different locations, such as iso3166 and global lat/lon). 
             * Defaults to classpath://brooklyn/location-metadata.properties
             */
            public Builder defaultLocationMetadataUrl(String val) {
                defaultLocationMetadataUrl = checkNotNull(val, "file");
                return this;
            }

            /**
             * The URL of a location-metadata.properties file that appends to and overwrites values in the locationMetadataUrl. 
             * Defaults to ~/.brooklyn/location-metadata.properties
             */
            public Builder globalLocationMetadataFile(String val) {
                globalLocationMetadataFile = checkNotNull(val, "file");
                return this;
            }

            /**
             * The URL of a shared brooklyn.properties file. Defaults to ~/.brooklyn/brooklyn.properties.
             * Can be null to disable.
             */
            public Builder globalPropertiesFile(String val) {
                globalPropertiesFile = val;
                return this;
            }

            @Beta
            public boolean hasDelegateOriginalProperties() {
                return this.originalProperties==null;
            }

            /**
             * The URL of a brooklyn.properties file specific to this launch. Appends to and overwrites values in globalPropertiesFile.
             */
            public Builder localPropertiesFile(String val) {
                localPropertiesFile = val;
                return this;
            }

            public BrooklynPropertiesImpl build() {
                if (originalProperties != null) {
                    return new BrooklynPropertiesImpl().addFromMap(originalProperties.asMapWithStringKeys());
                }

                BrooklynPropertiesImpl properties = new BrooklynPropertiesImpl();

                // TODO Could also read from http://brooklyn.io, for up-to-date values?
                // But might that make unit tests run very badly when developer is offline?
                addPropertiesFromUrl(properties, defaultLocationMetadataUrl, false);

                addPropertiesFromFile(properties, globalLocationMetadataFile);
                addPropertiesFromFile(properties, globalPropertiesFile);
                addPropertiesFromFile(properties, localPropertiesFile);

                properties.addEnvironmentVars();
                properties.addSystemProperties();

                return properties;
            }

            public static Builder fromProperties(BrooklynPropertiesImpl brooklynProperties) {
                return new Builder(brooklynProperties);
            }

            @Override
            @SuppressWarnings("deprecation")
            public String toString() {
                return MoreObjects.toStringHelper(this)
                        .omitNullValues()
                        .add("originalProperties", originalProperties)
                        .add("defaultLocationMetadataUrl", defaultLocationMetadataUrl)
                        .add("globalLocationMetadataUrl", globalLocationMetadataFile)
                        .add("globalPropertiesFile", globalPropertiesFile)
                        .add("localPropertiesFile", localPropertiesFile)
                        .toString();
            }
        }

        private static void addPropertiesFromUrl(BrooklynPropertiesImpl p, String url, boolean warnIfNotFound) {
            if (url==null) return;

            try {
                p.addFrom(ResourceUtils.create(BrooklynPropertiesImpl.class).getResourceFromUrl(url));
            } catch (Exception e) {
                if (warnIfNotFound)
                    LOG.warn("Could not load {}; continuing", url);
                if (LOG.isTraceEnabled()) LOG.trace("Could not load "+url+"; continuing", e);
            }
        }

        private static void addPropertiesFromFile(BrooklynPropertiesImpl p, String file) {
            if (file==null) return;

            String fileTidied = Os.tidyPath(file);
            File f = new File(fileTidied);

            if (f.exists()) {
                p.addFrom(f);
            }
        }
    }

    /**
     * The actual properties.
     * <p>
     * Care must be taken accessing/modifying these, to ensure it is correctly synchronized.
     */
    private final Map<String, Object> contents = new LinkedHashMap<>();
    
    protected BrooklynPropertiesImpl() {
    }

    @Override
    public BrooklynPropertiesImpl addEnvironmentVars() {
        addFrom(System.getenv());
        return this;
    }

    @Override
    public BrooklynPropertiesImpl addSystemProperties() {
        addFrom(System.getProperties());
        return this;
    }

    @Override
    public BrooklynPropertiesImpl addFrom(ConfigBag cfg) {
        addFrom(cfg.getAllConfig());
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BrooklynPropertiesImpl addFrom(Map map) {
        putAll(Maps.transformValues(map, StringFunctions.trim()));
        return this;
    }

    @Override
    public BrooklynPropertiesImpl addFrom(InputStream i) {
        // Ugly way to load them in order, but Properties is a Hashtable so loses order otherwise.
        @SuppressWarnings({ "serial" })
        Properties p = new Properties() {
            @Override
            public synchronized Object put(Object key, Object value) {
                // Trim the string values to remove leading and trailing spaces
                String s = (String) value;
                if (Strings.isBlank(s)) {
                    s = Strings.EMPTY;
                } else {
                    s = CharMatcher.BREAKING_WHITESPACE.trimFrom(s);
                }
                return BrooklynPropertiesImpl.this.put(key, s);
            }
        };
        try {
            p.load(i);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return this;
    }
    
    @Override
    public BrooklynPropertiesImpl addFrom(File f) {
        if (!f.exists()) {
            LOG.warn("Unable to find file '"+f.getAbsolutePath()+"' when loading properties; ignoring");
            return this;
        } else {
            try {
                return addFrom(new FileInputStream(f));
            } catch (FileNotFoundException e) {
                throw Throwables.propagate(e);
            }
        }
    }
    @Override
    public BrooklynPropertiesImpl addFrom(URL u) {
        try {
            return addFrom(u.openStream());
        } catch (IOException e) {
            throw new RuntimeException("Error reading properties from "+u+": "+e, e);
        }
    }
    /**
     * @see ResourceUtils#getResourceFromUrl(String)
     *
     * of the form form file:///home/... or http:// or classpath://xx ;
     * for convenience if not starting with xxx: it is treated as a classpath reference or a file;
     * throws if not found (but does nothing if argument is null)
     */
    @Override
    public BrooklynPropertiesImpl addFromUrl(String url) {
        try {
            if (url==null) return this;
            return addFrom(ResourceUtils.create(this).getResourceFromUrl(url));
        } catch (Exception e) {
            throw new RuntimeException("Error reading properties from "+url+": "+e, e);
        }
    }

    /** expects a property already set in scope, whose value is acceptable to {@link #addFromUrl(String)};
     * if property not set, does nothing */
    @Override
    public BrooklynPropertiesImpl addFromUrlProperty(String urlProperty) {
        String url = (String) get(urlProperty);
        if (url==null) addFromUrl(url);
        return this;
    }

    /**
     * adds the indicated properties
     */
    @Override
    public BrooklynPropertiesImpl addFromMap(Map properties) {
        putAll(properties);
        return this;
    }

    /** inserts the value under the given key, if it was not present */
    @Override
    public boolean putIfAbsent(String key, Object value) {
        if (containsKey(key)) return false;
        put(key, value);
        return true;
    }

    /** @deprecated attempts to call get with this syntax are probably mistakes; get(key, defaultValue) is fine but
     * Map is unlikely the key, much more likely they meant getFirst(flags, key).
     */
    @Override
    @Deprecated
    public String get(Map flags, String key) {
        LOG.warn("Discouraged use of 'BrooklynProperties.get(Map,String)' (ambiguous); use getFirst(Map,String) or get(String) -- assuming the former");
        LOG.debug("Trace for discouraged use of 'BrooklynProperties.get(Map,String)'",
                new Throwable("Arguments: "+flags+" "+key));
        return getFirst(flags, key);
    }

    /** returns the value of the first key which is defined
     * <p>
     * takes the following flags:
     * 'warnIfNone', 'failIfNone' (both taking a boolean (to use default message) or a string (which is the message));
     * and 'defaultIfNone' (a default value to return if there is no such property); defaults to no warning and null response */
    @Override
    public String getFirst(String ...keys) {
        return getFirst(MutableMap.of(), keys);
    }
    @Override
    public String getFirst(Map flags, String ...keys) {
        for (String k: keys) {
            if (k!=null && containsKey(k)) return (String) get(k);
        }
        if (flags.get("warnIfNone")!=null && !Boolean.FALSE.equals(flags.get("warnIfNone"))) {
            if (Boolean.TRUE.equals(flags.get("warnIfNone")))
                LOG.warn("Unable to find Brooklyn property "+keys);
            else
                LOG.warn(""+flags.get("warnIfNone"));
        }
        if (flags.get("failIfNone")!=null && !Boolean.FALSE.equals(flags.get("failIfNone"))) {
            Object f = flags.get("failIfNone");
            if (f instanceof Closure) {
                LOG.warn("Use of groovy.lang.Closure is deprecated as value for 'failIfNone', in BrooklynProperties.getFirst()");
                ((Closure)f).call((Object[])keys);
            }
            if (Boolean.TRUE.equals(f))
                throw new NoSuchElementException("Brooklyn unable to find mandatory property "+keys[0]+
                        (keys.length>1 ? " (or "+(keys.length-1)+" other possible names, full list is "+Arrays.asList(keys)+")" : "") );
            else
                throw new NoSuchElementException(""+f);
        }
        if (flags.get("defaultIfNone")!=null) {
            return (String) flags.get("defaultIfNone");
        }
        return null;
    }

    @Override
    public String toString() {
        return "BrooklynProperties["+size()+"]";
    }

    /** like normal map.put, except config keys are dereferenced on the way in */
    @Override
    public Object put(Object rawKey, Object value) {
        String key;
        if (rawKey == null) {
            throw new NullPointerException("Null key not permitted in BrooklynProperties");
        } else if (rawKey instanceof String) {
            key = (String) rawKey;
        } else if (rawKey instanceof CharSequence) {
            key = rawKey.toString();
        } else if (rawKey instanceof HasConfigKey) {
            key = ((HasConfigKey)rawKey).getConfigKey().getName();
        } else if (rawKey instanceof ConfigKey) {
            key = ((ConfigKey)rawKey).getName();
        } else {
            throw new IllegalArgumentException("Invalid key (value='" + rawKey + "', type=" + rawKey.getClass().getName() + ") for BrooklynProperties");
        }
        return putImpl(key, value);
    }

    /** like normal map.putAll, except config keys are dereferenced on the way in */
    @Override
    public void putAll(Map vals) {
        for (Map.Entry<?,?> entry : ((Map<?,?>)vals).entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public <T> Object put(HasConfigKey<T> key, T value) {
        return putImpl(key.getConfigKey().getName(), value);
    }

    @Override
    public <T> Object put(ConfigKey<T> key, T value) {
        return putImpl(key.getName(), value);
    }
    
    @Override
    public <T> boolean putIfAbsent(ConfigKey<T> key, T value) {
        return putIfAbsent(key.getName(), value);
    }

    @Override
    public Object getConfig(String key) {
        return get(key);
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        return getConfig(key, null);
    }

    @Override
    public <T> T getConfig(HasConfigKey<T> key) {
        return getConfig(key.getConfigKey(), null);
    }

    @Override
    public <T> T getConfig(HasConfigKey<T> key, T defaultValue) {
        return getConfig(key.getConfigKey(), defaultValue);
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key, T defaultValue) {
        // TODO does not support MapConfigKey etc where entries use subkey notation; for now, access using submap
        if (!containsKey(key.getName())) {
            if (defaultValue!=null) return defaultValue;
            return key.getDefaultValue();
        }
        Object value = get(key.getName());
        if (value==null) return null;
        // no evaluation / key extraction here
        return TypeCoercions.coerce(value, key.getTypeToken());
    }

    @Override
    public Maybe<Object> getConfigRaw(ConfigKey<?> key) {
        if (containsKey(key.getName())) return Maybe.of(get(key.getName()));
        return Maybe.absent();
    }

    @Override
    public Maybe<Object> getConfigRaw(ConfigKey<?> key, boolean includeInherited) {
        return getConfigRaw(key);
    }

    @Override
    public Maybe<Object> getConfigLocalRaw(ConfigKey<?> key) {
        return getConfigRaw(key);
    }

    @Override
    public Map<ConfigKey<?>,Object> getAllConfigLocalRaw() {
        Map<ConfigKey<?>, Object> result = new LinkedHashMap<>();
        synchronized (contents) {
            for (Map.Entry<String, Object> entry : contents.entrySet()) {
                result.put(new BasicConfigKey<Object>(Object.class, entry.getKey()), entry.getValue());
            }
        }
        return result;
    }

    @Override @Deprecated
    public Map<ConfigKey<?>, Object> getAllConfig() {
        return getAllConfigLocalRaw();
    }

    @Override @Deprecated
    public Set<ConfigKey<?>> findKeys(Predicate<? super ConfigKey<?>> filter) {
        return findKeysDeclared(filter);
    }

    @Override
    public Set<ConfigKey<?>> findKeysDeclared(Predicate<? super ConfigKey<?>> filter) {
        Set<ConfigKey<?>> result = new LinkedHashSet<ConfigKey<?>>();
        synchronized (contents) {
            for (Object entry: contents.entrySet()) {
                ConfigKey<?> k = new BasicConfigKey<Object>(Object.class, ""+((Map.Entry)entry).getKey());
                if (filter.apply(k)) {
                    result.add(k);
                }
            }
        }
        return result;
    }

     @Override                                                                                                                                
    public Set<ConfigKey<?>> findKeysPresent(Predicate<? super ConfigKey<?>> filter) {                                                       
        return findKeysDeclared(filter);                                                                                                     
    } 

    @Override
    public BrooklynProperties submap(Predicate<ConfigKey<?>> filter) {
        BrooklynPropertiesImpl result = Factory.newEmpty();
        synchronized (contents) {
            for (Map.Entry<String, Object> entry : contents.entrySet()) {
                ConfigKey<?> k = new BasicConfigKey<Object>(Object.class, entry.getKey());
                if (filter.apply(k)) {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }

    @Override
    public BrooklynProperties submapByName(Predicate<? super String> filter) {
        BrooklynPropertiesImpl result = Factory.newEmpty();
        synchronized (contents) {
            for (Map.Entry<String, Object> entry : contents.entrySet()) {
                if (filter.apply(entry.getKey())) {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, Object> asMapWithStringKeys() {
        synchronized (contents) {
            return MutableMap.copyOf(contents).asUnmodifiable();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (contents) {
            return contents.isEmpty();
        }
    }
    
    @Override
    public int size() {
        synchronized (contents) {
            return contents.size();
        }
    }
    
    @Override
    public boolean containsKey(String key) {
        synchronized (contents) {
            return contents.containsKey(key);
        }
    }

    @Override
    public boolean containsKey(ConfigKey<?> key) {
        return containsKey(key.getName());
    }
    
    @Override
    public boolean remove(String key) {
        synchronized (contents) {
            boolean result = contents.containsKey(key);
            contents.remove(key);
            return result;
        }
    }
    
    @Override
    public boolean remove(ConfigKey<?> key) {
        return remove(key.getName());
    }
    
    protected Object get(String key) {
        synchronized (contents) {
            return contents.get(key);
        }
    }
    
    protected Object putImpl(String key, Object value) {
        synchronized (contents) {
            return contents.put(key, value);
        }
    }
}
