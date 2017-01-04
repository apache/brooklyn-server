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

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.javalang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;

/*
 * This provider keeps a cache of the class-renames, which is lazily populated (see {@link #cache}.
 * Calling {@link #reset()} will set this cache to null, causing it to be reloaded next time
 * it is requested.
 *
 * Loading the cache involves iterating over the {@link #loaders}, returning the union of
 * the results from {@link Loader#load()}.
 *
 * Initially, the only loader is the basic {@link ClasspathConfigLoader}.
 *
 * However, when running in karaf the {@link OsgiConfigLoader} will be instantiated and added.
 * See karaf/init/src/main/resources/OSGI-INF/blueprint/blueprint.xml
 */
@Beta
public class DeserializingClassRenamesProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DeserializingClassRenamesProvider.class);

    private static final List<ConfigLoader> loaders = Lists.newCopyOnWriteArrayList();
    static {
        loaders.add(new ClasspathConfigLoader());
    }
    
    private static volatile Map<String, String> cache;

    public static List<ConfigLoader> getLoaders() {
        return loaders;
    }

    @Beta
    public static Map<String, String> loadDeserializingClassRenames() {
        synchronized (DeserializingClassRenamesProvider.class) {
            if (cache == null) {
                MutableMap.Builder<String, String> builder = MutableMap.<String, String>builder();
                for (ConfigLoader loader : loaders) {
                    builder.putAll(loader.load());
                }
                cache = builder.build();
                LOG.info("Class-renames cache loaded, size {}", cache.size());
            }
            return cache;
        }
    }

    /**
     * Handles inner classes, where the outer class has been renamed. For example:
     * 
     * {@code findMappedName("com.example.MyFoo$MySub")} will return {@code com.example.renamed.MyFoo$MySub}, if
     * the renamed contains {@code com.example.MyFoo: com.example.renamed.MyFoo}.
     */
    @Beta
    public static String findMappedName(String name) {
        return Reflections.findMappedNameAndLog(DeserializingClassRenamesProvider.loadDeserializingClassRenames(), name);
    }

    public static void reset() {
        synchronized (DeserializingClassRenamesProvider.class) {
            cache = null;
        }
    }
}
