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

import java.util.Arrays;

import com.google.common.annotations.Beta;
import org.apache.brooklyn.util.javalang.Reflections;

/*
 * This provider keeps a cache of the class-renames, which is lazily populated (see {@link #cache}.
 * Calling {@link #reset()} will set this cache to null, causing it to be reloaded next time
 * it is requested.
 *
 * Loading the cache involves iterating over the {@link #loaders}, returning the union of
 * the results from {@link Loader#load()}.
 *
 * Initially, the only loader is the basic {@link DeserializingClassRenamesProvider}.
 *
 * However, when running in karaf the {@link OsgiConfigLoader} will be instantiated and added.
 * See karaf/init/src/main/resources/OSGI-INF/blueprint/blueprint.xml
 */
@Beta
public class DeserializingClassRenamesProvider extends DeserializingProvider{

    private static final String DESERIALIZING_CLASS_RENAMES_PROPERTIES_PATH = "classpath://org/apache/brooklyn/core/mgmt/persist/deserializingClassRenames.properties";

    public static final DeserializingClassRenamesProvider INSTANCE = new DeserializingClassRenamesProvider();

    private DeserializingClassRenamesProvider(){
        super(Arrays.asList(new ConfigLoader[]{
                new PropertiesConfigLoader(DESERIALIZING_CLASS_RENAMES_PROPERTIES_PATH)
        }));
    }

    /**
     * Handles inner classes, where the outer class has been renamed. For example:
     *
     * {@code findMappedName("com.example.MyFoo$MySub")} will return {@code com.example.renamed.MyFoo$MySub}, if
     * the renamed contains {@code com.example.MyFoo: com.example.renamed.MyFoo}.
     */
    @Beta
    public String findMappedName(String name) {
        return Reflections.findMappedNameAndLog(loadDeserializingMapping(), name);
    }

}
