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

import org.apache.brooklyn.core.mgmt.rebind.dto.MementosGenerators;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.osgi.framework.Bundle;

import com.google.common.base.Optional;

/**
 * For serialize/deserialize of persisted state, we need to prefix classes with their OSGi bundle 
 * (so we know which classloader to use when deserializing). In most cases, this can be done when
 * serializing the instances. However, in some cases the field of a memento is the String representation  
 * of the type (e.g. see (@link {@link org.apache.brooklyn.core.mgmt.rebind.dto.AbstractMemento#getType()}).
 * In that situation, the code writing the field must include the prefix (e.g. see
 * {@link MementosGenerators}.
 */
public class OsgiClassPrefixer {

    private final ClassLoaderUtils whiteListRetriever;
    
    public OsgiClassPrefixer() {
        whiteListRetriever = new ClassLoaderUtils(getClass());
    }
    
    public Optional<String> getPrefix(Class<?> type) {
        Optional<Bundle> bundle  = Osgis.getBundleOf(type);
        if (bundle.isPresent() && !whiteListRetriever.isBundleWhiteListed(bundle.get())) {
            return Optional.of(bundle.get().getSymbolicName() + ":");
        }
        return Optional.absent();
    }
    
    public Optional<String> stripMatchingPrefix(Bundle bundle, String type) {
        String symbolicName = bundle.getSymbolicName();
        if (symbolicName != null && type.startsWith(symbolicName + ":")) {
            return Optional.of(type.substring((symbolicName + ":").length()));
        }
        return Optional.absent();
    }
}
