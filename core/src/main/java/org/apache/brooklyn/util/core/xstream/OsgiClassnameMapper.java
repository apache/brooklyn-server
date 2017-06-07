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
package org.apache.brooklyn.util.core.xstream;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.mapper.CannotResolveClassException;
import com.thoughtworks.xstream.mapper.MapperWrapper;

/** Attaches a prefix to _all_ classes written out.
 * Ensures the xstream class loader is used to read 
 * (because that's where we set the OSGi-prefix-aware code).
 * <p>
 * We also have the context in that loader so if we wanted to optimize
 * we could scan that for bundles and suppress bundles if it's in scope.
 * However if we plan to move to referring to RegisteredTypes for anything
 * serialized that's irrelevant.
 * <p>
 * We could have code that uses the search path from that loader
 * to prefers types in local bundles, ignoring the bundle name
 * if the class is found there (either always, or just if the bundle is not found / deprecated).
 */
// TODO above, and also see discussion at https://github.com/apache/brooklyn-server/pull/718
public class OsgiClassnameMapper extends MapperWrapper {
    private final OsgiClassPrefixer prefixer;
    private final Supplier<XStream> xstream;
    
    public OsgiClassnameMapper(Supplier<XStream> xstream, MapperWrapper mapper) {
        super(mapper);
        this.xstream = xstream;
        prefixer = new OsgiClassPrefixer();
    }
    
    @Override
    public String serializedClass(@SuppressWarnings("rawtypes") Class type) {
        // TODO What if previous stages have already renamed it?
        // For example the "outer class renaming stuff"?!
        String superResult = super.serializedClass(type);
        if (type != null && type.getName().equals(superResult)) {
            Optional<String> prefix = prefixer.getPrefix(type);
            if (prefix.isPresent()) {
                return prefix.get() + superResult;
            }
        }
        return superResult;
    }
    
    @Override
    @SuppressWarnings("rawtypes") 
    public Class realClass(String elementName) {
        CannotResolveClassException tothrow;
        try {
            return super.realClass(elementName);
        } catch (CannotResolveClassException e) {
            tothrow = e;
        }

        // Class.forName(elementName, false, classLader) does not seem to like us, returned a 
        // class whose name does not match that passed in. Therefore fallback to using loadClass.
        try {
            return xstream.get().getClassLoaderReference().getReference().loadClass(elementName);
        } catch (ClassNotFoundException e) {
            throw new CannotResolveClassException(elementName + " via super realClass (nested exception) or xstream reference classloader loadClass (class not found)", tothrow);
        }
    }
}