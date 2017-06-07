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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.thoughtworks.xstream.mapper.CannotResolveClassException;
import com.thoughtworks.xstream.mapper.Mapper;
import com.thoughtworks.xstream.mapper.MapperWrapper;

/**
 * An xstream mapper that handles class-renames, so we can rebind to historic persisted state.
 */
public class ClassRenamingMapper extends MapperWrapper {
    
    /*
     * TODO There is a strange relationship between this and OsgiClassnameMapper.
     * Should these be perhaps merged?
     * 
     * TODO For class-loading on deserialization, should we push the class-rename logic into 
     * org.apache.brooklyn.util.core.ClassLoaderUtils instead? Does the xstream mapper do
     * anything else important, beyond that class-loading responsibility? It's registration
     * in XmlSerializer makes it look a bit scary: wrapMapperForAllLowLevelMentions().
     * 
     * ---
     * TODO This code feels overly complicated, and deserves a cleanup.
     * 
     * The aim is to handle two use-cases in the deserializingClassRenames.properties:
     * 
     *  1. A very explicit rename that includes bundle prefixes (e.g. so as to limit scope, or to support 
     *     moving a class from one bundle to another).
     *  
     *  2. Just the class-rename (e.g. `com.acme.Foo: com.acme.Bar`).
     *     This would rename "acme-bundle:com.acme.Foo" to "acme-bundle:com.acme.Bar".
     * 
     * However, to achieve that is fiddly for several reasons:
     * 
     *  1. We might be passed qualified or unqualified names (e.g. "com.acme.Foo" or "acme-bundle:com.acme.Foo"),
     *     depending how old the persisted state is, where OSGi was used previously, and whether 
     *     whitelabelled bundles were used. 
     * 
     *  2. Calling `super.realClass(name)` must return a class that has exactly the same name as 
     *     was passed in. This is because xstream will subsequently use `Class.forName` which is 
     *     fussy about that. However, if we're passed "acme-bundle:com.acme.Foo" then we'd expect
     *     to return a class named "com.acme.Foo". The final classloading in our 
     *     `XmlMementoSerializer$OsgiClassLoader.findClass()` will handle stripping out the bundle
     *     name, and using the right bundle.
     *     
     *     In the case where we haven't changed the name, then we can leave it up to 
     *     `XmlMementoSerializer$OsgiClassnameMapper.realClass()` to do sort this out. But if we've 
     *     done a rename, then unforutnately it's currently this class' responsibility!
     *     
     *     That means it has to fallback to calling classLoader.loadClass().
     *  
     *  3. As mentioned under the use-cases, the rename could include the full bundle name prefix, 
     *     or it might just be the classname. We want to handle both, so need to implement yet
     *     more fallback behaviour.
     * 
     * ---
     * TODO Wanted to pass xstream, rather than Supplier<ClassLoader>, in constructor. However, 
     * this caused NPE because of how this is constructed from inside 
     * XmlMementoSerializer.wrapMapperForNormalUsage, called from within an anonymous subclass of XStream!
     */
    
    public static final Logger LOG = LoggerFactory.getLogger(ClassRenamingMapper.class);
    
    private final Map<String, String> nameToType;
    private final Supplier<? extends ClassLoader> classLoaderSupplier;
    
    public ClassRenamingMapper(Mapper wrapped, Map<String, String> nameToType, Supplier<? extends ClassLoader> classLoaderSupplier) {
        super(wrapped);
        this.nameToType = checkNotNull(nameToType, "nameToType");
        this.classLoaderSupplier = checkNotNull(classLoaderSupplier, "classLoaderSupplier");
    }
    
    @Override
    public Class<?> realClass(String elementName) {
        String elementNamOrig = elementName;
        Maybe<String> elementNameOpt = Reflections.findMappedNameMaybe(nameToType, elementName);
        if (elementNameOpt.isPresent()) {
            LOG.debug("Mapping class '"+elementName+"' to '"+elementNameOpt.get()+"'");
            elementName = elementNameOpt.get();
        }

        CannotResolveClassException tothrow;
        try {
            return super.realClass(elementName);
        } catch (CannotResolveClassException e) {
            LOG.trace("Failed to load class using super.realClass({}), for orig class {}, attempting fallbacks: {}", new Object[] {elementName, elementNamOrig, e});
            tothrow = e;
        }
        
        // We didn't do any renaming; just throw the exception. Our responsibilities are done.
        // See XmlMementoSerializer.OsgiClassnameMapper.
        
        if (elementNameOpt.isPresent() && hasBundlePrefix(elementName)) {
            // We've renamed the class, so can't rely on XmlMementoSerializer$OsgiClassnameMapper.
            // Workaround for xstream using `Class.forName`, and therefore not liking us stripping
            // the bundle prefix.
            try {
                return classLoaderSupplier.get().loadClass(elementName);
            } catch (ClassNotFoundException e) {
                LOG.trace("Fallback loadClass({}) attempt failed (orig class {}): {}", new Object[] {elementName, elementNamOrig, e});
            }
        }

        if (hasBundlePrefix(elementNamOrig)) {
            PrefixAndClass prefixAndClass = splitBundlePrefix(elementNamOrig);
            Maybe<String> classNameOpt = Reflections.findMappedNameMaybe(nameToType, prefixAndClass.clazz);
            
            if (classNameOpt.isPresent()) {
                if (hasBundlePrefix(classNameOpt.get())) {
                    // It has been renamed to include a (potentially different!) bundle prefix; use that
                    elementName = classNameOpt.get();
                } else {
                    elementName = joinBundlePrefix(prefixAndClass.prefix, classNameOpt.get());
                }
                LOG.debug("Mapping class '"+elementNamOrig+"' to '"+elementName+"'");

                try {
                    return super.realClass(elementName);
                } catch (CannotResolveClassException e) {
                    LOG.trace("Fallback super.realClass({}) attempt failed (orig class {}): {}", new Object[] {elementName, elementNamOrig, e});
                }
                
                // As above, we'll fallback to loadClass because xstream's use of Class.forName doesn't like
                // the bundle prefix stuff.
                try {
                    return classLoaderSupplier.get().loadClass(elementName);
                } catch (ClassNotFoundException e) {
                    LOG.trace("Fallback loadClass({}) attempt failed (orig class {}): {}", new Object[] {elementName, elementNamOrig, e});
                }
            }
        }
        
        throw tothrow;
    }
    
    private boolean hasBundlePrefix(String type) {
        return type != null && type.contains(":");
    }
    
    private PrefixAndClass splitBundlePrefix(String type) {
        int index = type.lastIndexOf(OsgiClassPrefixer.DELIMITER);
        if (index <= 0) throw new IllegalStateException("'"+type+"' is not in a valid bundle:class format");
        String prefix = type.substring(0, index);
        String clazz = type.substring(index + 1);
        return new PrefixAndClass(prefix, clazz);
    }
    
    private String joinBundlePrefix(String prefix, String clazz) {
        return checkNotNull(prefix, "prefix") + OsgiClassPrefixer.DELIMITER + checkNotNull(clazz, "clazz");
    }
    
    private static class PrefixAndClass {
        private final String prefix;
        private final String clazz;
        
        public PrefixAndClass(String prefix, String clazz) {
            this.prefix = checkNotNull(prefix, "prefix");
            this.clazz = checkNotNull(clazz, "clazz");
        }
    }
}
