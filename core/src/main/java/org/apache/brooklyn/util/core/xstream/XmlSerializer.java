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

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.extended.JavaClassConverter;
import com.thoughtworks.xstream.mapper.DefaultMapper;
import com.thoughtworks.xstream.mapper.Mapper;
import com.thoughtworks.xstream.mapper.MapperWrapper;

public class XmlSerializer<T> {

    private final Map<String, String> deserializingClassRenames;
    protected final XStream xstream;

    public XmlSerializer() {
        this(ImmutableMap.<String, String>of());
    }
    
    public XmlSerializer(Map<String, String> deserializingClassRenames) {
        this(null, deserializingClassRenames);
    }
    
    public XmlSerializer(ClassLoader loader, Map<String, String> deserializingClassRenames) {
        this.deserializingClassRenames = deserializingClassRenames;
        xstream = new XStream() {
            @Override
            protected MapperWrapper wrapMapper(MapperWrapper next) {
                return XmlSerializer.this.wrapMapperForNormalUsage( super.wrapMapper(next) );
            }
        };
        if (loader!=null) {
            xstream.setClassLoader(loader);
        }
        
        xstream.registerConverter(newCustomJavaClassConverter(), XStream.PRIORITY_NORMAL);
        
        // list as array list is default
        xstream.alias("map", Map.class, LinkedHashMap.class);
        xstream.alias("set", Set.class, LinkedHashSet.class);
        
        xstream.registerConverter(new StringKeyMapConverter(xstream.getMapper()), /* priority */ 10);
        xstream.alias("MutableMap", MutableMap.class);
        xstream.alias("MutableSet", MutableSet.class);
        xstream.alias("MutableList", MutableList.class);
        
        // Needs an explicit MutableSet converter!
        // Without it, the alias for "set" seems to interfere with the MutableSet.map field, so it gets
        // a null field on deserialization.
        xstream.registerConverter(new MutableSetConverter(xstream.getMapper()));
        
        xstream.aliasType("ImmutableList", ImmutableList.class);
        xstream.registerConverter(new ImmutableListConverter(xstream.getMapper()));
        xstream.registerConverter(new ImmutableSetConverter(xstream.getMapper()));
        xstream.registerConverter(new ImmutableMapConverter(xstream.getMapper()));

        xstream.registerConverter(new EnumCaseForgivingConverter());
        xstream.registerConverter(new Inet4AddressConverter());
        
        // See ObjectWithDefaultStringImplConverter (and its usage) for why we want to auto-detect 
        // annotations (usages of this is in the camp project, so we can't just list it statically
        // here unfortunately).
        xstream.autodetectAnnotations(true);
    }

    /**
     * JCC is used when Class instances are serialized/deserialized as a value 
     * (not as tags) and there are no aliases configured for that type.
     * It is configured in XStream default *without* access to the XStream mapper,
     * which is meant to apply when serializing the type name for instances of that type.
     * <p>
     * However we need a few selected mappers (see {@link #wrapMapperForHandlingClasses(Mapper)} )
     * to apply to all class renames, but many of the mappers must NOT be used,
     * e.g. because some might intercept all Class<? extends Entity> references
     * (and that interception is only wanted when serializing <i>instances</i>,
     * as in {@link #wrapMapperForNormalUsage(Mapper)}).
     * <p>
     * This can typically be done simply by registering our own instance of this (due to order guarantee of PrioritizedList),
     * after the instance added by XStream.setupConverters()
     */
    private JavaClassConverter newCustomJavaClassConverter() {
        return new JavaClassConverter(wrapMapperForHandlingClasses(new DefaultMapper(xstream.getClassLoaderReference()))) {};
    }
    
    /** Extension point where sub-classes can add mappers needed for handling class names.
     * This is used by {@link #wrapMapperForNormalUsage(Mapper)} and also to set up the {@link JavaClassConverter}
     * (see {@link #newCustomJavaClassConverter()} for what that does).
     * <p>
     * This should apply when nice names are used for inner classes, or classes are renamed;
     * however mappers which affect field aliases or intercept references to entities are not
     * wanted in the {@link JavaClassConverter} and so should be added by {@link #wrapMapperForNormalUsage(Mapper)}
     * instead of this.
     * <p>
     * Developers note this is called from the constructor; be careful when overriding and 
     * see comment on {@link #wrapMapperForNormalUsage(Mapper)} about field availability. */
    protected MapperWrapper wrapMapperForHandlingClasses(Mapper next) {
        MapperWrapper result = new CompilerIndependentOuterClassFieldMapper(next);
        
        Supplier<ClassLoader> classLoaderSupplier = new Supplier<ClassLoader>() {
            @Override public ClassLoader get() {
                return xstream.getClassLoaderReference().getReference();
            }
        };
        result = new ClassRenamingMapper(result, deserializingClassRenames, classLoaderSupplier);
        result = new OsgiClassnameMapper(new Supplier<XStream>() {
            @Override public XStream get() { return xstream; } }, result);
        // TODO as noted in ClassRenamingMapper that class can be simplified if 
        // we swap the order of the above calls, because it _will_ be able to rely on
        // OsgiClassnameMapper to attempt to load with the xstream reference stack
        // (not doing it just now because close to a release)
        
        return result;
    }
    /** Extension point where sub-classes can add mappers to set up the main {@link Mapper} given to XStream.
     * This includes all of {@link #wrapMapperForHandlingClasses(Mapper)} plus anything wanted for normal usage.
     * <p>
     * Typically any non-class-name mappers wanted should be added in a subclass by overriding this field,
     * calling this superclass method, then wrapping the result.
     * <p>
     * Developers note this is called from the constructor; be careful when overriding 
     * because most fields won't be available.  In particular in a subclass, 
     * this method in the subclass will be invoked very early in its constructor.
     * Fields like {@link #xstream} (and <i>anything</i> set in the subclass) won't
     * yet be available. For this reason some mappers will need to be given a {@link Supplier} for late resolution. */
    protected MapperWrapper wrapMapperForNormalUsage(Mapper next) {
        return wrapMapperForHandlingClasses(next);
    }

    public void serialize(Object object, Writer writer) {
        xstream.toXML(object, writer);
    }

    @SuppressWarnings("unchecked")
    public T deserialize(Reader xml) {
        return (T) xstream.fromXML(xml);
    }

    public String toString(T memento) {
        Writer writer = new StringWriter();
        serialize(memento, writer);
        return writer.toString();
    }

    public T fromString(String xml) {
        return deserialize(new StringReader(xml));
    }

}
