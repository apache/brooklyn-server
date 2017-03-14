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

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Boxing;

import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.ConverterLookup;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

/**
 * A converter that delegates to {@link ConverterLookup#lookupConverterForType(Class)}, where
 * possible. On unmarshalling, if there is no {@code class} attribute then it assumes that the
 * type must be a {@link String}.
 * 
 * This is useful if a class has its field changed from type {@link String} to {@link Object}.
 * The old persisted state will look like {@code <mykey>myval</mykey>}, rather than 
 * {@code <mykey class="string">myval</mykey>}.
 * 
 * Use this converter by annotation the field with
 * {@code @XStreamConverter(ObjectWithDefaultStringImplConverter.class)}. One must also call
 * {@code xstream.processAnnotations(MyClazz.class)} to ensure the annotation is actually parsed.
 */
public class ObjectWithDefaultStringImplConverter implements Converter {
    private final ConverterLookup lookup;
    private final ClassLoader loader;
    private final Class<?> defaultImpl = String.class;

    public ObjectWithDefaultStringImplConverter(ConverterLookup lookup, ClassLoader loader) {
        this.lookup = lookup;
        this.loader = loader;
    }

    @Override
    public boolean canConvert(@SuppressWarnings("rawtypes") Class type) {
        // We expect this to be used for a field of type `Object`, so could be passed an instance
        // of anything at all to marshal - therefore say we can convert anything!
        // We'll then delegate, based on the type we're given.
        return true;
    }

    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        if (source == null) return;
        Converter converter = lookup.lookupConverterForType(source.getClass());
        converter.marshal(source, writer, context);
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        String clazzName = reader.getAttribute("class");
        Class<?> clazz;
        if (clazzName == null) {
            clazz = defaultImpl;
        } else if (clazzName.equals("string")) {
            clazz = String.class;
        } else if (Boxing.getPrimitiveType(clazzName).isPresent()) {
            clazz = Boxing.getPrimitiveType(clazzName).get();
        } else {
            try {
                clazz = loader.loadClass(clazzName);
            } catch (ClassNotFoundException e) {
                throw Exceptions.propagate(e);
            }
        }
        Converter converter = lookup.lookupConverterForType(clazz);
        return converter.unmarshal(reader, context);
    }
}
