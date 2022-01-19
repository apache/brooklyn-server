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

import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.reflection.ReflectionProvider;
import com.thoughtworks.xstream.converters.reflection.SerializableConverter;
import com.thoughtworks.xstream.core.ClassLoaderReference;
import com.thoughtworks.xstream.io.ExtendedHierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.MapperWrapper;
import org.apache.brooklyn.util.collections.MutableList;

import com.thoughtworks.xstream.converters.collections.CollectionConverter;
import com.thoughtworks.xstream.mapper.Mapper;

public class MutableListConverter extends CollectionConverter {

    final SerializableConverter serializableConverterTweakedForDeserializing;

    public MutableListConverter(Mapper mapper, ReflectionProvider reflectionProvider, ClassLoaderReference classLoaderReference) {
        super(mapper);
        serializableConverterTweakedForDeserializing = new SerializableConverter(new MapperWrapper(mapper) {
            @Override
            public String aliasForSystemAttribute(String attribute) {
                // don't expect "serialization=custom" as an attribute on our node; always fall back to custom
                if ("serialization".equals(attribute)) return null;
                return super.aliasForSystemAttribute(attribute);
            }
        }, reflectionProvider, classLoaderReference);
    }

    @Override
    public boolean canConvert(@SuppressWarnings("rawtypes") Class type) {
        return MutableList.class.isAssignableFrom(type);
    }

    @Override
    protected Object createCollection(Class type) {
        return new MutableList<Object>();
    }

    // take a copy first to avoid CMEs
    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        super.marshal(MutableList.copyOf((MutableList)source), writer, context);
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        // support legacy format (when SerializableConverter wrote this, not us)
        boolean legacy = "unserializable-parents".equals(((ExtendedHierarchicalStreamReader)reader).peekNextChild());
        if (legacy) {
            return serializableConverterTweakedForDeserializing.unmarshal(reader, context);
        }

        return super.unmarshal(reader, context);
    }
}
