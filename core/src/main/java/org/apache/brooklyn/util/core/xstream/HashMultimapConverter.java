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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.SingleValueConverter;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.collections.CollectionConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.Mapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Map;
//import org.apache.brooklyn.util.javalang.ReflectionsTest.K;

/** serialization of HashMultimap changed after Guava 18;
 * to support pre-Guava-18 serialized objects we just continue to use the old logic.
 * To test, note that this is testable with the RebingTest.testSshFeed_2018_...
 */
public class HashMultimapConverter extends CollectionConverter{

    private final Mapper mapper;

    public HashMultimapConverter(Mapper mapper) {
        super(mapper);
        this.mapper = mapper;
    }

    // TODO how do we convert this from ObjectOutputStream to XStream mapper ?
    // (code below copied from HashMultimap.readObject / writeObject)


    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        HashMultimap hashMultimap = (HashMultimap) source;
//        writeCompleteItem(source, context,writer);
        XStream xstream = new XStream();
        try {
            ObjectOutputStream stream = xstream.createObjectOutputStream(writer);
//            stream.writeObject(hashMultimap);
            stream.writeInt(2);
            writeMultimap(hashMultimap, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        reader.moveDown();
        if(reader.getNodeName().equals("unserializable-parents")){
            reader.moveUp();
            reader.moveDown();
        }
        if(reader.getNodeName().equals("com.google.guava:com.google.common.collect.HashMultimap")){
            reader.moveDown();
        }
        if(reader.getNodeName().equals("default")){
            reader.moveUp();
            reader.moveDown();
        }
        String value = reader.getValue();

        int distinctKeys = Integer.parseInt(value);
        HashMultimap<Object, Object> objectObjectHashMultimap = HashMultimap.create();

        XStream xstream = new XStream();
        try (ObjectInputStream objectInputStream = xstream.createObjectInputStream(reader)) {
//            objectInputStream.defaultReadObject();
            populateMultimap(objectObjectHashMultimap, reader, distinctKeys, context);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return objectObjectHashMultimap;
//        int expectedValuesPerKey = stream.readInt();
//        stream.defaultReadObject();
//        int distinctKeys = Serialization.readCount(stream);
//        Map<K, Collection<V>> map = Maps.newHashMapWithExpectedSize(distinctKeys);
//        setMap(map);
//        HashMultimapConverter.populateMultimap(this, stream, expectedValuesPerKey);

    }

    @Override
    public boolean canConvert(Class type) {
        return HashMultimap.class.isAssignableFrom(type);
    }


    /**
     * Stores the contents of a multimap in an output stream, as part of serialization. It does not
     * support concurrent multimaps whose content may change while the method is running. The {@link
     * Multimap#asMap} view determines the ordering in which data is written to the stream.
     *
     * <p>The serialized output consists of the number of distinct keys, and then for each distinct
     * key: the key, the number of values for that key, and the key's values.
     */
    static <K, V> void writeMultimap(Multimap<K, V> multimap, HierarchicalStreamWriter stream)
            throws IOException {
/*        stream.defaultWriteObject();
        stream.writeInt(multimap.asMap().size());
        for (Map.Entry<K, Collection<V>> entry : multimap.asMap().entrySet()) {
            stream.writeObject(entry.getKey());
            stream.writeInt(entry.getValue().size());
            for (V value : entry.getValue()) {
                stream.writeObject(value);
            }
        }*/
    }


    <K, V> void populateMultimap(
            HashMultimap<Object, Object> multimap, HierarchicalStreamReader stream, int distinctKeys, UnmarshallingContext context)
            throws IOException, ClassNotFoundException {
        for (int i = 0; i < distinctKeys; i++) {
            stream.moveUp();
            stream.moveDown();

            stream.moveUp();
            stream.moveDown();
            int valueCount = stream.getAttributeCount();
            K key = (K) stream.getNodeName();
            @SuppressWarnings("unchecked") // reading data stored by writeMultimap
            Collection<V> values = (Collection<V>) multimap.get(key);
//            populateCollection(stream, context, values );

            while(stream.hasMoreChildren()){
                stream.moveDown();
//                @SuppressWarnings("unchecked") // reading data stored by writeMultimap
                Object bareItem = readBareItem(stream, context, values);
                values.add((V) bareItem);
//                V value = (V) stream.getValue();
//                values.add(value);
                stream.moveUp();
            }

        }
    }
}
