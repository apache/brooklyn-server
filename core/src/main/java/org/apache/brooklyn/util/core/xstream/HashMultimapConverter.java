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
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.collections.CollectionConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.Mapper;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Set;

/** serialization of HashMultimap changed after Guava 18;
 * to support pre-Guava-18 serialized objects we just continue to use the old logic.
 * To test, note that this is testable with the RebingTest.testSshFeed_2018_...
 */
public class HashMultimapConverter extends CollectionConverter{

    public HashMultimapConverter(Mapper mapper) {
        super(mapper);
    }

    // TODO how do we convert this from ObjectOutputStream to XStream mapper ?
    // (code below copied from HashMultimap.readObject / writeObject)
    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        HashMultimap hashMultimap = (HashMultimap) source;

        writer.addAttribute("serialization", "custom");
        writer.startNode("unserializable-parents");
        writer.endNode();
        writer.startNode(HashMultimap.class.getCanonicalName());
        writer.startNode("default");
        writer.endNode();
        writer.startNode("int");
        writer.setValue("2"); // HashMultimap.DEFAULT_VALUES_PER_KEY
        writer.endNode();
        writer.startNode("int");
        Set keys = hashMultimap.keys().elementSet();
        int distinctKeys = hashMultimap.asMap().size();
        writer.setValue(Integer.toString(distinctKeys));
        writer.endNode();

        keys.forEach(key -> {
            writeCompleteItem(key, context, writer);
            Collection entry = hashMultimap.get(key);
            writer.startNode("int");
            writer.setValue(Integer.toString(entry.size()));
            writer.endNode();
            entry.forEach(child -> writeCompleteItem(child, context, writer));
        });

        writer.endNode();
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        reader.moveDown();
        if (reader.getNodeName().equals("unserializable-parents")) {
            reader.moveUp();
            reader.moveDown();
        }
        if (reader.getNodeName().equals("com.google.common.collect.HashMultimap") || reader.getNodeName().equals("com.google.guava:com.google.common.collect.HashMultimap")) {
            reader.moveDown();
        }

        if (reader.getNodeName().equals("default")) {
            reader.moveUp();
            reader.moveDown();
        }
        reader.moveUp();
        reader.moveDown();
        String value = reader.getValue();
        int distinctKeys = Integer.parseInt(value);
        reader.moveUp();

        HashMultimap<Object, Object> objectObjectHashMultimap = HashMultimap.create();
        for (int i = 0; i < distinctKeys; i++) {
            Object key = readCompleteItem(reader, context, null);
            reader.moveDown();
            int children= Integer.parseInt(reader.getValue());
            reader.moveUp();
            for (int j = 0; j < children; j++) {
                Object child = readCompleteItem(reader, context, null);
                objectObjectHashMultimap.put(key, child);
            }
        }

        return objectObjectHashMultimap;
    }

    @Override
    public boolean canConvert(Class type) {
        return HashMultimap.class.isAssignableFrom(type);
    }
}
