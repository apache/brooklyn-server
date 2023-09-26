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

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.Mapper;
import net.minidev.json.JSONObject;

// JSONObject is
public class MinidevJsonObjectConverter extends MapConverter {

    public MinidevJsonObjectConverter(Mapper mapper) {
        super(mapper);
    }

    @Override
    public boolean canConvert(@SuppressWarnings("rawtypes") Class type) {
        return JSONObject.class.isAssignableFrom(type);
    }

    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        // marshall as a normal map
        super.marshal(source, writer, context);
    }

    // return as a JSONObject; this class invoked when XML attributes specify that is the type
    @Override
    public JSONObject unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        // unmarshall as a normal map
        Map result = (Map) super.unmarshal(reader, context);
        // but return as a JSONObject, just in case that is required
        return new JSONObject(result);
    }
}
