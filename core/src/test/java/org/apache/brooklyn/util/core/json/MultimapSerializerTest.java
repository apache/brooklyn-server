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

package org.apache.brooklyn.util.core.json;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

public class MultimapSerializerTest extends BrooklynAppUnitTestSupport {

    ObjectMapper mapper;

    @Override
    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        super.setUp();
        mapper = new ObjectMapper();
        SimpleModule mapperModule = new SimpleModule("MultimapSerializerTest", new Version(0, 0, 0, "ignored", null, null));
        mapperModule.addSerializer(new MultimapSerializer());
        mapper.registerModule(mapperModule);
    }

    @Test
    public void testSerializeStringKey() throws Exception {
        Multimap<String, Integer> map = MultimapBuilder.hashKeys().arrayListValues().build();
        map.put("a", 1);
        map.put("a", 2);
        map.put("a", 3);
        String json = mapper.writer().writeValueAsString(map);
        assertEquals(json, "{\"a\":[1,2,3]}");
    }

    @Test
    public void testSerializeEntityKey() throws Exception {
        Entity entity = app.createAndManageChild(EntitySpec.create(BasicEntity.class));
        Multimap<Entity, Integer> map = MultimapBuilder.hashKeys().arrayListValues().build();
        map.put(entity, 1);
        map.put(entity, 2);
        map.put(entity, 3);
        String json = mapper.writer().writeValueAsString(map);
        assertEquals(json, "{\"BasicEntityImpl{id=" + entity.getId() + "}" + "\":[1,2,3]}");
    }

}