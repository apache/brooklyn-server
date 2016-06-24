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
package org.apache.brooklyn.util.yorml;

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.yorml.serializers.FieldsInFieldsMap;
import org.apache.brooklyn.util.yorml.serializers.InstantiateType;


public class Yorml {

    YormlConfig config;
    
    private Yorml() {}
    
    public static Yorml newInstance(YormlTypeRegistry typeRegistry) {
        return newInstance(typeRegistry, MutableList.<YormlSerializer>of(
            new FieldsInFieldsMap(),
            new InstantiateType() ));
    }
    
    public static Yorml newInstance(YormlTypeRegistry typeRegistry, List<YormlSerializer> serializers) {
        Yorml result = new Yorml();
        result.config = new YormlConfig();
        result.config.typeRegistry = typeRegistry;
        result.config.serializersPost.addAll(serializers);
        
        return result;
    }
    
    public Object read(String yaml) {
        return read(yaml, null);
    }
    public Object read(String yaml, String type) {
        return readFromYamlObject(new org.yaml.snakeyaml.Yaml().load(yaml), type);
    }
    public Object readFromYamlObject(Object yamlObject, String type) {
        YormlContextForRead context = new YormlContextForRead("", type);
        context.setYamlObject(yamlObject);
        new YormlConverter(config).read(context);
        return context.getJavaObject();
    }

    public Object write(Object java) {
        YormlContextForWrite context = new YormlContextForWrite("", null);
        context.setJavaObject(java);
        new YormlConverter(config).write(context);
        return context.getYamlObject();
    }
    
//    public <T> T read(String yaml, Class<T> type) {
//    }
//    public <T> T read(String yaml, TypeToken<T> type) {
//    }
    
}
