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
package org.apache.brooklyn.util.yoml;

import org.apache.brooklyn.util.yoml.internal.YomlContextForRead;
import org.apache.brooklyn.util.yoml.internal.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.internal.YomlConverter;


public class Yoml {

    final YomlConfig config;
    
    private Yoml(YomlConfig config) { this.config = config; }
    
    public static Yoml newInstance(YomlConfig config) {
        return new Yoml(config);
    }
    
    public static Yoml newInstance(YomlTypeRegistry typeRegistry) {
        return new Yoml(YomlConfig.Builder.builder().typeRegistry(typeRegistry).serializersPostAddDefaults().build());
    }
    
    public YomlConfig getConfig() {
        return config;
    }
    
    public Object read(String yaml) {
        return read(yaml, null);
    }
    public Object read(String yaml, String expectedType) {
        return readFromYamlObject(new org.yaml.snakeyaml.Yaml().load(yaml), expectedType);
    }
    public Object readFromYamlObject(Object yamlObject, String type) {
        return new YomlConverter(config).read( new YomlContextForRead(yamlObject, "", type, null) );
    }

    public Object write(Object java) {
        return write(java, null);
    }
    public Object write(Object java, String expectedType) {
        return new YomlConverter(config).write( new YomlContextForWrite(java, "", expectedType, null) );
    }
    
//    public <T> T read(String yaml, Class<T> type) {
//    }
//    public <T> T read(String yaml, TypeToken<T> type) {
//    }
    
}
