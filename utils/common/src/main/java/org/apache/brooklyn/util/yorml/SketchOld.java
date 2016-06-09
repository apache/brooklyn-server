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

import java.util.Map;

public class SketchOld {

/*

catalog items can register a converter

api for finding/using stock converters, and supplying add'l converters (recursive context)

Context
    mgmt
    extraGlobalConverters -- immutable list copy on write
    extraGlobalHandlers
    findConverterFor(String type)
ReadContext<T>
    yamlParseNode
    Map inputMap -- immutable
    Set inputKeys
    Object objectBeingCreated
    childContextFor(object)
WriteContext<T>
    Object objectToOutput
    Map outputMap
    Set remainingFieldsToWrite

Handler
    
    read(ReadContext) <- updates an object, invoking converters as needed

Converter
    ReadContext child = createObject(ReadContext parent)
      just creating the type
    void populateObject(RC parent)
      invoking handlers etc
    


handleKey("effectors", Handler)
 
EntitySpecConverter reads 


- id: sample
  type: InitdishSoftwareProcess

  inputs:
  - name: some.config: xxx
  brooklyn.config:
    some.other.field: xxx

  effectors:
  - name: start
    type: InitdishEffector
    description: some start
    parameters:
    - name: foo
    impl:
      00-acquire_lock: acquire-lock
      01-start: xxx
  
  sensors:
  - name: foo
    type: int
    value: xxx
    persist: optional
  
  feeds:
  - type: https
    url: /endpoint
    period: 5s
    set: all
    then:
    - json_path: a/b/c
      set: foo_abc
      then:
        json_path: d
        set: foo_abc_d
    - json_path: x
      set: foo_x

  initializers:
  - type: InitdishEffectorInitializer
    XXX
      
*/
    public static class InitdishEffectorParser {
        /*
         * at spec parse time when we see type is InitdishSoftwareProcess
         * we add some Converters
         * 
         * so CatalogItem can specify Converters.
         * if it extends we also take those converters.
         * 
         * GlobalConverters
         */
    }
    
    public interface Converter {
        void readingMap(Map<Object,Object> remainingKeys, Object objectBeingBuilt);
        void writingMap(Map<Object,Object> remainingFields, Object objectBeingWritten, Map<Object,Object> targetMap);
    }
    
}
