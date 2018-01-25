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
package org.apache.brooklyn.util.yoml.internal;

import org.apache.brooklyn.util.text.Strings;

public class YomlContextForRead extends YomlContext {

    public YomlContextForRead(Object yamlObject, String jsonPath, String expectedType, YomlContext parent) {
        super(jsonPath, expectedType, parent);
        setYamlObject(yamlObject);
    }
    
    @Override
    public YomlContextForRead subpath(String subpath, Object newItem, String superType) {
        return new YomlContextForRead(newItem, getJsonPath()+subpath, superType, this);
    }

    String origin;
    int offset;
    int length;
    
    @Override
    public String toString() {
        return "reading"+(expectedType!=null ? " "+expectedType : "")+" at "+(Strings.isNonBlank(jsonPath) ? jsonPath : "root");
    }

    public YomlContextForRead constructionInstruction(ConstructionInstruction newConstruction) {
        YomlContextForRead result = new YomlContextForRead(yamlObject, jsonPath, expectedType, parent);
        result.constructionInstruction = newConstruction;
        return result;
    }
}
