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

public abstract class YormlContext {

    final String jsonPath;
    final String expectedType;
    Object javaObject;
    Object yamlObject;
    
    public YormlContext(String jsonPath, String expectedType) {
        this.jsonPath = jsonPath;
        this.expectedType = expectedType;
    }
    
    public String getJsonPath() {
        return jsonPath;
    }
    public String getExpectedType() {
        return expectedType;
    }
    public Object getJavaObject() {
        return javaObject;
    }
    public void setJavaObject(Object javaObject) {
        this.javaObject = javaObject;
    }
    public Object getYamlObject() {
        return yamlObject;
    }
    public void setYamlObject(Object yamlObject) {
        this.yamlObject = yamlObject;
    }
    
}
