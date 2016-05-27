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
package org.apache.brooklyn.config;

import java.io.Serializable;

import org.apache.brooklyn.util.text.Strings;

import com.google.common.annotations.Beta;

@SuppressWarnings("serial")
public abstract class ConfigInheritance implements Serializable {

    public enum InheritanceMode {
        NONE,
        IF_NO_EXPLICIT_VALUE,
        MERGE
    }

    public static final ConfigInheritance NONE = new None();
    public static final ConfigInheritance ALWAYS = new Always();
    public static final ConfigInheritance MERGE = new Merged();
    
    public static ConfigInheritance fromString(String val) {
        if (Strings.isBlank(val)) return null;
        switch (val.toLowerCase().trim()) {
        case "none":
            return NONE;
        case "always": 
            return ALWAYS;
        case "merge" :
            return MERGE;
        default:
            throw new IllegalArgumentException("Invalid config-inheritance '"+val+"' (legal values are none, always or merge)");
        }
    }
    
    private ConfigInheritance() {}
    
    @Beta
    public abstract InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to);

    private static class Always extends ConfigInheritance {
        @Override
        public InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to) {
            return InheritanceMode.IF_NO_EXPLICIT_VALUE;
        }
    }

    private static class None extends ConfigInheritance {
        @Override
        public InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to) {
            return InheritanceMode.NONE;
        }
    }
    
    private static class Merged extends ConfigInheritance {
        @Override
        public InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to) {
            return InheritanceMode.MERGE;
        }
    }
}
