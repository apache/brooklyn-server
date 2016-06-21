/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.entity.software.base;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.core.json.BrooklynObjectsJsonMapper;
import org.apache.brooklyn.util.exceptions.Exceptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ShellEnvironmentSerializer {
    private ObjectMapper mapper;
    
    public ShellEnvironmentSerializer(ManagementContext mgmt) {
        mapper = BrooklynObjectsJsonMapper.newMapper(mgmt);
    }

    public String serialize(Object value) {
        if (value == null) return null;
        if (value instanceof String) return (String)value;
        try {
            String str = mapper.writeValueAsString(value);
            // Avoid dealing with unquoting and unescaping the serialized result is a string
            if (isString(str)) {
                return value.toString();
            } else {
                return str;
            }
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    protected boolean isString(String str) {
        return str.length() > 0 && str.charAt(0) == '"';
    }
}
