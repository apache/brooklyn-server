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
package org.apache.brooklyn.util.core.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

public class ShellEnvironmentSerializer {
    private ObjectMapper mapper;
    private final Function<Object, Object> resolver;

    public ShellEnvironmentSerializer(ManagementContext mgmt) {
        this(mgmt, null);
    }
    public ShellEnvironmentSerializer(ManagementContext mgmt, Function<Object,Object> resolver) {
        mapper = BrooklynObjectsJsonMapper.newMapper(mgmt);
        this.resolver = resolver;
    }

    public String serialize(Object value) {
        if (value == null) return null;
        if (value instanceof String) return (String)value;
        try {
            if (value instanceof DeferredSupplier) {
                if (resolver!=null) {
                    value = resolver.apply(value);
                } else {
                    // could warn, because this probably isn't intended, but it might be
                    // throw new IllegalStateException("Cannot pass deferred suppliers to shell environment without a resolve function.");
                }
            }
            String str = mapper.writeValueAsString(value);
            if (isJsonString(str)) {
                // previously (2022-06) we would just write value.toString() in this block; but some things are serialized more nicely than toString, so prefer that format
                // however if it is unambiguously a string then unwrap
                // (not strictly the JSON unwrapping, but for the subset we treat unescaped it is okay)
                String unescaped = StringEscapes.BashStringEscapes.unwrapBashQuotesAndEscapes(str);
                if (unescaped.matches("[A-Za-z0-9 :,./*?!_+^=-]*")) {
                    return unescaped;
                }
                return str;
            } else {
                return str;
            }
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }
    
    public Map<String, String> serialize(@Nullable Map<?, ?> env) {
        if (env == null) {
            return null;
        }
        Map<String, String> serializedEnv = Maps.newHashMap();
        for (Entry<?, ?> entry : env.entrySet()) {
            String key = serializeShellEnv(entry.getKey());
            String value = serializeShellEnv(entry.getValue());
            serializedEnv.put(key, value);
        }
        return serializedEnv;
    }

    protected boolean isJsonString(String str) {
        return str.length() > 0 && str.charAt(0) == '"';
    }

    private String serializeShellEnv(Object value) {
        return StringUtils.defaultString(serialize(value));
    }

}
