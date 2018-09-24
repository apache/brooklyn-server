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
package org.apache.brooklyn.rest.domain;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.objs.ConstraintSerialization;
import org.apache.brooklyn.util.collections.Jsonya;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ConfigSummary implements HasName, Serializable {

    private static final long serialVersionUID = -2831796487073496730L;

    private final String name;
    private final String type;
    @JsonInclude(Include.NON_NULL)
    private final Object defaultValue;
    @JsonInclude(Include.NON_NULL)
    private final String description;
    @JsonSerialize
    private final boolean reconfigurable;
    @JsonInclude(Include.NON_NULL)
    private final String label;
    @JsonInclude(Include.NON_NULL)
    private final Double priority;
    @JsonInclude(Include.NON_NULL)
    private final List<Map<String, String>> possibleValues;
    @JsonInclude(Include.NON_NULL)
    private final Boolean pinned;
    @JsonInclude(Include.NON_NULL)
    private final List<Object> constraints;

    @JsonInclude(Include.NON_NULL)
    private final Map<String, URI> links;

    // json deserialization
    ConfigSummary() {
        this(null, null, null, null, false, null, null, null, null, null, null);
    }
    
    protected ConfigSummary(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("description") String description,
            @JsonProperty("defaultValue") Object defaultValue,
            @JsonProperty("reconfigurable") boolean reconfigurable,
            @JsonProperty("label") String label,
            @JsonProperty("priority") Double priority,
            @JsonProperty("possibleValues") List<Map<String, String>> possibleValues,
            @JsonProperty("pinned") Boolean pinned,
            @JsonProperty("constraints") List<?> constraints,
            @JsonProperty("links") Map<String, URI> links) {
        this.name = name;
        this.type = type;
        this.description = description;
        this.defaultValue = defaultValue;
        this.reconfigurable = reconfigurable;
        this.label = label;
        this.priority = priority;
        this.possibleValues = possibleValues;
        this.pinned = pinned;
        this.constraints = (constraints == null) ? ImmutableList.<Object>of() : ImmutableList.copyOf(constraints);
        this.links = (links == null) ? ImmutableMap.<String, URI>of() : ImmutableMap.copyOf(links);
    }

    public ConfigSummary(ConfigKey<?> config, String label, Double priority, Boolean pinned, Map<String, URI> links) {
        this.name = config.getName();
        this.description = config.getDescription();
        this.reconfigurable = config.isReconfigurable();

        /* Use String, to guarantee it is serializable; otherwise get:
         *   No serializer found for class org.apache.brooklyn.policy.autoscaling.AutoScalerPolicy$3 and no properties discovered to create BeanSerializer (to avoid exception, disable SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS) ) (through reference chain: java.util.ArrayList[9]->org.apache.brooklyn.rest.domain.PolicyConfigSummary["defaultValue"])
         *   at org.codehaus.jackson.map.ser.impl.UnknownSerializer.failForEmpty(UnknownSerializer.java:52)
         */
        this.label = label;
        this.priority = priority;
        this.pinned = pinned;
        this.constraints = ConstraintSerialization.INSTANCE.toJsonList(config);
        if (config.getType().isEnum()) {
            this.type = Enum.class.getName();
            this.defaultValue = config.getDefaultValue() instanceof Enum? ((Enum<?>) config.getDefaultValue()).name() : 
                Jsonya.convertToJsonPrimitive(config.getDefaultValue());
            this.possibleValues = FluentIterable
                    .from(Arrays.asList((Enum<?>[])(config.getType().getEnumConstants())))
                    .transform(new Function<Enum<?>, Map<String, String>>() {
                        @Nullable
                        @Override
                        public Map<String, String> apply(@Nullable Enum<?> input) {
                            return ImmutableMap.of(
                                    "value", input != null ? input.name() : null,
                                   "description", input != null ? input.toString() : null);
                        }})
                    .toList();
        } else {
            this.type = config.getTypeName();
            this.defaultValue = Jsonya.convertToJsonPrimitive(config.getDefaultValue());
            this.possibleValues = null;
        }
        this.links = (links == null) ? ImmutableMap.<String, URI>of() : ImmutableMap.copyOf(links);
    }

    @Override
    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public boolean isReconfigurable() {
        return reconfigurable;
    }

    public Object getDefaultValue() {
        // note constructor has converted to string, so this is safe for clients to use
        return defaultValue;
    }

    public String getLabel() {
        return label;
    }

    public Double getPriority() {
        return priority;
    }

    public List<Map<String, String>> getPossibleValues() {
        return possibleValues;
    }

    public Boolean isPinned() {
        return pinned;
    }

    public List<Object> getConstraints() {
        return constraints;
    }

    public Map<String, URI> getLinks() {
        return links;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConfigSummary)) return false;
        ConfigSummary that = (ConfigSummary) o;
        return reconfigurable == that.reconfigurable &&
                Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(description, that.description) &&
                Objects.equals(label, that.label) &&
                Objects.equals(priority, that.priority) &&
                Objects.equals(possibleValues, that.possibleValues) &&
                Objects.equals(pinned, that.pinned) &&
                Objects.equals(constraints, that.constraints) &&
                Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, defaultValue, description, reconfigurable, label, priority, 
            possibleValues, pinned, constraints);
    }

    @Override
    public String toString() {
        return "ConfigSummary{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", defaultValue=" + defaultValue +
                ", description='" + description + '\'' +
                ", reconfigurable=" + reconfigurable +
                ", label='" + label + '\'' +
                ", priority=" + priority +
                ", possibleValues=" + possibleValues +
                ", pinned=" + pinned +
                ", constraints=" + constraints +
                '}';
    }
}
