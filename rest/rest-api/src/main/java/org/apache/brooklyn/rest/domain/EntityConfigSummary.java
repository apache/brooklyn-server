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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.text.StringPredicates;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class EntityConfigSummary extends ConfigSummary {

    private static final long serialVersionUID = -1336134336883426030L;

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final Boolean pinned;

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final List<String> constraints;

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final Map<String, URI> links;

    public EntityConfigSummary(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("description") String description,
            @JsonProperty("defaultValue") Object defaultValue,
            @JsonProperty("reconfigurable") boolean reconfigurable,
            @JsonProperty("label") String label,
            @JsonProperty("priority") Double priority,
            @JsonProperty("possibleValues") List<Map<String, String>> possibleValues,
            @JsonProperty("pinned") Boolean pinned,
            @JsonProperty("constraints") List<String> constraints,
            @JsonProperty("links") Map<String, URI> links) {
        super(name, type, description, defaultValue, reconfigurable, label, priority, possibleValues);
        this.pinned = pinned;
        this.constraints = (constraints == null) ? ImmutableList.<String>of() : ImmutableList.copyOf(constraints);
        this.links = (links == null) ? ImmutableMap.<String, URI>of() : ImmutableMap.copyOf(links);
    }

    public EntityConfigSummary(ConfigKey<?> config, String label, Double priority, Boolean pinned, Map<String, URI> links) {
        super(config, label, priority);
        this.pinned = pinned;
        this.constraints = !config.getConstraint().equals(Predicates.alwaysTrue())
                ? ImmutableList.of((config.getConstraint().getClass().equals(StringPredicates.isNonBlank().getClass()) ? "required" : config.getConstraint().toString()))
                : ImmutableList.<String>of();
        this.links = links != null ? ImmutableMap.copyOf(links) : null;
    }

    public Boolean isPinned() {
        return pinned;
    }

    public List<String> getConstraints() {
        return constraints;
    }

    @Override
    public Map<String, URI> getLinks() {
        return links;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EntityConfigSummary that = (EntityConfigSummary) o;
        if (pinned != that.pinned) return false;
        if (constraints != null ? !constraints.equals(that.constraints) : that.constraints != null) return false;
        return links != null ? links.equals(that.links) : that.links == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), links);
    }

    @Override
    public String toString() {
        return "EntityConfigSummary{" +
                "name='" + getName() + '\'' +
                ", type='" + getType() + '\'' +
                ", description='" + getDescription() + '\'' +
                "links=" + links +
                '}';
    }
}
