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
import java.util.Map;
import java.util.Objects;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.javalang.JavaClassNames;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class EntitySummary implements HasId, HasName, Serializable {

    private static final long serialVersionUID = 100490507982229165L;

    private final String id;
    private final String name;
    private final String type;
    @JsonInclude(Include.NON_NULL)
    private final String catalogItemId;
    private final Map<String, URI> links;

    // not exported directly, but used to provide other top-level json fields
    // for specific types
    @JsonIgnore
    private final Map<String,Object> others = MutableMap.of();

    public EntitySummary(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("catalogItemId") String catalogItemId,
            @JsonProperty("links") Map<String, URI> links) {
        this.type = type;
        this.id = id;
        this.name = name;
        this.catalogItemId = catalogItemId;
        this.links = (links == null) ? ImmutableMap.<String, URI> of() : ImmutableMap.copyOf(links);
    }

    public String getType() {
        return type;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getCatalogItemId() {
        return catalogItemId;
    }

    public Map<String, URI> getLinks() {
        return links;
    }

    /** Mutable map of other top-level metadata included on this DTO (eg listing config keys or effectors) */ 
    @JsonAnyGetter 
    public Map<String,Object> getExtraFields() {
        return others;
    }
    @JsonAnySetter
    public void setExtraField(String name, Object value) {
        others.put(name, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntitySummary)) return false;
        EntitySummary that = (EntitySummary) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(catalogItemId, that.catalogItemId) &&
                Objects.equals(links, that.links);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type, catalogItemId, links);
    }

    @Override
    public String toString() {
        return JavaClassNames.simpleClassName(this)+"{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", catalogItemId='" + catalogItemId + '\'' +
                (!getExtraFields().isEmpty() ? ", others='"+getExtraFields()+"'" : "")+
                ", links=" + links +
                '}';
    }
}
