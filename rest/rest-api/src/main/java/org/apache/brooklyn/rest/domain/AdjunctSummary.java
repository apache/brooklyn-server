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

import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.objs.HighlightTuple;
import org.apache.brooklyn.api.objs.Identifiable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.ImmutableMap;

public class AdjunctSummary implements HasName, Serializable, Identifiable {

    private static final long serialVersionUID = -8106551648118942612L;
    
    private String id;
    private String name;
    private BrooklynObjectType adjunctType;
    @JsonInclude(Include.NON_EMPTY)
    private String catalogItemId;
    private Status state;
    @JsonInclude(Include.NON_EMPTY)
    private Map<String, HighlightTuple> highlights;
    
    private Map<String, URI> links;

    // for json
    protected AdjunctSummary() {}
    
    public AdjunctSummary(EntityAdjunct a) {
        id = a.getId();
        name = a.getDisplayName();
        adjunctType = BrooklynObjectType.of(a);
        catalogItemId = a.getCatalogItemId();
        highlights = a.getHighlights();
    }
        
    /** @deprecated since 0.12.0 only for legacy type-specific summary classes */
    @Deprecated   
    protected AdjunctSummary(
            String id,
            String name,
            BrooklynObjectType adjunctType,
            String catalogItemId,
            Status state,
            Map<String, HighlightTuple> highlights,
            Map<String, URI> links) {
        this.id = id;
        this.name = name;
        this.adjunctType = adjunctType;
        this.catalogItemId = catalogItemId;
        this.state = state;
        this.highlights = (highlights == null) ? ImmutableMap.of() : ImmutableMap.copyOf(highlights);
        this.links = (links == null) ? ImmutableMap.<String, URI> of() : ImmutableMap.copyOf(links);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public BrooklynObjectType getAdjunctType() {
        return adjunctType;
    }
    
    public String getCatalogItemId() {
        return catalogItemId;
    }

    public Status getState() {
        return state;
    }

    public Map<String, HighlightTuple> getHighlights() {
        return highlights;
    }

    public Map<String, URI> getLinks() {
        return links;
    }
    
    public AdjunctSummary state(Status state) {
        this.state = state; return this;
    }
    
    public AdjunctSummary links(Map<String, URI> links) {
        this.links = links; return this;
    }

    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AdjunctSummary)) return false;
        AdjunctSummary that = (AdjunctSummary) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(adjunctType, that.adjunctType) &&
                Objects.equals(catalogItemId, that.catalogItemId) &&
                Objects.equals(state, that.state) &&
                Objects.equals(highlights, that.highlights) &&
                Objects.equals(links, that.links) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, adjunctType, catalogItemId, state, highlights, links);
    }

    @Override
    public String toString() {
        return (adjunctType!=null ? adjunctType.name() : "AdjunctSummary")+"{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", catalogItemId='" + catalogItemId + '\'' +
                ", state='" + state + '\'' +
                ", highlights=" + highlights +
                ", links=" + links +
                '}';
    }

}
