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
import java.util.Map;

import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.HighlightTuple;

import com.fasterxml.jackson.annotation.JsonProperty;

/** @deprecated since 0.12.0 use {@link AdjunctSummary}; this class is identical */
@Deprecated
public class PolicySummary extends AdjunctSummary {

    private static final long serialVersionUID = -5086680835225136768L;

    public PolicySummary(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("catalogItemId") String catalogItemId,
            @JsonProperty("state") Status state,
            @JsonProperty("highlights") Map<String, HighlightTuple> highlights,
            @JsonProperty("links") Map<String, URI> links) {
        super(id, name, BrooklynObjectType.POLICY, catalogItemId, state, highlights, links);
    }

}
