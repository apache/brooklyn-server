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

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

public class RelationSummary implements Serializable {

    private static final long serialVersionUID = -3273187304766851859L;

    private final RelationType type;
    private final Set<String> targets;

    public RelationSummary(
            @JsonProperty("type") RelationType type,
            @JsonProperty("targets") Set<String> targets) {
        this.type = type;
        this.targets = (targets == null) ? ImmutableSet.of() : ImmutableSet.copyOf(targets);
    }

    public RelationType getType() {
        return type;
    }

    public Set<String> getTargets() {
        return targets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RelationSummary)) return false;
        RelationSummary that = (RelationSummary) o;
        return Objects.equals(type, that.type)
                && Objects.equals(targets, that.targets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, targets);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("targets", targets)
                .toString();
    }
}
