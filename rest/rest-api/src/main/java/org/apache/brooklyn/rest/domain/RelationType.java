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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

public class RelationType implements Serializable {

    private static final long serialVersionUID = -6467217117683357832L;

    private final String name;
    private final String target;
    private final String source;

    public RelationType(
            @JsonProperty("name") String name,
            @JsonProperty("target") String target,
            @JsonProperty("source") String source) {
        this.name = name;
        this.target = target;
        this.source = source;
    }

    public String getName() {
        return name;
    }
    public String getTarget() {
        return target;
    }
    public String getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RelationType)) return false;
        RelationType that = (RelationType) o;
        return Objects.equals(name, that.name)
                && Objects.equals(target, that.target)
                && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, target, source);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("target", target)
                .add("source", source)
                .toString();
    }
}
