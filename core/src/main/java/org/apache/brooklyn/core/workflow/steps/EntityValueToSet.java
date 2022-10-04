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
package org.apache.brooklyn.core.workflow.steps;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.brooklyn.api.entity.Entity;

import java.util.function.Consumer;

/** Deserialization bean allowing to specify a sensor or config on an entity */
public class EntityValueToSet extends TypedValueToSet {

    public EntityValueToSet() {}
    public EntityValueToSet(String name) {
        super(name);
    }
    public EntityValueToSet(TypedValueToSet other) {
        super(other);
        if (other instanceof EntityValueToSet) this.entity = ((EntityValueToSet)other).entity;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Entity entity;

    public static EntityValueToSet fromString(String name) {
        return new EntityValueToSet(name);
    }

}