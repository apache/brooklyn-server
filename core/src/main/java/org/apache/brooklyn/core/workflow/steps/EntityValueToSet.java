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

import org.apache.brooklyn.api.entity.Entity;

import java.util.function.Consumer;

/** Deserialization bean allowing to specify a sensor or config on an entity */
public class EntityValueToSet {

    public EntityValueToSet() {}
    public EntityValueToSet(String name) {
        this.name = name;
    }

    public String name;
    public String type;
    public Entity entity;

    public static EntityValueToSet parseFromShorthand(String expression, Consumer<Object> valueSetter) {
        String[] itemValue = expression.split("=", 2);
        if (itemValue.length!=2) {
            throw new IllegalArgumentException("Invalid shorthand '" + expression + "'; must be of the form `[TYPE] NAME = VALUE`. Equals is missing.");
        }
        valueSetter.accept(itemValue[1].trim());
        String[] optTypeName = itemValue[0].trim().split(" ", 2);

        EntityValueToSet result = new EntityValueToSet();
        if (optTypeName.length==1) {
            result.name = optTypeName[0];
        } else if (optTypeName.length==2) {
            result.type = optTypeName[0].trim();
            result.name = optTypeName[1].trim();
        } else {
            throw new IllegalArgumentException("Invalid shorthand '"+expression+"'; must be of the form `[TYPE] NAME = VALUE`. Too many words before the equals.");
        }
        return result;
    }
}