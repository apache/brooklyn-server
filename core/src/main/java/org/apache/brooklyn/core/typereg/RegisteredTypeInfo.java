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
package org.apache.brooklyn.core.typereg;

import java.util.Set;

import org.apache.brooklyn.api.typereg.RegisteredType;

/** Includes metadata for types, if available. Varies by {@link BrooklynTypePlanTransformer},
 * with the main one (CAMP) being quite complete. */
public class RegisteredTypeInfo {
    
    private final RegisteredType type;
    
    private String planText;
    private Set<Object> supertypes;
    
    private RegisteredTypeInfo(RegisteredType type) {
        this.type = type;
    }
    
    public static RegisteredTypeInfo create(RegisteredType type, BrooklynTypePlanTransformer transformer,
            String planText, Set<Object> someSupertypes) {
        
        return new RegisteredTypeInfo(type);
    }

    public RegisteredType getType() {
        return type;
    }
    
    public String getPlanText() {
        return planText;
    }

    void setPlanText(String planText) {
        this.planText = planText;
    }

    // list of supertypes, as RegisteredType and Class instances
    public Set<Object> getSupertypes() {
        return supertypes;
    }

    void setSupertypes(Set<Object> supertypes) {
        this.supertypes = supertypes;
    }
    
}
