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

import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredType.TypeImplementationPlan;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class TypeDetail extends TypeSummary {

    public static class TypeImplementationPlanSummary {
        @JsonInclude(value=Include.NON_EMPTY)
        private String format;
        private Object data;
        private TypeImplementationPlanSummary() {}
        private TypeImplementationPlanSummary(TypeImplementationPlan p) {
            format = p.getPlanFormat();
            data = p.getPlanData();
        }
        public String getFormat() {
            return format;
        }
        public Object getData() {
            return data;
        }
    }
    private final TypeImplementationPlanSummary plan;
    
    /** Constructor for JSON deserialization use only. */
    TypeDetail() {
        plan = null;
    }
    
    public TypeDetail(RegisteredType t) {
        super(t);
        plan = new TypeImplementationPlanSummary(t.getPlan());
    }
    
    public TypeImplementationPlanSummary getPlan() {
        return plan;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((plan == null) ? 0 : plan.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeDetail other = (TypeDetail) obj;
        if (plan == null) {
            if (other.plan != null)
                return false;
        } else if (!plan.equals(other.plan))
            return false;
        return true;
    }
    
}
