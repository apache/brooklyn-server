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

import org.apache.brooklyn.api.typereg.RegisteredType.TypeImplementationPlan;

import com.google.common.base.Objects;

public class BasicTypeImplementationPlan implements TypeImplementationPlan {
    final String format;
    final Object data;
    
    public BasicTypeImplementationPlan(String format, Object data) {
        this.format = format;
        this.data = data;
    }
    
    @Override
    public String getPlanFormat() {
        return format;
    }

    @Override
    public Object getPlanData() {
        return data;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + ((format == null) ? 0 : format.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BasicTypeImplementationPlan other = (BasicTypeImplementationPlan) obj;
        if (!Objects.equal(format, other.format)) return false;
        if (!Objects.equal(data, other.data)) return false;
        return true;
    }
    
}
