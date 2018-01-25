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
package org.apache.brooklyn.util.yoml.serializers;

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;

public class TypeFromOtherFieldBlackboard {

    public static final String KEY = TypeFromOtherFieldBlackboard.class.getCanonicalName();

    public static TypeFromOtherFieldBlackboard get(Map<Object,Object> blackboard) {
        Object v = blackboard.get(KEY);
        if (v==null) {
            v = new TypeFromOtherFieldBlackboard();
            blackboard.put(KEY, v);
        }
        return (TypeFromOtherFieldBlackboard) v;
    }
    
    private final Map<String,String> typeConstraintField = MutableMap.of();
    private final Map<String,Boolean> typeConstraintFieldIsReal = MutableMap.of();

    public void setTypeConstraint(String field, String typeField, boolean isReal) {
        typeConstraintField.put(field, typeField);
        typeConstraintFieldIsReal.put(field, isReal);
    }

    public String getTypeConstraintField(String field) {
        return typeConstraintField.get(field);
    }

    public boolean isTypeConstraintFieldReal(String f) {
        return typeConstraintFieldIsReal.get(f);
    }
    
}
