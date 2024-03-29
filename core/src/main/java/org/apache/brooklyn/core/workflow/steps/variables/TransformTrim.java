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
package org.apache.brooklyn.core.workflow.steps.variables;

import org.apache.brooklyn.util.collections.MutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TransformTrim extends WorkflowTransformDefault {
    @Override
    public Object apply(Object v) {
        if (v == null) return null;
        if (v instanceof String) return ((String) v).trim();
        if (v instanceof Set) return ((Set) v).stream().filter(TransformTrim::shouldTrimKeepInList).collect(Collectors.toSet());
        if (v instanceof Collection)
            return ((Collection) v).stream().filter(TransformTrim::shouldTrimKeepInList).collect(Collectors.toList());
        if (v instanceof Map) {
            Map<Object, Object> result = MutableMap.of();
            ((Map) v).forEach((k, vi) -> {
                if (k != null && vi != null) result.put(k, vi);
            });
            return result;
        }
        return v;
    }

    public static boolean shouldTrimKeepInList(Object x) {
        if (x==null) return false;
        if (x instanceof String) return !((String) x).isEmpty();
        return true;
    }

}
