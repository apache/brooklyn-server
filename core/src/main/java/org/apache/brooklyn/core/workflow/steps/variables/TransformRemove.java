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

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransformRemove extends WorkflowTransformDefault {
    private String removal;

    @Override
    protected void initCheckingDefinition() {
        List<String> d = definition.subList(1, definition.size());
        if (d.size()!=1) throw new IllegalArgumentException("Transform 'remove' requires a single argument being the item (for a map) or index (for a list) to remove");
        removal = d.get(0);
    }

    @Override
    public Object apply(Object v) {
        if (v instanceof Map) {
            Map vm = MutableMap.copyOf((Map)v);
            Object removalResolved = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, removal);
            vm.remove(removalResolved);
            return vm;

        } else if (v instanceof Iterable) {
            List vl = MutableList.copyOf((Iterable) v);
            Integer removalIndex = TransformSlice.resolveAs(removal, context, "Argument", true, Integer.class, "an integer");
            if (removalIndex<0) removalIndex = vl.size() + removalIndex;
            if (removalIndex<0 || removalIndex>vl.size()) throw new IllegalArgumentException("Invalid removal index "+removalIndex+"; list is size "+vl.size());
            vl.remove((int)removalIndex);
            return vl;

        } else {
            throw new IllegalArgumentException("Invalid value for remove transform; expects a map or list, not "+(v==null ? "null" : v.getClass()));
        }
    }

}
