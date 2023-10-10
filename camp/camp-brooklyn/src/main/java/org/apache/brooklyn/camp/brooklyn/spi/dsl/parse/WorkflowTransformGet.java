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
package org.apache.brooklyn.camp.brooklyn.spi.dsl.parse;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslInterpreter;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon.DslLiteral;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.steps.variables.WorkflowTransformDefault;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.text.TemplateProcessor;

public class WorkflowTransformGet extends WorkflowTransformDefault {

    String modifier;

    @Override
    protected void initCheckingDefinition() {
        List<String> d = MutableList.copyOf(definition.subList(1, definition.size()));
        if (d.size()>1) throw new IllegalArgumentException("Transform requires at most a single argument being the index or modifier to get");
        if (!d.isEmpty()) modifier = d.get(0);
    }

    @Override
    public Object apply(Object v) {
        String modifierResolved = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, modifier, String.class);
        if (modifierResolved==null) {
            if (v instanceof Supplier) return ((Supplier)v).get();
            return v;
        }
        modifierResolved = modifierResolved.trim();
        if (modifierResolved.startsWith("[") || modifierResolved.startsWith(".")) {
            // already in modifier form
        } else {
            if (modifierResolved.contains(".") || modifierResolved.contains("[") || modifierResolved.contains(" ")) {
                throw new IllegalArgumentException("Argument to 'get' must be a simple key (no spaces, dots, or brackets) or a bracketed string expression or start with an initial dot");
            } else {
                modifierResolved = "[\"" + modifierResolved+"\"]";
            }
        }

        String m = "$brooklyn:literal(\"ignored\")" + modifierResolved;
        List parse = (List) new DslParser(m).parse();
        parse = parse.subList(1, parse.size());
        // should be a bunch of property access
        BrooklynDslInterpreter ip = new BrooklynDslInterpreter();
        Object result = new DslLiteral(v);
        for (Object p: parse) {
            if (p instanceof PropertyAccess) {
                result = ip.evaluateOn(result, (PropertyAccess) p);
            } else {
                throw new IllegalArgumentException("Invalid entry in 'get' transform argument; should be property access/modifiers");
            }
        }
        return ((BrooklynDslDeferredSupplier) result).get();
    }

}
