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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.parse.*;
import org.apache.brooklyn.camp.spi.resolve.PlanInterpreter;
import org.apache.brooklyn.camp.spi.resolve.PlanInterpreter.PlanInterpreterAdapter;
import org.apache.brooklyn.camp.spi.resolve.interpret.PlanInterpretationNode;
import org.apache.brooklyn.camp.spi.resolve.interpret.PlanInterpretationNode.Role;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PlanInterpreter} which understands the $brooklyn DSL
 */
public class BrooklynDslInterpreter extends PlanInterpreterAdapter {

    private static final Logger log = LoggerFactory.getLogger(BrooklynDslInterpreter.class);

    @Override
    public boolean isInterestedIn(PlanInterpretationNode node) {
        return node.matchesPrefix("$brooklyn:") || node.getNewValue() instanceof FunctionWithArgs;
    }

    private static ThreadLocal<PlanInterpretationNode> currentNode = new ThreadLocal<PlanInterpretationNode>();
    /** returns the current node, stored in a thread-local, to populate the dsl field of {@link BrooklynDslDeferredSupplier} instances */
    public static PlanInterpretationNode currentNode() {
        return currentNode.get();
    }
    /** sets the current node */
    public static void currentNode(PlanInterpretationNode node) {
        currentNode.set(node);
    }
    public static void currentNodeClear() {
        currentNode.set(null);
    }
    
    @Override
    public void applyYamlPrimitive(PlanInterpretationNode node) {
        String expression = node.getNewValue().toString();

        try {
            currentNode.set(node);
            Object parsedNode = new DslParser(expression).parse();
            if (parsedNode instanceof PropertyAccess) {
                parsedNode = new FunctionWithArgs(""+((PropertyAccess)parsedNode).getSelector(), null);
            }
            if ((parsedNode instanceof FunctionWithArgs) && ((FunctionWithArgs)parsedNode).getArgs()==null) {
                if (node.getRoleInParent() == Role.MAP_KEY) {
                    node.setNewValue(parsedNode);
                    // will be handled later
                } else {
                    throw new IllegalStateException("Invalid function-only expression '"+((FunctionWithArgs)parsedNode).getFunction()+"'");
                }
            } else {
                node.setNewValue( evaluate(parsedNode, true) );
            }
        } catch (Exception e) {
            // we could try parsing it as yaml and if it comes back as a map or a list, reapply the interpreter;
            // useful in some contexts where strings are required by the source (eg CFN, TOSCA)
            log.warn("Error evaluating node (rethrowing) '"+expression+"': "+e);
            Exceptions.propagateIfFatal(e);
            throw new IllegalArgumentException("Error evaluating node '"+expression+"'", e);
        } finally {
            currentNodeClear();
        }
    }
    
    @Override
    public boolean applyMapEntry(PlanInterpretationNode node, Map<Object, Object> mapIn, Map<Object, Object> mapOut,
            PlanInterpretationNode key, PlanInterpretationNode value) {
        Object knv = key.getNewValue();
        if (knv instanceof PropertyAccess) {
            // when property access is used as a key, it is a function without args
            knv = new FunctionWithArgs(""+((PropertyAccess)knv).getSelector(), null);
        }
        if (knv instanceof FunctionWithArgs) {
            try {
                currentNode.set(node);

                FunctionWithArgs f = (FunctionWithArgs) knv;
                if (f.getArgs()!=null)
                    throw new IllegalStateException("Invalid map key function "+f.getFunction()+"; should not have arguments if taking arguments from map");

                // means evaluation acts on values
                List<Object> args = new ArrayList<>();
                if (value.getNewValue() instanceof Iterable<?>) {
                    for (Object vi: (Iterable<?>)value.getNewValue())
                        args.add(vi);
                } else {
                    args.add(value.getNewValue());
                }

                try {
                    // TODO in future we should support functions of the form 'Maps.clear', 'Maps.reset', 'Maps.remove', etc;
                    // default approach only supported if mapIn has single item and mapOut is empty
                    if (mapIn.size()!=1) 
                        throw new IllegalStateException("Map-entry DSL syntax only supported with single item in map, not "+mapIn);
                    if (mapOut.size()!=0) 
                        throw new IllegalStateException("Map-entry DSL syntax only supported with empty output map-so-far, not "+mapOut);

                    node.setNewValue( evaluate(new FunctionWithArgs(f.getFunction(), args), false) );
                    return false;
                } catch (Exception e) {
                    log.warn("Error evaluating map-entry (rethrowing) '"+f.getFunction()+args+"': "+e);
                    Exceptions.propagateIfFatal(e);
                    throw new IllegalArgumentException("Error evaluating map-entry '"+f.getFunction()+args+"'", e);
                }

            } finally {
                currentNodeClear();
            }
        }
        return super.applyMapEntry(node, mapIn, mapOut, key, value);
    }

    public Object evaluate(Object f, boolean deepEvaluation) {
        if (f instanceof FunctionWithArgs) {
            return evaluateOn(BrooklynDslCommon.class, (FunctionWithArgs) f, deepEvaluation);
        }
        
        if (f instanceof List) {
            Object o = BrooklynDslCommon.class;
            for (Object i: (List<?>)f) {
                if (i instanceof FunctionWithArgs) {
                    o = evaluateOn(o, (FunctionWithArgs) i, deepEvaluation);
                } else if (i instanceof PropertyAccess) {
                    o = evaluateOn(o, (PropertyAccess) i);
                } else throw new IllegalArgumentException("Unexpected element in parse tree: '"+i+"' (type "+(i!=null ? i.getClass() : null)+")");
            }
            return o;
        }

        if (f instanceof QuotedString) {
            return ((QuotedString)f).unwrapped();
        }

        if (f instanceof PropertyAccess) {
            return ((PropertyAccess)f).getSelector();
        }

        throw new IllegalArgumentException("Unexpected element in parse tree: '"+f+"' (type "+(f!=null ? f.getClass() : null)+")");
    }
    
    public Object evaluateOn(Object o, FunctionWithArgs f, boolean deepEvaluation) {
        if (f.getArgs()==null)
            throw new IllegalStateException("Invalid function-only expression '"+f.getFunction()+"'");

        String fn = f.getFunction();
        fn = Strings.removeFromStart(fn, BrooklynDslCommon.PREFIX);
        if (fn.startsWith("function.")) {
            // If the function name starts with 'function.', then we look for the function in BrooklynDslCommon.Functions
            // As all functions in BrooklynDslCommon.Functions are static, we don't need to worry whether a class
            // or an instance was passed into this method
            o = BrooklynDslCommon.Functions.class;
            fn = Strings.removeFromStart(fn, "function.");
        }
        List<Object> args = new ArrayList<>();
        for (Object arg: f.getArgs()) {
            args.add( deepEvaluation ? evaluate(arg, true) : arg );
        }
        try {
            // TODO Could move argument resolve in DslDeferredFunctionCall freeing each Deffered implementation
            // having to handle it separately. The shortcoming is that will lose the eager evaluation we have here.
            if (o instanceof BrooklynDslDeferredSupplier && !(o instanceof DslFunctionSource)) {
                return new DslDeferredFunctionCall(o, fn, args);
            } else {
                // Would prefer to keep the invocation logic encapsulated in DslDeferredFunctionCall, but
                // for backwards compatibility will evaluate as much as possible eagerly (though it shouldn't matter in theory).
                return DslDeferredFunctionCall.invokeOn(o, fn, args).get();
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw Exceptions.propagate(new InvocationTargetException(e, "Error invoking '"+fn+"' on '"+o+"' with arguments "+args+""));
        }
    }

    public Object evaluateOn(Object o, PropertyAccess propAccess) {
        if(propAccess.getSelector() == null) {
            throw new IllegalStateException("Invalid property-selector expression!");
        }
        try {
            Object index = propAccess.getSelector();
            while (index instanceof PropertyAccess) {
                index = ((PropertyAccess)index).getSelector();
            }
            if (index instanceof QuotedString) {
                index = ((QuotedString)index).unwrapped();
            }
            return new DslDeferredPropertyAccess((BrooklynDslDeferredSupplier) o, index);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw Exceptions.propagate(new InvocationTargetException(e, "Error accessing property " + propAccess.getSelector() + " on " + o));
        }
    }
}
