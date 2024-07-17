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
package org.apache.brooklyn.camp.brooklyn.spi.dsl.methods;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;

/** Helper methods for producing toString outputs for DSL components, with the $brooklyn: prefix in the right place.
 * In general, component's toString methods should produce re-parseable output, normally using these methods. */
public class DslToStringHelpers {
    
    /** concatenates the arguments, each appropriately dsl-tostringed with prefixes removed,
     * and then prefixes the result */
    // no longer needed now that we use chain... functions
    public static String concat(String ...x) {
        StringBuilder r = new StringBuilder();
        for (String xi: x) {
            r.append(Strings.removeFromStart(xi, BrooklynDslCommon.PREFIX));
        }
        if (r.length()==0) {
            return "";
        }
        return BrooklynDslCommon.PREFIX+r.toString();
    }

    /** convenience for functions, inserting parentheses and commas */
    public static String fn(boolean yamlAllowed, String functionName, Iterable<?> args) {
        return fn(yamlAllowed, true, functionName, args);
    }
    public static String fn(boolean yamlAllowed, boolean prefixWanted, String functionName, Iterable<?> args) {
        try {
            StringBuilder out = new StringBuilder();
            if (prefixWanted) out.append(BrooklynDslCommon.PREFIX);
            out.append(functionName);
            out.append("(");
            if (args != null) {
                boolean nonFirst = false;
                for (Object s : args) {
                    if (nonFirst) out.append(", ");
                    out.append(internal(false, s));
                    nonFirst = true;
                }
            }
            out.append(")");
            return out.toString();
        } catch (YamlSyntaxRequired e) {
            if (!yamlAllowed) throw e;

            StringBuilder out = new StringBuilder();
            out.append("{ ");
            if (prefixWanted) out.append(BrooklynDslCommon.PREFIX);
            out.append(functionName);
            out.append(": [ ");
            if (args != null) {
                boolean nonFirst = false;
                for (Object s : args) {
                    if (nonFirst) out.append(", ");
                    out.append(internal(true, s));
                    nonFirst = true;
                }
            }
            out.append("] }");
            return out.toString();
        }
    }

    public static String fn(boolean yamlAllowed, String functionName) {
        return fn(yamlAllowed, functionName, Collections.emptyList());
    }
    public static String fn1(boolean yamlAllowed, String functionName, Object arg1) {
        return fn(yamlAllowed, functionName, Collections.singletonList(arg1));
    }
    public static String fn(boolean yamlAllowed, String functionName, Object arg1, Object arg2, Object ...args) {
        return fn(yamlAllowed, functionName, MutableList.of(arg1, arg2).appendAll(Arrays.asList(args)));
    }

    public static class YamlSyntaxRequired extends RuntimeException {}

    /** convenience for internal arguments, removing any prefix, and applying wrap/escapes and other conversions */
    public static String internal(boolean yamlAllowed, Object x) {
        return internal(yamlAllowed, false, x);
    }
    public static String internal(boolean yamlAllowed, boolean prefixWanted, Object x) {
        if (x==null) return "null";
        if (x instanceof String) return JavaStringEscapes.wrapJavaString((String)x);
        if (x instanceof BrooklynObject) return fn(yamlAllowed, "entity", MutableList.of( ((BrooklynObject)x).getId() ));

        // these may not produce valid yaml, if used from within a function
        if (!yamlAllowed) {
            if (x instanceof Iterable || x instanceof Map) throw new YamlSyntaxRequired();
        }
        if (x instanceof Iterable) return "[ " + MutableList.copyOf((Iterable)x).stream().map(xi -> internal(yamlAllowed, true, xi)).collect(Collectors.joining(", ")) + " ]";
        if (x instanceof Map) return "{ " + ((Map<?,?>)x).entrySet().stream().map(entry ->
                DslToStringHelpers.internal(yamlAllowed, entry.getKey())+": "+DslToStringHelpers.internal(yamlAllowed, true, entry.getValue())).collect(Collectors.joining(", ")) + " }";

        String v;
        if (x instanceof BrooklynDslDeferredSupplier) {
            v = ((BrooklynDslDeferredSupplier<?>) x).toDslString(yamlAllowed);
        } else {
            v = x.toString();
        }
        if (!prefixWanted) v = Strings.removeFromStart(v, BrooklynDslCommon.PREFIX);
        return v;
    }

    public static String chainFunctionOnComponent(boolean yamlAllowed, DslComponent component, String fnNameNoArgs) {
        return chainFunctionOnComponent(yamlAllowed, component, fnNameNoArgs, Collections.emptyList());
    }
    public static String chainFunctionOnComponent(boolean yamlAllowed, DslComponent component, String fnName, Object arg1, Object arg2, Object ...args) {
        return chainFunctionOnComponent(yamlAllowed, component, fnName, MutableList.of(arg1, arg2).appendAll(Arrays.asList(args)));
    }
    public static String chainFunctionOnComponent1(boolean yamlAllowed, DslComponent component, String fnName, Object arg1) {
        return chainFunctionOnComponent(yamlAllowed, component, fnName, Collections.singletonList(arg1));
    }
    public static String chainFunctionOnComponent(boolean yamlAllowed, DslComponent component, String fnName, List<Object> args) {
        if (component==null || component.getScope()==Scope.THIS) {
            // ignore component
            return DslToStringHelpers.fn(yamlAllowed, fnName, args);
        }
        return chainFunction(yamlAllowed, component, fnName, args);
    }

    public static String chainFunction(boolean yamlAllowed, Object object, String fnName, List<?> args) {
        try {
            return DslToStringHelpers.fn(false, DslToStringHelpers.internal(false, object) + "." + fnName, args);
        } catch (YamlSyntaxRequired e) {
            if (!yamlAllowed) throw e;
            return "{ $brooklyn:chain: [ "+
                    DslToStringHelpers.internal(true, true, object) + ", " +
                    "{ "+fnName+": "+internal(true, args) + " } ] }";
        }
    }

}
