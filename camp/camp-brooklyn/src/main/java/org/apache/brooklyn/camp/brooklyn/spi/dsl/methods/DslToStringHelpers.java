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

import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;

/** Helper methods for producing toString outputs for DSL components, with the $brooklyn: prefix in the right place.
 * In general, component's toString methods should produce re-parseable output, normally using these methods. */
public class DslToStringHelpers {
    
    /** concatenates the arguments, each appropriately dsl-tostringed with prefixes removed,
     * and then prefixes the result */
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
    public static String fn(String functionName, Iterable<Object> args) {
        StringBuilder out = new StringBuilder();
        out.append(BrooklynDslCommon.PREFIX);
        out.append(functionName);
        out.append("(");
        if (args!=null) {
            boolean nonFirst = false;
            for (Object s: args) {
                if (nonFirst) out.append(", ");
                out.append(internal(s));
                nonFirst = true;
            }
        }
        out.append(")");
        return out.toString();
    }

    public static String fn(String functionName, Object ...args) {
        return fn(functionName, Arrays.asList(args));
    }
    
    /** convenience for internal arguments, removing any prefix, and applying wrap/escapes and other conversions */
    public static String internal(Object x) {
        if (x==null) return "null";
        if (x instanceof String) return JavaStringEscapes.wrapJavaString((String)x);
        if (x instanceof BrooklynObject) return fn("entity", MutableList.of( ((BrooklynObject)x).getId() ));
        return Strings.removeFromStart(x.toString(), BrooklynDslCommon.PREFIX);
    }

    public static String component(DslComponent component, String remainder) {
        return component==null || component.getScope()==Scope.THIS ? remainder : concat(internal(component), ".", remainder);
    }
}
