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

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;

public class DslParser {
    private final String expression;
    int index = -1;

    public DslParser(String expression) {
        this.expression = expression;
    }

    public synchronized Object parse() {
        if (index>=0)
            throw new IllegalStateException("Parser can only be used once");

        index++;
        Object result = next();

        if (index < expression.length())
            throw new IllegalStateException("Unexpected character at position "+index+" in "+expression);

        return result;
    }

    @SuppressWarnings("unchecked")
    public Object next() {
        int start = index;

        boolean isProperty = (index > 0 && (expression.charAt(index -1) == '[' )) || // for [x] syntax - indexes in lists
            (index > 1 && (expression.charAt(index -1) == '"' && expression.charAt(index -2) == '[')) ;  // for ["x"] syntax - string properties and map keys
        if (!isProperty) {
            skipWhitespace();
            if (index >= expression.length())
                throw new IllegalStateException("Unexpected end of expression to parse, looking for content since position " + start);

            if (expression.charAt(index) == '"') {
                // assume a string, that is why for property syntax using [x] or ["x"], we skip this part
                int stringStart = index;
                index++;
                do {
                    if (index >= expression.length())
                        throw new IllegalStateException("Unexpected end of expression to parse, looking for close quote since position " + stringStart);
                    char c = expression.charAt(index);
                    if (c == '"') break;
                    if (c == '\\') index++;
                    index++;
                } while (true);
                index++;
                return new QuotedString(expression.substring(stringStart, index));
            }
        }
        // not a string, must be a function (or chain thereof)
        List<Object> result = new MutableList<Object>();

        int fnStart = index;
        isProperty =  expression.charAt(fnStart) == '[';
        if(!isProperty) {
            do {
                if (index >= expression.length())
                    break;
                char c = expression.charAt(index);
                if (Character.isJavaIdentifierPart(c)) ;
                    // these chars also permitted
                else if (".:".indexOf(c)>=0) ;
                    // other things e.g. whitespace, parentheses, etc, skip
                else break;
                index++;
            } while (true);
        }
        String fn = isProperty ? "" : expression.substring(fnStart, index);
        if (fn.length()==0 && !isProperty)
            throw new IllegalStateException("Expected a function name or double-quoted string at position "+start);
        skipWhitespace();

        if (index < expression.length() && ( expression.charAt(index)=='(' || expression.charAt(index)=='[')) {
            // collect arguments
            int parenStart = index;
            List<Object> args = new MutableList<>();
            if (expression.charAt(index)=='[' && expression.charAt(index +1)=='"') { index ++;} // for ["x"] syntax needs to be increased to extract the name of the property correctly
            index ++;
            do {
                skipWhitespace();
                if (index >= expression.length())
                    throw new IllegalStateException("Unexpected end of arguments to function '"+fn+"', no close parenthesis matching character at position "+parenStart);
                char c = expression.charAt(index);
                if(isProperty && c =='"') { index++; break; } // increasing the index for ["x"] syntax to account for the presence of the '"'
                if (c==')'|| c == ']') break;
                if (c==',') {
                    if (args.isEmpty())
                        throw new IllegalStateException("Invalid character at position"+index);
                    index++;
                } else {
                    if (!args.isEmpty() && !isProperty)
                        throw new IllegalStateException("Expected , before position"+index);
                }
                args.add(next()); // call with first letter of the property
            } while (true);

            if (fn.isEmpty()) {
                Object arg = args.get(0);
                if (arg instanceof FunctionWithArgs) {
                    FunctionWithArgs holder = (FunctionWithArgs) arg;
                    if(holder.getArgs() == null || holder.getArgs().isEmpty()) {
                        result.add(new PropertyAccess(holder.getFunction()));
                    } else {
                        throw new IllegalStateException("Expected empty FunctionWithArgs - arguments present!");
                    }
                } else {
                    result.add(new PropertyAccess(arg));
                }
            } else {
                result.add(new FunctionWithArgs(fn, args));
            }

            index++;
            skipWhitespace();
            if (index >= expression.length())
                return result;
            char c = expression.charAt(index);
            if (c=='.') {
                // chained expression
                int chainStart = index;
                index++;
                Object next = next();
                if (next instanceof List) {
                    result.addAll((Collection<? extends FunctionWithArgs>) next);
                    return result;
                } else {
                    throw new IllegalStateException("Expected functions following position "+chainStart);
                }
            } else if (c=='[') {
                int selectorsStart = index;
                Object next = next();
                if (next instanceof List) {
                    result.addAll((Collection<? extends PropertyAccess>) next);
                    return result;
                } else {
                    throw new IllegalStateException("Expected property selectors following position "+selectorsStart);
                }
            } else {
                // following word not something handled at this level; assume parent will handle (or throw) - e.g. a , or extra )
                return result;
            }
        } else {
            // it is just a word; return it with args as null;
            return new FunctionWithArgs(fn, null);
        }
    }

    private void skipWhitespace() {
        while (index<expression.length() && Character.isWhitespace(expression.charAt(index)))
            index++;
    }

}