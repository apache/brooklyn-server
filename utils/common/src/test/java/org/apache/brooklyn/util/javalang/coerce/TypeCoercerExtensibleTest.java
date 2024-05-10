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
package org.apache.brooklyn.util.javalang.coerce;

import org.apache.brooklyn.util.guava.TypeTokens;
import static org.testng.Assert.assertEquals;

import java.util.Objects;

import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.Test;

import com.google.common.base.MoreObjects;
import com.google.common.reflect.TypeToken;

public class TypeCoercerExtensibleTest {

    TypeCoercerExtensible coercer = TypeCoercerExtensible.newDefault();
    
    protected <T> T coerce(Object x, Class<T> type) {
        return coercer.coerce(x, type);
    }
    protected <T> T coerce(Object x, TypeToken<T> type) {
        return coercer.coerce(x, type);
    }
    
    @Test
    public void testRegisterNewGenericCoercer() {
        coercer.registerAdapter("test-"+Strings.makeRandomId(4), new TryCoercer() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) {
                if (input instanceof String && TypeTokens.equalsRaw(MyClazz.class, type)) {
                    return (Maybe<T>) Maybe.of(new MyClazz("myprefix"+input));
                } else {
                    return null;
                }
            }
        });
        
        assertEquals(coerce("abc", MyClazz.class), new MyClazz("myprefixabc"));
    }
    
    public static class MyClazz {
        private final String val;

        public MyClazz(String val) {
            this.val = val;
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(val);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MyClazz))  return false;
            return Objects.equals(val, ((MyClazz)obj).val);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("val", val).toString();
        }
    }

    void registerOrderedConditionalMyClassAdapter(int order, String requiredToken) {
        coercer.registerAdapter(""+order+"-"+requiredToken,
                new TryCoercer() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) {
                        if (input instanceof String && type.getRawType().isAssignableFrom(Integer.class) && ((String)input).contains(requiredToken))
                            return Maybe.of((T)(Object) (order*100+((String)input).length()));
                        return null;
                    }
                });
    }

    @Test
    public void testPriority() {
        registerOrderedConditionalMyClassAdapter(-1, "11");
        registerOrderedConditionalMyClassAdapter(1, "a");
        registerOrderedConditionalMyClassAdapter(2, "b");
        registerOrderedConditionalMyClassAdapter(3, "33");
        registerOrderedConditionalMyClassAdapter(-2, "22");

        assertEquals(coerce("a", Integer.class), (Integer) 101);
        assertEquals(coerce("ab", Integer.class), (Integer) 102);
        assertEquals(coerce("bb", Integer.class), (Integer) 202);
        assertEquals(coerce("33", Integer.class), (Integer) 302);
        assertEquals(coerce("ab11", Integer.class), (Integer) 104); // coercer 1-a applies
        assertEquals(coerce("d11", Integer.class), (Integer) (-97)); // coercer -1-11 applies
        assertEquals(coerce("d1122", Integer.class), (Integer) (-95)); // coercer -1-11 applies
        assertEquals(coerce("d2222", Integer.class), (Integer) (-195)); // coercer -2-22 applies
        assertEquals(coerce("33", Integer.class), (Integer) (302)); // coercer 3-33 applies before Integer.toString
        assertEquals(coerce("1122", Integer.class), (Integer) (1122)); // Integer.fromString coercion applies before negative coercers
    }
}
