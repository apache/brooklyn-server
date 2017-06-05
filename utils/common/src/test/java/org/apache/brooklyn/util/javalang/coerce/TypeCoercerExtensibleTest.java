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

import static org.testng.Assert.assertEquals;

import java.util.Objects;

import org.apache.brooklyn.util.guava.Maybe;
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
        coercer.registerAdapter(new TryCoercer() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) {
                if (input instanceof String && type.getRawType() == MyClazz.class) {
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
}
