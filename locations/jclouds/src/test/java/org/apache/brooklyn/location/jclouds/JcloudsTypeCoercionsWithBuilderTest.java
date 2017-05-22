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
package org.apache.brooklyn.location.jclouds;

import static org.apache.brooklyn.util.core.flags.TypeCoercions.coerce;
import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.coerce.ClassCoercionException;
import org.testng.annotations.Test;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

public class JcloudsTypeCoercionsWithBuilderTest {

    static {
        JcloudsTypeCoercions.init();
    }
    
    @Test
    public void testCallsBuilder() {
        assertEquals(
                coerce(ImmutableMap.of("arg1", "val1", "arg2", "val2"), MyClazz.class), 
                MyClazz.builder().arg1("val1").arg2("val2").build());
        assertEquals(
                coerce(ImmutableMap.of("arg2", "val2", "arg1", "val1"), MyClazz.class), 
                MyClazz.builder().arg1("val1").arg2("val2").build());
        assertEquals(
                coerce(ImmutableMap.of("arg1", "val1"), MyClazz.class), 
                MyClazz.builder().arg1("val1").build());
        assertEquals(
                coerce(ImmutableMap.of("arg2", "val2"), MyClazz.class), 
                MyClazz.builder().arg2("val2").build());
    }

    @Test
    public void testFailsIfExtraArgs() {
        try {
            coerce(ImmutableMap.of("arg1", "val1", "arg2", "val2", "arg3", "val3"), MyClazz.class);
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, "Builder for", "MyClazz", "failed to call method for arg3");
        }
    }

    @Test
    public void testFailsIfNoBuildMethod() {
        try {
            coerce(ImmutableMap.of("arg1", "val1"), MyClazzWithNoBuildMethod.class);
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, "Builder for", "MyClazzWithNoBuildMethod", "has no build() method");
        }
    }

    @Test
    public void testFailsIfNoNoargBuildMethod() {
        try {
            coerce(ImmutableMap.of("arg1", "val1"), MyClazzWithNoNoargBuildMethod.class);
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, "Builder for", "MyClazzWithNoNoargBuildMethod", "has no build() method");
        }
    }

    @Test
    public void testFailsIfNoNoargBuilderMethod() {
        try {
            coerce(ImmutableMap.of("arg1", "val1"), MyClazzWithNoNoargBuilderMethod.class);
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, "MyClazzWithNoNoargBuilderMethod", "no adapter known");
        }
    }

    @Test
    public void testCompositeOfCreatedObjs() {
        assertEquals(
                coerce(ImmutableMap.of("val",ImmutableMap.of("arg1", "val1", "arg2", "val2")), MyCompositeClazz.class), 
                MyCompositeClazz.builder().val((MyClazz.builder().arg1("val1").arg2("val2").build())).build());
    }

    public static class MyClazz {
        private final String arg1;
        private final String arg2;
        
        public static class Builder {
            private String arg1;
            private String arg2;
            
            public Builder arg1(String val) {
                this.arg1 = val;
                return this;
            }
            public Builder arg2(String val) {
                this.arg2 = val;
                return this;
            }
            public MyClazz build() {
                return new MyClazz(arg1, arg2);
            }
        }
            
        public static Builder builder() {
            return new Builder();
        }
        
        private MyClazz(String arg1, String arg2) {
            this.arg1 = arg1;
            this.arg2 = arg2;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) return false;
            MyClazz o = (MyClazz) obj;
            return Objects.equal(arg1, o.arg1) && Objects.equal(arg2, o.arg2);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(arg1, arg2);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("arg1", arg1).add("arg2", arg2).toString();
        }
    }
    
    public static class MyCompositeClazz {
        private final MyClazz val;
        
        public static class Builder {
            private MyClazz val;
            
            public Builder val(MyClazz val) {
                this.val = val;
                return this;
            }
            public MyCompositeClazz build() {
                return new MyCompositeClazz(val);
            }
        }
            
        public static Builder builder() {
            return new Builder();
        }
        
        private MyCompositeClazz(MyClazz val) {
            this.val = val;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) return false;
            MyCompositeClazz o = (MyCompositeClazz) obj;
            return Objects.equal(val, o.val);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(val);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("val", val).toString();
        }
    }
    
    public static class MyClazzWithNoNoargBuilderMethod {
        @SuppressWarnings("unused")
        private final String arg1;
        
        public static class Builder {
            private String arg1;
            
            public Builder arg1(String val) {
                this.arg1 = val;
                return this;
            }
            public MyClazzWithNoNoargBuilderMethod build() {
                return new MyClazzWithNoNoargBuilderMethod(arg1);
            }
        }
            
        public static Builder builder(String extraArg) {
            return new Builder();
        }
        
        private MyClazzWithNoNoargBuilderMethod(String arg1) {
            this.arg1 = arg1;
        }
    }

    public static class MyClazzWithNoBuildMethod {
        public static class Builder {
            public Builder arg1(String val) {
                return this;
            }
        }
            
        public static Builder builder() {
            return new Builder();
        }
        
        private MyClazzWithNoBuildMethod(String arg1) {
        }
    }
    
    public static class MyClazzWithNoNoargBuildMethod {
        public static class Builder {
            private String arg1;
            
            public Builder arg1(String val) {
                this.arg1 = val;
                return this;
            }
            public MyClazzWithNoNoargBuildMethod build(String extraArg) {
                return new MyClazzWithNoNoargBuildMethod(arg1);
            }
        }
            
        public static Builder builder() {
            return new Builder();
        }
        
        private MyClazzWithNoNoargBuildMethod(String arg1) {
        }
    }
}
