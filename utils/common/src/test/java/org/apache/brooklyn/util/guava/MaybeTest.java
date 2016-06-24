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
package org.apache.brooklyn.util.guava;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

public class MaybeTest {

    @Test
    public void testMaybeGet() {
        Maybe<String> m = Maybe.of("yes");
        Assert.assertTrue(m.isPresent());
        Assert.assertEquals(m.get(), "yes");
    }

    @Test
    public void testMaybeToOptional() {
        Maybe<String> m = Maybe.of("yes");
        Optional<String> o = m.toOptional();
        Assert.assertTrue(o.isPresent());
        Assert.assertEquals(o.get(), "yes");
    }
    
    @Test
    public void testMaybeGetNull() {
        Maybe<String> m = Maybe.ofAllowingNull(null);
        Assert.assertTrue(m.isPresent());
        Assert.assertEquals(m.get(), null);
    }

    @Test
    public void testMaybeDefaultAllowsNull() {
        // but note you have to cast it if you try to do it explicitly
        // (of course normally it's a variable...)
        Maybe<Object> m = Maybe.of((Object)null);
        Assert.assertTrue(m.isPresent());
        Assert.assertEquals(m.get(), null);
    }

    @Test
    public void testMaybeDisallowingNull() {
        Maybe<Object> m = Maybe.ofDisallowingNull(null);
        try {
            m.get();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "null");
        }
    }

    @Test
    public void testMaybeToOptionalFailsOnNull() {
        Maybe<String> m = Maybe.ofAllowingNull(null);
        try {
            m.toOptional();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, NullPointerException.class);
        }
    }
    
    @Test
    public void testMaybeAbsent() {
        Maybe<String> m = Maybe.absent("nope");
        Assert.assertFalse(m.isPresent());
        assertGetFailsContaining(m, "nope");
    }

    protected Exception assertGetFailsContaining(Maybe<?> m, String phrase) {
        try {
            getInExplicitMethod(m);
            throw Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, phrase);
            return e;
        }
    }
    
    @Test
    public void testMaybeAbsentToOptional() {
        Maybe<String> m = Maybe.absent("nope");
        Optional<String> o = m.toOptional();
        Assert.assertFalse(o.isPresent());
        try {
            o.get();
            throw Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "absent");
        }
    }

    // --- now check traces ----
    
    /** an explicit method we can search for in the trace */
    protected <T> T getInExplicitMethod(Maybe<T> m) {
        return m.get();
    }
    
    protected boolean containsClassAndMethod(StackTraceElement[] stackTrace, String className, String methodName) {
        boolean methodFound = (methodName==null);
        boolean classFound = (className==null);
        for (StackTraceElement element: stackTrace) {
            if (className!=null && className.equals(element.getClassName())) classFound = true;
            if (methodName!=null && methodName.equals(element.getMethodName())) methodFound = true;
        }
        return methodFound && classFound;
    }
    protected boolean containsGetExplicitMethod(Throwable e) {
        return containsClassAndMethod(e.getStackTrace(), MaybeTest.class.getName(), "getInExplicitMethod");
    }
    protected boolean containsMaybeMethod(Throwable e, String methodName) {
        return containsClassAndMethod(e.getStackTrace(), Maybe.class.getName(), methodName);
    }
    
    @Test
    public void testMaybeAbsentMessageIncludesSource() {
        Maybe<Object> m = Maybe.absent("nope");
        Assert.assertFalse(m.isPresent());
        Exception e = assertGetFailsContaining(m, "nope");
        
        e.printStackTrace();
        Assert.assertTrue(containsGetExplicitMethod(e), "Outer trace should be in explicit method");
        Assert.assertFalse(containsMaybeMethod(e, "absent"), "Outer trace should not be from 'absent'");
        
        Assert.assertFalse(containsGetExplicitMethod(e.getCause()), "Inner trace should not be in explicit method");
        Assert.assertTrue(containsMaybeMethod(e.getCause(), "absent"), "Inner trace should be from 'absent'");
    }

    @Test
    public void testMaybeAbsentMessageNoTraceDoesNotIncludeSource() {
        Maybe<Object> m = Maybe.absentNoTrace("nope");
        Assert.assertFalse(m.isPresent());
        Exception e = assertGetFailsContaining(m, "nope");
        Assert.assertTrue(containsGetExplicitMethod(e), "Outer trace should be in explicit method");
        Assert.assertFalse(containsMaybeMethod(e, "absentNoTrace"), "Outer trace should not be from 'absentNoTrace'");
        Assert.assertNull(e.getCause());
    }

    protected Exception newRE(String message) { return new UserFacingException(message); }
    
    @Test
    public void testMaybeAbsentThrowableIncludesGivenSourceOnly() {
        Maybe<Object> m = Maybe.absent(newRE("nope"));
        Assert.assertFalse(m.isPresent());
        Exception e = assertGetFailsContaining(m, "nope");
        Assert.assertTrue(e instanceof IllegalStateException, "Outer should be ISE, not "+e);
        Assert.assertTrue(containsGetExplicitMethod(e), "Outer trace should be in explicit method");
        Assert.assertFalse(containsMaybeMethod(e, "absent"), "Outer trace should not be from 'absent'");
        Assert.assertFalse(containsMaybeMethod(e, "newRE"), "Outer trace should not be 'newRE'");

        Assert.assertTrue(e.getCause() instanceof UserFacingException, "Inner should be UFE, not "+e.getCause());
        Assert.assertFalse(containsGetExplicitMethod(e.getCause()), "Inner trace should not be in explicit method");
        Assert.assertFalse(containsMaybeMethod(e.getCause(), "absent"), "Inner trace should not be from 'absent'");
        Assert.assertTrue(containsClassAndMethod(e.getCause().getStackTrace(), MaybeTest.class.getName(), "newRE"), "Inner trace SHOULD be from 'newRE'");
    }

}
