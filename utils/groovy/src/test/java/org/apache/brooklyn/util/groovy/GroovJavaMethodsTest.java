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
package org.apache.brooklyn.util.groovy;

import groovy.lang.Closure;
import groovy.lang.GString;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import static org.apache.brooklyn.util.groovy.GroovyJavaMethods.truth;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class GroovJavaMethodsTest {
    @Test
    public void testTruth() {
        assertTrue(truth("someString"));
        assertTrue(truth(1));
        assertFalse(truth(false));
        assertFalse(truth(null));
    }

    @Test
    public void testIsCase() throws Throwable {
        assertFalse(callScriptBytecodeAdapter_isCase(
                null,
                GString.class));
        assertTrue(
                callScriptBytecodeAdapter_isCase(
                        new GString(new String[]{"Hi"}) {
                            @Override public String[] getStrings() {
                                return new String[0];
                            }
                        },
                        GString.class));
        assertFalse(
                callScriptBytecodeAdapter_isCase(
                        "exampleString",
                        GString.class));

        assertTrue(
                callScriptBytecodeAdapter_isCase(
                        new Callable<Void>() {
                            @Override public Void call() {
                                return null;
                            }
                        },
                        Callable.class));
        assertFalse(
                callScriptBytecodeAdapter_isCase(
                        "exampleString",
                        Callable.class));

        assertTrue(
                callScriptBytecodeAdapter_isCase(
                        new Closure(null) {
                            @Override public Void call() {
                                return null;
                            }
                        },
                        Closure.class));
        assertFalse(
                callScriptBytecodeAdapter_isCase(
                        "exampleString",
                        Closure.class));
    }

    private boolean callScriptBytecodeAdapter_isCase(Object switchValue, Class caseExpression) throws Throwable {
//        return org.codehaus.groovy.runtime.ScriptBytecodeAdapter.isCase(switchValue, caseExpression);
        return org.apache.brooklyn.util.groovy.GroovyJavaMethods.safeGroovyIsCase(switchValue, caseExpression);
    }
}
