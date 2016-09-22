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
package org.apache.brooklyn.util.yoml.tests;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConstructionInstructionsTest {

    @Test
    public void testBasicNoArgs() {
        ConstructionInstruction c1 = ConstructionInstructions.Factory.newDefault(String.class, null);
        Assert.assertEquals(c1.create().get(), "");
    }
    
    @Test
    public void testBasicWithArgs() {
        ConstructionInstruction c1 = ConstructionInstructions.Factory.newUsingConstructorWithArgs(String.class, MutableList.of("x"), null);
        Assert.assertEquals(c1.create().get(), "x");
    }
    
    @Test
    public void testBasicArgsFromWrapper() {
        ConstructionInstruction c1 = ConstructionInstructions.Factory.newDefault(String.class, null);
        ConstructionInstruction c2 = ConstructionInstructions.Factory.newUsingConstructorWithArgs(null, MutableList.of("x"), c1);
        Assert.assertEquals(c2.create().get(), "x");
    }
    
    @Test
    public void testBasicArgsWrapped() {
        ConstructionInstruction c1 = ConstructionInstructions.Factory.newUsingConstructorWithArgs(null, MutableList.of("x"), null);
        ConstructionInstruction c2 = ConstructionInstructions.Factory.newDefault(String.class, c1);
        Assert.assertEquals(c2.create().get(), "x");
    }
    
    @Test
    public void testBasicArgsOverwrite() {
        ConstructionInstruction c1 = ConstructionInstructions.Factory.newUsingConstructorWithArgs(String.class, MutableList.of("x"), null);
        ConstructionInstruction c2 = ConstructionInstructions.Factory.newUsingConstructorWithArgs(null, MutableList.of("y"), c1);
        Assert.assertEquals(c2.create().get(), "x");
    }
        
}
