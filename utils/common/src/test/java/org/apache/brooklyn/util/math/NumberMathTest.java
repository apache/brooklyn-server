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
package org.apache.brooklyn.util.math;

import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

public class NumberMathTest {

    @Test
    public void testVarious() {
        Asserts.assertEquals((int) new NumberMath<>(1).add(2), 3);
        Asserts.assertEquals((Object) new NumberMath<>(1).add(2), 3);

        Asserts.assertEquals(new NumberMath<>((Number)1).add(2.0d), 3);
        Asserts.assertFailsWith(() -> new NumberMath<>((Number)1).add(2.1d), e -> Asserts.expectedFailureContains(e, "Cannot cast 3.1 to class java.lang.Integer"));
        Asserts.assertFailsWith(() -> new NumberMath<>((Number)1).add(2.1d), e -> Asserts.expectedFailureContains(e, "Cannot cast 3.1 to class java.lang.Integer"));
        Asserts.assertEquals(new NumberMath<>(1, Number.class).add(2.0d), 3);
        Asserts.assertEquals(new NumberMath<>(1.1).add(2.0d), 3.1d);
        Asserts.assertEquals(new NumberMath<>((Number)1.1).add(2.1d), 3.2d);
        Asserts.assertThat(new NumberMath<Number>((byte)(-10)).add(4), x -> { Asserts.assertInstanceOf(x, Byte.class); Asserts.assertEquals(x, (byte) -6); return true; });

        // division must be exact
//        Asserts.assertEquals((Object) new NumberMath<>(3).divide(2), 1);
        Asserts.assertFailsWith(() -> new NumberMath<>(3).divide(2), e -> Asserts.expectedFailureContains(e, "Cannot cast 1.5 to class java.lang.Integer"));

        // if division might be double then cast to Number
        Asserts.assertEquals(new NumberMath(3, Number.class).divide(2), 1.5d);
    }

}
