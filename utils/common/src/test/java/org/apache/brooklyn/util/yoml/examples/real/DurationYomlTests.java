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
package org.apache.brooklyn.util.yoml.examples.real;

import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.yoml.tests.YomlTestFixture;
import org.testng.annotations.Test;

public class DurationYomlTests {
    
    YomlTestFixture y = YomlTestFixture.newInstance().
        addTypeWithAnnotations(Duration.class);

    @Test 
    public void testReadBasic() {
        y.read("{ nanos: 1000000000 }", "duration")
         .assertResult(Duration.ONE_SECOND);
    }

    @Test 
    public void testReadNice() {
        y.read("1s", "duration")
         .assertResult(Duration.ONE_SECOND);
    }

    @Test 
    public void testOneSecond() {
        System.out.println("x:"+Duration.ONE_SECOND.toString());
        y.write(Duration.ONE_SECOND, "duration")
         .readLastWrite().assertLastsMatch()
         .assertLastWriteIgnoringQuotes("1s");
    }

    @Test 
    public void testZero() {
        y.write(Duration.ZERO, "duration")
         .readLastWrite().assertLastsMatch()
         .assertLastWriteIgnoringQuotes("0ms");
    }

    @Test 
    public void testForever() {
        y.write(Duration.PRACTICALLY_FOREVER, "duration")
         .readLastWrite().assertLastsMatch()
         .assertLastWriteIgnoringQuotes(Duration.PRACTICALLY_FOREVER_NAME);
    }

}
