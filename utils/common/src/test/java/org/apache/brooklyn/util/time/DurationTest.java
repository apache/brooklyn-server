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
package org.apache.brooklyn.util.time;

import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DurationTest {

    public void testMinutes() {
        Assert.assertEquals(3*60*1000, new Duration(3, TimeUnit.MINUTES).toMilliseconds());
    }

    public void testAdd() {
        Assert.assertEquals((((4*60+3)*60)+30)*1000,
            new Duration(3, TimeUnit.MINUTES).
                add(new Duration(4, TimeUnit.HOURS)).
                add(new Duration(30, TimeUnit.SECONDS)).
            toMilliseconds());
    }

    public void testStatics() {
        Assert.assertEquals((((4*60+3)*60)+30)*1000,
            Duration.ONE_MINUTE.multiply(3).
                add(Duration.ONE_HOUR.multiply(4)).
                add(Duration.THIRTY_SECONDS).
            toMilliseconds());
    }

    public void testParse() {
        Assert.assertEquals((((4*60+3)*60)+30)*1000,
                Duration.of("4h 3m 30s").toMilliseconds());
    }

    public void testParseNegative() {
        Assert.assertEquals(Duration.of("- 1 m 1 s"), Duration.seconds(-61));

        Assert.assertEquals(Duration.of("-42 m"), Duration.minutes(-42));
        Assert.assertEquals(Duration.of("-42m"), Duration.minutes(-42));
        Assert.assertEquals(Duration.of("- 1 m 1 s"), Duration.seconds(-61));
        Asserts.assertFailsWith(() -> Duration.of("-1m 1s"), e -> Asserts.expectedFailureContainsIgnoreCase(e, "negati", "space"));
        Asserts.assertFailsWith(() -> Duration.of("1m -1s"), e -> Asserts.expectedFailureContainsIgnoreCase(e, "negati", "individual time unit"));
        Asserts.assertFailsWith(() -> Duration.of("1m -1 s"), e -> Asserts.expectedFailureContainsIgnoreCase(e, "negati", "individual time unit"));
        Asserts.assertFailsWith(() -> Duration.of("- 1m - 1s"), e -> Asserts.expectedFailureContainsIgnoreCase(e, "negati", "individual time unit"));
    }

    public void testConvesion() {
        Assert.assertEquals(1, Duration.nanos(1).toNanoseconds());
        Assert.assertEquals(1, Duration.nanos(1.1).toNanoseconds());
        Assert.assertEquals(1, Duration.millis(1).toMilliseconds());
        Assert.assertEquals(1, Duration.millis(1.0).toMilliseconds());
        Assert.assertEquals(1, Duration.millis(1.1).toMilliseconds());
        Assert.assertEquals(1100000, Duration.millis(1.1).toNanoseconds());
        Assert.assertEquals(500, Duration.seconds(0.5).toMilliseconds());
    }

    public void testToString() {
        Assert.assertEquals("4h 3m 30s",
                Duration.of("4h 3m 30s").toString());
    }

    public void testToStringRounded() {
        Assert.assertEquals("4h 3m",
                Duration.of("4h 3m 30s").toStringRounded());
    }

    public void testParseToString() {
        Assert.assertEquals(Duration.of("4h 3m 30s"),
                Duration.parse(Duration.of("4h 3m 30s").toString()));
    }

    public void testRoundUp() {
        Assert.assertEquals(Duration.nanos(1).toMillisecondsRoundingUp(), 1);
    }

    public void testRoundZero() {
        Assert.assertEquals(Duration.ZERO.toMillisecondsRoundingUp(), 0);
    }

    public void testRoundUpNegative() {
        Assert.assertEquals(Duration.nanos(-1).toMillisecondsRoundingUp(), -1);
    }

    public void testNotRounding() {
        Assert.assertEquals(Duration.nanos(-1).toMilliseconds(), 0);
    }

    public void testNotRoundingNegative() {
        Assert.assertEquals(Duration.nanos(-1).toMillisecondsRoundingUp(), -1);
    }

    public void testComparison() {
        Assert.assertTrue(Duration.seconds(1.8).isLongerThan(Duration.millis(1600)));
        Assert.assertTrue(Duration.millis(1600).isShorterThan(Duration.seconds(1.8)));

        Assert.assertTrue(Duration.seconds(1).isLongerThan(Duration.ZERO));
        Assert.assertFalse(Duration.seconds(-1).isLongerThan(Duration.ZERO));
    }

    public void testIsPositive() {
        Assert.assertTrue(Duration.minutes(1).isPositive());
    }

    public void testOverflows() {
        Assert.assertEquals(Duration.PRACTICALLY_FOREVER.add(Duration.nanos(1)), Duration.PRACTICALLY_FOREVER);
        Assert.assertEquals(Duration.PRACTICALLY_FOREVER.subtract(Duration.nanos(-1)), Duration.PRACTICALLY_FOREVER);
        Assert.assertEquals(Duration.PRACTICALLY_FOREVER.multiply(2), Duration.PRACTICALLY_FOREVER);

        Duration justLessThanForever = Duration.PRACTICALLY_FOREVER.subtract(Duration.nanos(1));
        Asserts.assertThat(justLessThanForever, Duration::isPositive);
        Asserts.assertEquals(justLessThanForever.toString(), "forever");

        Assert.assertEquals(Duration.of(-Long.MAX_VALUE).toString(), "-forever");
        Assert.assertEquals(Duration.of(Long.MIN_VALUE).toString(), "-forever");
        Assert.assertEquals(Duration.of(Duration.ALMOST_PRACTICALLY_FOREVER).toString(), "forever");
        Assert.assertEquals(Duration.of(Duration.ALMOST_PRACTICALLY_FOREVER).multiply(-1).toString(), "-forever");

        // anything less than half of max value is NOT rounded to forever; beyond half of max value it might be, and close to max value is will be
        Assert.assertEquals(Duration.of(Duration.ALMOST_PRACTICALLY_FOREVER).multiply(-0.5d).add(Duration.nanos(1)).toString(), "-44250d 23h 53m 38s 427ms 387us 904ns");
    }

}
