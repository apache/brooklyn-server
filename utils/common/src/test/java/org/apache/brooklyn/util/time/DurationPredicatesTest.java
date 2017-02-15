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

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class DurationPredicatesTest {

    private static final List<Duration> TEST_DURATIONS_POSITIVE = Lists.newArrayList(null,
            Duration.minutes(1), Duration.minutes(2), Duration.minutes(5), Duration.minutes(10));

    private static final List<Duration> TEST_DURATIONS_NEGATIVE = Lists.newArrayList(null,
            Duration.minutes(-1), Duration.minutes(-2), Duration.minutes(-5), Duration.minutes(-10));

    private static final List<Stopwatch> TEST_STOPWATCHES = new ArrayList<>(TEST_DURATIONS_POSITIVE.size());

    @BeforeClass
    public static void setUp() throws Exception {
        for (Duration duration : TEST_DURATIONS_POSITIVE) {
            TEST_STOPWATCHES.add(createStopwatchWithElapsedTime(duration));
        }
    }

    @Test
    public void testPositive() {
        Iterable<Duration> result = Iterables.filter(TEST_DURATIONS_POSITIVE, DurationPredicates.positive());
        assertEquals(Iterables.size(result), Iterables.size(TEST_DURATIONS_POSITIVE) - 1);

        result = Iterables.filter(TEST_DURATIONS_NEGATIVE, DurationPredicates.positive());
        assertEquals(Iterables.size(result), 0);
    }

    @Test
    public void testNegative() {
        Iterable<Duration> result = Iterables.filter(TEST_DURATIONS_POSITIVE, DurationPredicates.negative());
        assertEquals(Iterables.size(result), 0);

        result = Iterables.filter(TEST_DURATIONS_NEGATIVE, DurationPredicates.negative());
        assertEquals(Iterables.size(result), Iterables.size(TEST_DURATIONS_NEGATIVE) - 1);
    }

    @Test
    public void testLongerThan() {
        Duration testDuration = Duration.minutes(3);

        Iterable<Duration> result = Iterables.filter(TEST_DURATIONS_POSITIVE,
                DurationPredicates.longerThan(testDuration));
        assertEquals(Iterables.size(result), 2);

        result = Iterables.filter(TEST_DURATIONS_NEGATIVE, DurationPredicates.longerThan(testDuration));
        assertEquals(Iterables.size(result), 0);

        testDuration = Duration.minutes(-3);

        result = Iterables.filter(TEST_DURATIONS_POSITIVE, DurationPredicates.longerThan(testDuration));
        assertEquals(Iterables.size(result), 4);

        result = Iterables.filter(TEST_DURATIONS_NEGATIVE, DurationPredicates.longerThan(testDuration));
        assertEquals(Iterables.size(result), 2);
    }

    @Test
    public void testShorterThan() {
        Duration testDuration = Duration.minutes(3);

        Iterable<Duration> result = Iterables.filter(TEST_DURATIONS_POSITIVE,
                DurationPredicates.shorterThan(testDuration));
        assertEquals(Iterables.size(result), 2);

        result = Iterables.filter(TEST_DURATIONS_NEGATIVE, DurationPredicates.shorterThan(testDuration));
        assertEquals(Iterables.size(result), 4);

        testDuration = Duration.minutes(-3);

        result = Iterables.filter(TEST_DURATIONS_POSITIVE, DurationPredicates.shorterThan(testDuration));
        assertEquals(Iterables.size(result), 0);

        result = Iterables.filter(TEST_DURATIONS_NEGATIVE, DurationPredicates.shorterThan(testDuration));
        assertEquals(Iterables.size(result), 2);
    }

    @Test
    public void testLongerThanDuration() {
        Duration testDuration = Duration.minutes(3);

        Iterable<Stopwatch> result = Iterables.filter(TEST_STOPWATCHES,
                DurationPredicates.longerThanDuration(testDuration));
        assertEquals(Iterables.size(result), 2);

        testDuration = Duration.minutes(-3);

        result = Iterables.filter(TEST_STOPWATCHES, DurationPredicates.longerThanDuration(testDuration));
        assertEquals(Iterables.size(result), 4);
    }

    @Test
    public void testShorterThanDuration() {
        Duration testDuration = Duration.minutes(3);

        Iterable<Stopwatch> result = Iterables.filter(TEST_STOPWATCHES,
                DurationPredicates.shorterThanDuration(testDuration));
        assertEquals(Iterables.size(result), 2);

        testDuration = Duration.minutes(-3);

        result = Iterables.filter(TEST_STOPWATCHES, DurationPredicates.shorterThanDuration(testDuration));
        assertEquals(Iterables.size(result), 0);
    }

    private static Stopwatch createStopwatchWithElapsedTime(Duration duration) throws Exception {
        if (duration == null) {
            return null;
        }

        Stopwatch stopwatch = Stopwatch.createUnstarted();

        Field field = stopwatch.getClass().getDeclaredField("elapsedNanos");
        field.setAccessible(true);
        field.set(stopwatch, duration.nanos());

        return stopwatch;
    }
}
