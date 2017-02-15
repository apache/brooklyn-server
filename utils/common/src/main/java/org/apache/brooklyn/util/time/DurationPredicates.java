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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;

public class DurationPredicates {

    /**
     * @return A {@link Predicate} that checks if a {@link Duration} supplied to
     *         {@link Predicate#apply(Object)} is positive.
     */
    public static Predicate<Duration> positive() {
        return new Positive();
    }

    protected static class Positive implements Predicate<Duration> {
        @Override
        public boolean apply(Duration input) {
            return input != null && input.isPositive();
        }
    }

    /**
     * @return A {@link Predicate} that checks if a {@link Duration} supplied to
     *         {@link Predicate#apply(Object)} is negative.
     */
    public static Predicate<Duration> negative() {
        return new Negative();
    }

    protected static class Negative implements Predicate<Duration> {
        @Override
        public boolean apply(Duration input) {
            return input != null && input.isNegative();
        }
    }

    /**
     * @param duration
     *            The {@link Duration} that will be the basis for comparison in
     *            the returned {@link Predicate}.
     * @return A {@link Predicate} that checks if a {@link Duration} supplied to
     *         {@link Predicate#apply(Object)} is longer than the
     *         {@link Duration} that was supplied to this method.
     */
    public static Predicate<Duration> longerThan(final Duration duration) {
        return new LongerThan(duration);
    }

    protected static class LongerThan implements Predicate<Duration> {
        private final Duration value;

        protected LongerThan(Duration value) {
            Preconditions.checkNotNull(value);
            this.value = value;
        }

        @Override
        public boolean apply(Duration input) {
            return input != null && input.isLongerThan(value);
        }
    }

    /**
     * @param duration
     *            The {@link Duration} that will be the basis for comparison in
     *            the returned {@link Predicate}.
     * @return A {@link Predicate} that checks if a {@link Duration} supplied to
     *         {@link Predicate#apply(Object)} is shorter than the
     *         {@link Duration} that was supplied to this method.
     */
    public static Predicate<Duration> shorterThan(final Duration duration) {
        return new ShorterThan(duration);
    }

    protected static class ShorterThan implements Predicate<Duration> {
        private final Duration value;

        protected ShorterThan(Duration value) {
            Preconditions.checkNotNull(value);
            this.value = value;
        }

        @Override
        public boolean apply(Duration input) {
            return input != null && input.isShorterThan(value);
        }
    }

    /**
     * @param duration
     *            The {@link Duration} that will be the basis for comparison in
     *            the returned {@link Predicate}.
     * @return A {@link Predicate} that checks if a {@link Stopwatch} supplied to
     *         {@link Predicate#apply(Object)} is longer than the
     *         {@link Duration} that was supplied to this method.
     */
    public static Predicate<Stopwatch> longerThanDuration(final Duration duration) {
        return new LongerThanDuration(duration);
    }

    protected static class LongerThanDuration implements Predicate<Stopwatch> {
        private final Duration value;

        protected LongerThanDuration(Duration value) {
            Preconditions.checkNotNull(value);
            this.value = value;
        }

        @Override
        public boolean apply(Stopwatch input) {
            return input != null && Duration.millis(input.elapsed(TimeUnit.MILLISECONDS)).isLongerThan(value);
        }
    }

    /**
     * @param duration
     *            The {@link Duration} that will be the basis for comparison in
     *            the returned {@link Predicate}.
     * @return A {@link Predicate} that checks if a {@link Stopwatch} supplied to
     *         {@link Predicate#apply(Object)} is shorter than the
     *         {@link Duration} that was supplied to this method.
     */
    public static Predicate<Stopwatch> shorterThanDuration(final Duration duration) {
        return new ShorterThanDuration(duration);
    }

    protected static class ShorterThanDuration implements Predicate<Stopwatch> {
        private final Duration value;

        protected ShorterThanDuration(Duration value) {
            Preconditions.checkNotNull(value);
            this.value = value;
        }

        @Override
        public boolean apply(Stopwatch input) {
            return input != null && Duration.millis(input.elapsed(TimeUnit.MILLISECONDS)).isShorterThan(value);
        }
    }
}
