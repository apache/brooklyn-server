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
package org.apache.brooklyn.core.validation;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class BrooklynValidationTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynValidationTest.class);

    protected void startWithF(Object o) {
        if (!o.toString().startsWith("f")) throw new IllegalArgumentException("Should start with the letter f");
    }

    @Test
    public void testValidatorOnSuccess() {
        Target s = new Target("foo");
        BrooklynValidation.getInstance().setValidatorForInstance(s, this::startWithF);
        Asserts.assertEquals(BrooklynValidation.getInstance().validateIfPresent(s), true);
        Asserts.assertEquals(BrooklynValidation.getInstance().ensureValid(s), s);
    }

    @Test
    public void testValidatorOnFailure() {
        Target s = new Target("bar");
        BrooklynValidation.getInstance().setValidatorForInstance(s, this::startWithF);
        Asserts.assertFailsWith(() -> BrooklynValidation.getInstance().validateIfPresent(s),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "the letter f"));
        Asserts.assertFailsWith(() -> BrooklynValidation.getInstance().ensureValid(s),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "the letter f"));
    }

    @Test
    public void testValidatorOnFailureSuppressed() {
        Target s = new Target("bar");
        BrooklynValidation.getInstance().setValidatorForInstance(s, this::startWithF);
        BrooklynValidation.getInstance().withValidationSuppressed(() -> {
            Asserts.assertEquals(BrooklynValidation.getInstance().validateIfPresent(s), false);
            return null;
        });
    }

    @Test
    public void testValidatorOnAbsence() {
        Target s = new Target("bar");
        BrooklynValidation.getInstance().setValidatorForInstance(s, this::startWithF);

        Target s2 = new Target("foo");
        Asserts.assertEquals(BrooklynValidation.getInstance().validateIfPresent(s2), false);
        Asserts.assertEquals(BrooklynValidation.getInstance().ensureValid(s2), s2);
    }

    @Test
    public void testValidatorOnEvilTwin() {
        Target s = new Target("foo");
        BrooklynValidation.getInstance().setValidatorForInstance(s, this::startWithF);

        // and if another object claims hash and object equality - that should be irrelevant
        Target s2 = new Target("bar");
        s2.hash = s.hashCode();
        s2.equalsToHashes.add(s.hashCode());
        Asserts.assertEquals(BrooklynValidation.getInstance().validateIfPresent(s2), false);
    }

    @Test
    public void testValidatorOnHashChange() {
        Target s = new Target("foo");
        BrooklynValidation.getInstance().setValidatorForInstance(s, this::startWithF);

        // and if another object claims hash and object equality - that should be irrelevant
        s.hash = s.hashCode() - 1;
        Asserts.assertEquals(BrooklynValidation.getInstance().validateIfPresent(s), true);
    }

    @Test(groups = "Integration")
    public void testValidatorIsWeak() throws InterruptedException, ExecutionException, TimeoutException {
        Set<Object> inputs = MutableSet.of();
        for (int i=0; i<1000; i++) {
            Object o = new Target("x");
            inputs.add(o);
            BrooklynValidation.getInstance().setValidatorForInstance(o, this::startWithF);
        }

        int s1 = BrooklynValidation.getInstance().size();
        System.gc();
        System.gc();
        //LOG.info("Memory: "+BrooklynGarbageCollector.makeBasicUsageString());
        Asserts.assertTrue(s1 >= 1000);
        inputs.clear();

        Asserts.assertReturnsEventually(() -> {
            while (true) {
                System.gc();
                System.gc();
                //LOG.info("Memory now: " + BrooklynGarbageCollector.makeBasicUsageString());
                if (BrooklynValidation.getInstance().size() <= s1 - 1000) return;
                Time.sleep(Duration.millis(50));
            }
        }, Duration.seconds(30));
    }

    static class Target {
        String input;
        Integer hash;
        Set<Integer> equalsToHashes = MutableSet.of();

        public Target(String input) { this.input = input; }

        @Override
        public String toString() {
            return input.toString();
        }

        @Override
        public int hashCode() {
            if (hash!=null) return hash;
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (equalsToHashes.contains(o.hashCode())) return true;
            return super.equals(o);
        }
    }

}
