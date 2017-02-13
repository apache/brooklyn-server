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
package org.apache.brooklyn.util.core.task;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.NoSuchElementException;

import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.ValueResolverTest.FailingImmediateAndDeferredSupplier;
import org.apache.brooklyn.util.core.task.ValueResolverTest.WrappingImmediateAndDeferredSupplier;
import org.apache.brooklyn.util.guava.Maybe;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;

public class ValueResolverIteratorTest extends BrooklynAppUnitTestSupport {
    enum ResolveType {
        IMMEDIATE(true),
        BLOCKING(false);

        private boolean immediate;

        ResolveType(boolean immediate) {
            this.immediate = immediate;
        }

        public boolean isImmediate() {
            return immediate;
        }
    }

    @DataProvider
    public Object[][] resolveTypes() {
        return new Object[][] {{ResolveType.IMMEDIATE}, {ResolveType.BLOCKING}};
    }

    @Test(dataProvider="resolveTypes")
    public void testIteratorFailing(ResolveType resolveType) {
        FailingImmediateAndDeferredSupplier failingExpected = new FailingImmediateAndDeferredSupplier();
        WrappingImmediateAndDeferredSupplier wrapperExpected = new WrappingImmediateAndDeferredSupplier(failingExpected);
        ValueResolver<Object> resolver = Tasks.resolving(wrapperExpected)
                .as(Object.class)
                .context(app)
                .immediately(resolveType.isImmediate());
        ValueResolverIterator<Object> iter = resolver.iterator();
        assertTrue(iter.hasNext(), "expected wrapper");
        Maybe<Object> wrapperActual = iter.next();
        assertEquals(wrapperActual.get(), wrapperExpected);
        assertEquals(wrapperActual, iter.peek());
        assertTrue(iter.hasNext());
        Maybe<Object> failingActual = iter.next();
        assertEquals(failingActual.get(), failingExpected);
        assertEquals(failingActual, iter.peek());
        assertTrue(iter.hasNext());
        Maybe<Object> absent = iter.next();
        assertTrue(absent.isAbsent());

        assertFalse(iter.hasNext(), "expected absent due to resolve failure");
        try {
            iter.next();
            Asserts.shouldHaveFailedPreviously("no more elements in iterator");
        } catch (NoSuchElementException e) {
            // expected
        }

        Maybe<Object> last = iter.nextOrLast(Predicates.alwaysTrue());
        assertTrue(last.isAbsent(), "expected absent because previous resolve failed");
        assertEquals(last, iter.peek());

        try {
            iter.remove();
            Asserts.shouldHaveFailedPreviously("can't remove elements");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test(dataProvider="resolveTypes")
    public void testIterator(ResolveType resolveType) {
        Integer o3ExpectedLast = Integer.valueOf(10);
        String o3Expected = o3ExpectedLast.toString(); //test coercion
        WrappingImmediateAndDeferredSupplier o2Expected = new WrappingImmediateAndDeferredSupplier(o3Expected);
        WrappingImmediateAndDeferredSupplier o1Expected = new WrappingImmediateAndDeferredSupplier(o2Expected);
        ValueResolver<Integer> resolver = Tasks.resolving(o1Expected)
                .as(Integer.class)
                .context(app)
                .immediately(resolveType.isImmediate());
        ValueResolverIterator<Integer> iter = resolver.iterator();
        assertTrue(iter.hasNext(), "expected o1");
        Maybe<Object> o1Actual = iter.next();
        assertEquals(o1Actual.get(), o1Expected);
        assertEquals(o1Actual, iter.peek());
        assertTrue(iter.hasNext());
        Maybe<Object> o2Actual = iter.next();
        assertEquals(o2Actual.get(), o2Expected);
        assertEquals(o2Actual, iter.peek());
        assertTrue(iter.hasNext());
        Maybe<Object> o3Actual = iter.next();
        assertEquals(o3Actual.get(), o3Expected);
        assertEquals(o3Actual, iter.peek());

        assertFalse(iter.hasNext(), "expected no more elements");
        try {
            iter.next();
            Asserts.shouldHaveFailedPreviously("no more elements in iterator");
        } catch (NoSuchElementException e) {
            // expected
        }

        Maybe<Object> lastUntyped = iter.nextOrLast(Predicates.alwaysTrue());
        assertTrue(lastUntyped.isPresent(), "expected present - all resolves successful");
        // Note these are NOT equal because peek doesn't coerce
        assertEquals(lastUntyped, iter.peek());
        assertEquals(lastUntyped.get(), o3Expected);
    }

    @Test(dataProvider="resolveTypes")
    public void testNull(ResolveType resolveType) {
        ValueResolver<Void> resolver = Tasks.resolving(null)
            .as(Void.class)
            .context(app)
            .immediately(resolveType.isImmediate());
        assertNull(resolver.get());
        ValueResolverIterator<Void> iter = resolver.iterator();
        assertTrue(iter.hasNext());
        assertNull(iter.next().get());
        assertFalse(iter.hasNext());

        Maybe<Object> voidOrLast = resolver.iterator().nextOrLast(Void.class);
        assertNull(voidOrLast.get());

        Maybe<Void> voidItem = resolver.iterator().next(Void.class);
        assertTrue(voidItem.isAbsent());

        Maybe<Void> lastItem = resolver.iterator().last();
        assertNull(lastItem.get());
    }

    @Test(dataProvider="resolveTypes")
    public void testNextOrLastFound(ResolveType resolveType) {
        FailingImmediateAndDeferredSupplier failingExpected = new FailingImmediateAndDeferredSupplier();
        WrappingImmediateAndDeferredSupplier wrapperExpected = new WrappingImmediateAndDeferredSupplier(failingExpected);
        ValueResolver<Object> resolver = Tasks.resolving(wrapperExpected)
                .as(Object.class)
                .context(app)
                .immediately(resolveType.isImmediate());
        ValueResolverIterator<Object> iter = resolver.iterator();
        Maybe<Object> actual = iter.nextOrLast(FailingImmediateAndDeferredSupplier.class);
        assertTrue(actual.isPresent());
        assertNotEquals(actual, iter.last());
    }

    @Test(dataProvider="resolveTypes")
    public void testNextOrLastNotFound(ResolveType resolveType) {
        FailingImmediateAndDeferredSupplier failingExpected = new FailingImmediateAndDeferredSupplier();
        WrappingImmediateAndDeferredSupplier wrapperExpected = new WrappingImmediateAndDeferredSupplier(failingExpected);
        ValueResolver<Object> resolver = Tasks.resolving(wrapperExpected)
                .as(Object.class)
                .context(app)
                .immediately(resolveType.isImmediate());
        ValueResolverIterator<Object> iter = resolver.iterator();
        Maybe<Object> actual = iter.nextOrLast(Void.class);
        assertFalse(actual.isPresent());
        assertEquals(actual, iter.last());
    }
    
    @Test(dataProvider="resolveTypes")
    public void testNextTypeFound(ResolveType resolveType) {
        FailingImmediateAndDeferredSupplier failingExpected = new FailingImmediateAndDeferredSupplier();
        WrappingImmediateAndDeferredSupplier wrapperExpected = new WrappingImmediateAndDeferredSupplier(failingExpected);
        ValueResolver<Object> resolver = Tasks.resolving(wrapperExpected)
                .as(Object.class)
                .context(app)
                .immediately(resolveType.isImmediate());
        ValueResolverIterator<Object> iter = resolver.iterator();
        Maybe<FailingImmediateAndDeferredSupplier> actual = iter.next(FailingImmediateAndDeferredSupplier.class);
        assertTrue(actual.isPresent());
        assertNotEquals(actual, iter.last());
    }

    @Test(dataProvider="resolveTypes")
    public void testNextTypeNotFound(ResolveType resolveType) {
        FailingImmediateAndDeferredSupplier failingExpected = new FailingImmediateAndDeferredSupplier();
        WrappingImmediateAndDeferredSupplier wrapperExpected = new WrappingImmediateAndDeferredSupplier(failingExpected);
        ValueResolver<Object> resolver = Tasks.resolving(wrapperExpected)
                .as(Object.class)
                .context(app)
                .immediately(resolveType.isImmediate());
        ValueResolverIterator<Object> iter = resolver.iterator();
        Maybe<Void> actual = iter.next(Void.class);
        assertFalse(actual.isPresent());
        assertEquals(actual, iter.last());
    }
}
