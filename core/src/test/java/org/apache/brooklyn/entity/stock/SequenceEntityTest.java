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
package org.apache.brooklyn.entity.stock;

import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEquals;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;
import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SequenceEntityTest extends BrooklynAppUnitTestSupport {

    private SimulatedLocation loc1;
    private SequenceEntity sequence;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc1 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
    }

    @Test
    public void testSequenceInitial() throws Exception {
        sequence = app.addChild(EntitySpec.create(SequenceEntity.class)
                .configure(SequenceEntity.SEQUENCE_START, 0));
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(sequence, Startable.SERVICE_UP, true);

        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 0);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "0");
    }

    @Test
    public void testSequenceInitialConfig() throws Exception {
        sequence = app.addChild(EntitySpec.create(SequenceEntity.class)
                .configure(SequenceEntity.SEQUENCE_START, 42)
                .configure(SequenceEntity.SEQUENCE_FORMAT, "id-%02x"));
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(sequence, Startable.SERVICE_UP, true);

        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 42);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "id-2a");
    }

    @Test
    public void testSequenceIncrementEffector() throws Exception {
        sequence = app.addChild(EntitySpec.create(SequenceEntity.class));
        app.start(ImmutableList.of(loc1));

        EntityAsserts.assertAttributeEqualsEventually(sequence, Startable.SERVICE_UP, true);

        assertEquals(sequence.currentValue(), Integer.valueOf(1));
        assertEquals(sequence.currentString(), "1");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 1);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "1");

        sequence.increment();

        assertEquals(sequence.currentValue(), Integer.valueOf(2));
        assertEquals(sequence.currentString(), "2");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 2);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "2");

        sequence.invoke(SequenceEntity.INCREMENT, ImmutableMap.<String, Object>of()).getUnchecked();

        assertEquals(sequence.currentValue(), Integer.valueOf(3));
        assertEquals(sequence.currentString(), "3");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 3);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "3");
    }

    @Test
    public void testSequenceIncrementEffectorConfig() throws Exception {
        sequence = app.addChild(EntitySpec.create(SequenceEntity.class)
                .configure(SequenceEntity.SEQUENCE_START, 0)
                .configure(SequenceEntity.SEQUENCE_INCREMENT, 2)
                .configure(SequenceEntity.SEQUENCE_FORMAT, "%03d"));
        app.start(ImmutableList.of(loc1));

        EntityAsserts.assertAttributeEqualsEventually(sequence, Startable.SERVICE_UP, true);

        assertEquals(sequence.currentValue(), Integer.valueOf(0));
        assertEquals(sequence.currentString(), "000");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 0);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "000");

        sequence.increment();

        assertEquals(sequence.currentValue(), Integer.valueOf(2));
        assertEquals(sequence.currentString(), "002");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 2);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "002");

        sequence.invoke(SequenceEntity.INCREMENT, ImmutableMap.<String, Object>of()).getUnchecked();

        assertEquals(sequence.currentValue(), Integer.valueOf(4));
        assertEquals(sequence.currentString(), "004");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 4);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "004");
    }

    @Test
    public void testSequenceNextEffectors() throws Exception {
        sequence = app.addChild(EntitySpec.create(SequenceEntity.class));
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(sequence, Startable.SERVICE_UP, true);

        assertEquals(sequence.currentValue(), Integer.valueOf(1));
        assertEquals(sequence.currentString(), "1");

        Integer nextValue = sequence.invoke(SequenceEntity.NEXT_VALUE, ImmutableMap.<String, Object>of()).getUnchecked();
        assertEquals(nextValue, Integer.valueOf(2));

        String nextString = sequence.invoke(SequenceEntity.NEXT_STRING, ImmutableMap.<String, Object>of()).getUnchecked();
        assertEquals(nextString, "3");

        assertEquals(sequence.currentValue(), Integer.valueOf(3));
        assertEquals(sequence.currentString(), "3");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 3);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "3");
    }

    @Test
    public void testSequenceReset() throws Exception {
        sequence = app.addChild(EntitySpec.create(SequenceEntity.class));
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(sequence, Startable.SERVICE_UP, true);

        assertEquals(sequence.currentValue(), Integer.valueOf(1));

        sequence.increment();
        sequence.increment();
        sequence.increment();

        assertEquals(sequence.currentValue(), Integer.valueOf(4));
        assertEquals(sequence.currentString(), "4");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 4);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "4");

        sequence.invoke(SequenceEntity.RESET, ImmutableMap.<String, Object>of()).getUnchecked();

        assertEquals(sequence.currentValue(), Integer.valueOf(1));
        assertEquals(sequence.currentString(), "1");
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_VALUE, 1);
        assertAttributeEquals(sequence, SequenceEntity.SEQUENCE_STRING, "1");
    }

}
