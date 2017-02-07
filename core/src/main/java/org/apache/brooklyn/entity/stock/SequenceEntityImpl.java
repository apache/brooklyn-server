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

import java.util.Collection;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceEntityImpl extends AbstractEntity implements SequenceEntity {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceEntity.class);

    private Object mutex = new Object();

    public SequenceEntityImpl() { }

    @Override
    public void start(Collection<? extends Location> locations) {
        addLocations(locations);
        reset();
        sensors().set(SERVICE_UP, Boolean.TRUE);
    }

    @Override
    public void stop() {
        sensors().set(SERVICE_UP, Boolean.FALSE);
    }

    @Override
    public void restart() {
        stop();
        start(getLocations());
    }

    @Override
    public Integer currentValue() {
        synchronized (mutex) {
            return sensors().get(SEQUENCE_VALUE);
        }
    }

    @Override
    public String currentString() {
        synchronized (mutex) {
            return sensors().get(SEQUENCE_STRING);
        }
    }

    @Override
    public Integer nextValue() {
        synchronized (mutex) {
            increment();
            return currentValue();
        }
    }

    @Override
    public String nextString() {
        synchronized (mutex) {
            increment();
            return currentString();
        }
    }

    @Override
    public Void increment() {
        synchronized (mutex) {
            Integer increment = config().get(SEQUENCE_INCREMENT);
            Integer current = currentValue();
            sequence(current + increment);
            return null;
        }
    }

    @Override
    public Void reset() {
        synchronized (mutex) {
            Integer start = config().get(SEQUENCE_START);
            sequence(start);
            return null;
        }
    }

    private void sequence(Integer value) {
        String format = config().get(SEQUENCE_FORMAT);
        String string = String.format(format, value);

        sensors().set(SEQUENCE_VALUE, value);
        sensors().set(SEQUENCE_STRING, string);

        LOG.debug("Sequence for {} set to {}", this, value);
    }

}
