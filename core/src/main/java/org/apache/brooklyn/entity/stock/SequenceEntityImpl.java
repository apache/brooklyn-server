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
import org.apache.brooklyn.core.feed.ConfigToAttributes;

public class SequenceEntityImpl extends AbstractEntity implements SequenceEntity {

    public SequenceEntityImpl() { }

    public void init() {
        super.init();

        ConfigToAttributes.apply(this, SEQUENCE_NAME);
    }

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

    protected void sequence(Integer value) {
        String format = config().get(SEQUENCE_FORMAT);

        sensors().set(SEQUENCE_VALUE, value);
        sensors().set(SEQUENCE_STRING, String.format(format, value));
    }

    @Override
    public Integer currentValue() {
        return sensors().get(SEQUENCE_VALUE);
    }

    @Override
    public String currentString() {
        return sensors().get(SEQUENCE_STRING);
    }

    @Override
    public synchronized Integer nextValue() {
        increment();
        return currentValue();
    }

    @Override
    public synchronized String nextString() {
        increment();
        return currentString();
    }

    @Override
    public synchronized Void increment() {
        Integer increment = config().get(SEQUENCE_INCREMENT);
        Integer current = currentValue();
        sequence(current + increment);
        return null;
    }

    @Override
    public synchronized Void reset() {
        Integer start = config().get(SEQUENCE_START);
        sequence(start);
        return null;
    }

}
