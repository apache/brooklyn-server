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
package org.apache.brooklyn.entity.group;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class SequenceGroupImpl extends DynamicGroupImpl implements SequenceGroup {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceGroupImpl.class);

    public SequenceGroupImpl() { }

    @Override
    public Void reset() {
        synchronized (memberChangeMutex) {
            sensors().set(SEQUENCE_CACHE, Maps.<String, Integer>newConcurrentMap());
            Integer initial = config().get(SEQUENCE_START);
            sensors().set(SEQUENCE_NEXT, initial);
            return null;
        }
    }

    @Override
    public void rescanEntities() {
        synchronized (memberChangeMutex) {
            reset();
            super.rescanEntities();
        }
    }

    @Override
    public boolean addMember(Entity member) {
        synchronized (memberChangeMutex) {
            boolean changed = super.addMember(member);
            if (changed) {
                Map<String, Integer> cache = sensors().get(SEQUENCE_CACHE);
                if (!cache.containsKey(member.getId())) {
                    Integer value = sequence(member);
                    cache.put(member.getId(), value);
                }
            }
            return changed;
        }
    }

    private Integer sequence(Entity entity) {
        String format = config().get(SEQUENCE_FORMAT);
        Integer current = sensors().get(SEQUENCE_NEXT);
        String string = String.format(format, current);

        entity.sensors().set(SEQUENCE_VALUE, current);
        entity.sensors().set(SEQUENCE_STRING,string);

        Integer increment = config().get(SEQUENCE_INCREMENT);
        Integer next = current + increment;
        LOG.debug("Sequence for {} incremented to {}", this, next);

        sensors().set(SEQUENCE_NEXT, next);

        return current;
    }

}
