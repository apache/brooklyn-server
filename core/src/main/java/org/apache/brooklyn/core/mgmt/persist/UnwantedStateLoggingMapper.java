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
package org.apache.brooklyn.core.mgmt.persist;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.brooklyn.api.mgmt.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thoughtworks.xstream.mapper.Mapper;
import com.thoughtworks.xstream.mapper.MapperWrapper;

public class UnwantedStateLoggingMapper extends MapperWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(UnwantedStateLoggingMapper.class);
    private static final AtomicLong WARN_CNT = new AtomicLong();

    public UnwantedStateLoggingMapper(Mapper wrapped) {
        super(wrapped);
    }

    @Override
    public String serializedClass(@SuppressWarnings("rawtypes") Class type) {
        logIfInteresting(type);
        return super.serializedClass(type);
    }

    private void logIfInteresting(Class<?> type) {
        if (type != null) {
            if (Task.class.isAssignableFrom(type)) {
                long cnt = WARN_CNT.getAndIncrement();
                if (cnt < 5 || cnt % 10000 == 0) {
                    LOG.warn("Trying to serialize a Task object of type " + type + ". " +
                            "Task object serialization is not supported or recommended. " +
                            "Check if the Task object is set as a config or sensor value by mistake.");
                }
            } else if (ThreadLocal.class.isAssignableFrom(type)) {
                long cnt = WARN_CNT.getAndIncrement();
                if (cnt < 5 || cnt % 10000 == 0) {
                    LOG.warn("Trying to serialize a ThreadLocal object of type " + type + ", which could lead to unexpected" +
                            "behaviour upon rebind. ThreadLocal object serialization is not supported or recommended. " +
                            "Check if a wrapper for ThreadLocal object is set as a config or sensor value by mistake.");
                }
            } else if (LOG.isTraceEnabled()) {
                LOG.trace("Serializing object of type " + type.getName());
            }
            // TODO could add more checks to guide developers, for example
            // checking for anonymous/nested classes, classloaders, etc.
        }
    }

}
