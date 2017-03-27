/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.policy.action;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;

/**
 * <pre>{@code
 * brooklyn.policies:
 *   - type: org.apache.brooklyn.policy.action.ScheduledEffectorPolicy
 *     brooklyn.config:
 *       effector: repaveCluster
 *       args:
 *         k: $brooklyn:config("repave.size")
 *       time: 12:00 01 January 2018
 * }</pre>
 */
@Beta
public class ScheduledEffectorPolicy extends AbstractScheduledEffectorPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledEffectorPolicy.class);

    public static final ConfigKey<String> TIME = ConfigKeys.builder(String.class)
            .name("time")
            .description("The time when this policy should be executed")
            .constraint(Predicates.notNull())
            .build();

    protected Date when;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public ScheduledEffectorPolicy() {
        this(MutableMap.<String,Object>of());
    }

    public ScheduledEffectorPolicy(Map<String,?> props) {
        super(props);
        String time = Preconditions.checkNotNull(config().get(TIME), "The time must be configured for this policy");
        DateFormat format = DateFormat.getDateTimeInstance();
        try {
            when = format.parse(time);
        } catch (ParseException e) {
            Exceptions.propagate(e);
        }
        Date now = new Date();
        if (when.before(now)) {
            throw new IllegalStateException("The time provided must be in the future");
        }
    }

    @Override
    public void setEntity(final EntityLocal entity) {
        super.setEntity(entity);
        Date now = new Date();
        long difference = Math.max(0, when.getTime() - now.getTime());
        executor.schedule(this, difference, TimeUnit.MILLISECONDS);
    }

}
