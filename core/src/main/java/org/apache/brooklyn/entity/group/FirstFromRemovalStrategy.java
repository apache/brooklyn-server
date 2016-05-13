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

import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

public class FirstFromRemovalStrategy extends RemovalStrategy {

    public static final ConfigKey<List<RemovalStrategy>> STRATEGIES = ConfigKeys.newConfigKey(new TypeToken<List<RemovalStrategy>>() {}, "firstfrom.strategies",
            "An ordered list of removal strategies to be used to determine which entity to remove");

    @Nullable
    @Override
    public Entity apply(@Nullable Collection<Entity> input) {
        List<RemovalStrategy> strategies = config().get(STRATEGIES);
        if (strategies == null || Iterables.isEmpty(strategies)) {
            return null;
        }
        for (RemovalStrategy strategy : strategies) {
            Entity entity = strategy.apply(input);
            if (entity != null) {
                return entity;
            }
        }
        return null;
    }
}
