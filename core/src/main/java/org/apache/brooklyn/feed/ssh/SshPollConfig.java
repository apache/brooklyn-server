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
package org.apache.brooklyn.feed.ssh;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.util.collections.MutableMap;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;

/**
 * @deprecated since 0.11.0. Please use {@link CommandPollConfig}.
 */
@Deprecated
public class SshPollConfig<T> extends CommandPollConfig<T> {
    public SshPollConfig(AttributeSensor<T> sensor) {
        super(sensor);
    }

    public SshPollConfig(CommandPollConfig<T> other) {
        super(other);
    }

    @Override
    public SshPollConfig<T> self() {
        return this;
    }
    
    // TODO Kept in case it's persisted; new code will not use this.
    @SuppressWarnings("unused")
    private static final Predicate<SshPollValue> unused_DEFAULT_SUCCESS = new Predicate<SshPollValue>() {
        @Override
        public boolean apply(@Nullable SshPollValue input) {
            return input != null && input.getExitStatus() == 0;
        }};
        
    // TODO Kept in case it's persisted; new code will not use this.
    // Can't just use deserializingClassRenames.properties, because persistence format
    // for a static versus anonymous (non-static) inner class is different. 
    @SuppressWarnings("unused")
    private void unused_getEnvSupplier() {
        new Supplier<Map<String,String>>() {
            @Override
            public Map<String, String> get() {
                Map<String,String> result = MutableMap.of();
                for (Supplier<Map<String, String>> envS: dynamicEnvironmentSupplier) {
                    if (envS!=null) {
                        Map<String, String> envM = envS.get();
                        if (envM!=null) {
                            mergeEnvMaps(envM, result);
                        }
                    }
                }
                return result;
            }
            private void mergeEnvMaps(Map<String,String> supplied, Map<String,String> target) {
                if (supplied==null) return;
                // as the value is a string there is no need to look at deep merge behaviour
                target.putAll(supplied);
            }
        };
    }
}
