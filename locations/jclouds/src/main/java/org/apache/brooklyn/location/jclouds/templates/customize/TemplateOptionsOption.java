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

package org.apache.brooklyn.location.jclouds.templates.customize;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.MethodCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.jclouds.compute.options.TemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemplateOptionsOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(TemplateOptionsOption.class);

    @Override
    public void apply(TemplateOptions options, ConfigBag config, Object v) {
        if (v == null) return;
        @SuppressWarnings("unchecked") Map<String, Object> optionsMap = (Map<String, Object>) v;
        if (optionsMap.isEmpty()) return;

        Class<? extends TemplateOptions> clazz = options.getClass();
        for (final Map.Entry<String, Object> option : optionsMap.entrySet()) {
            Object optionValue = option.getValue();
            if (optionValue != null) {

                try {
                    final ExecutionContext exec = BrooklynTaskTags.getCurrentExecutionContext();
                    if (exec != null) {
                        optionValue = Tasks.resolveDeepValue(optionValue, Object.class, exec);
                    }
                } catch (ExecutionException | InterruptedException e) {
                    Exceptions.propagate(e);
                }

                Maybe<?> result = MethodCoercions.tryFindAndInvokeBestMatchingMethod(options, option.getKey(), optionValue);
                if (result.isAbsent()) {
                    LOG.warn("Ignoring request to set template option {} because this is not supported by {}", new Object[]{option.getKey(), clazz.getCanonicalName()});
                }
            } else {
                // jclouds really doesn't like you to pass nulls; don't do it! For us,
                // null is the only way to remove an inherited value when the templateOptions
                // map is being merged.
                LOG.debug("Ignoring request to set template option {} because value is null", new Object[]{option.getKey(), clazz.getCanonicalName()});
            }
        }
    }
}
