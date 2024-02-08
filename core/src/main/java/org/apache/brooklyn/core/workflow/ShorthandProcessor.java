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
package org.apache.brooklyn.core.workflow;

import java.util.Map;

import org.apache.brooklyn.util.guava.Maybe;

/**
 * This impl delegates to one of the various classes that do this -- see notes in individual ones.
 */
public class ShorthandProcessor {

    ShorthandProcessorEpToQst delegate;

    public ShorthandProcessor(String template) {
        delegate = new ShorthandProcessorEpToQst(template);
    }

    public Maybe<Map<String,Object>> process(String input) {
        return delegate.process(input);
    }

    /** optionally skip the automatic unwrapping of a single quoted last word lining up with the last word of a template;
     * by default, we unwrap in that one special case, to facilitate eg return "something fancy" */
    public ShorthandProcessor withFinalMatchRaw(boolean finalMatchRaw) {
        delegate.withFinalMatchRaw(finalMatchRaw);
        return this;
    }

    /** whether to fail on mismatched quotes in the input, default true */
    public ShorthandProcessor withFailOnMismatch(boolean failOnMismatch) {
        // only supported for some
        delegate.withFailOnMismatch(failOnMismatch);
        return this;
    }

}
