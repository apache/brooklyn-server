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
package org.apache.brooklyn.core.effector.script;

import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Blocks access to any {@code org.apache.brooklyn} classes
 * with an entitlements check.
 */
public class ScriptClassLoader extends ClassLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ScriptClassLoader.class);

    private List<Pattern> blacklist = ImmutableList.of();

    public ScriptClassLoader(ClassLoader parent, String...blacklist) {
        super(parent);
        this.blacklist = compileBlacklist(blacklist);
    }

    private List<Pattern> compileBlacklist(String...blacklist) {
        List<Pattern> patterns = Lists.newArrayList();
        for (String entry : blacklist) {
            patterns.add(Pattern.compile(entry));
        }
        return ImmutableList.copyOf(patterns);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        LOG.info("Script class loader: {}", name);
        for (Pattern pattern : blacklist) {
            if (pattern.matcher(name).matches()) {
                throw new ClassNotFoundException(String.format("Class %s is blacklisted: %s", name, pattern.pattern()));
            }
        }
        return super.loadClass(name, resolve);
    }

    @Override
    public String toString() {
        return String.format("ScriptClassLoader %s", Iterables.toString(blacklist));
    }
}
