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
package org.apache.brooklyn.test.framework.live;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.brooklyn.util.collections.MutableList;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

public class TestFrameworkRun {
    public static class TestFrameworkRunBuilder {
        private TestFrameworkRunBuilder() {}

        private MutableList<URL> prerequisites = MutableList.of();
        private MutableList<URL> tests = MutableList.of();
        private String location;
        private Multimap<String, String> locationAliases = ArrayListMultimap.create();

        public TestFrameworkRunBuilder prerequisite(URL value) {
            prerequisites.add(checkNotNull(value, "value"));
            return this;
        }
        public TestFrameworkRunBuilder prerequisites() {
            prerequisites.clear();
            return this;
        }
        public TestFrameworkRunBuilder prerequisites(Iterable<URL> value) {
            Iterables.addAll(prerequisites, checkNotNull(value, "value"));
            return this;
        }
        public TestFrameworkRunBuilder test(URL value) {
            tests.add(value);
            return this;
        }
        public TestFrameworkRunBuilder tests() {
            tests.clear();
            return this;
        }
        public TestFrameworkRunBuilder tests(Iterable<URL> value) {
            Iterables.addAll(tests, checkNotNull(value, "value"));
            return this;
        }
        public TestFrameworkRunBuilder location(String value) {
            location = checkNotNull(value, "value");
            return this;
        }
        public TestFrameworkRunBuilder locationAlias(String location, String alias) {
            locationAliases.put(
                    checkNotNull(location, "location"),
                    checkNotNull(alias, "alias"));
            return this;
        }
        public TestFrameworkRunBuilder locationAliases() {
            locationAliases.clear();
            return this;
        }
        public TestFrameworkRunBuilder locationAliases(Map<String, String> aliases) {
            checkNotNull(aliases, "aliases");
            for (Entry<String, String> alias : aliases.entrySet()) {
                locationAliases.put(alias.getKey(), alias.getValue());
            }
            return this;
        }
        public TestFrameworkRunBuilder locationAliases(Multimap<String, String> aliases) {
            locationAliases.putAll(checkNotNull(aliases, "aliases"));
            return this;
        }
        public TestFrameworkRun build() {
            return new TestFrameworkRun(prerequisites.asImmutableCopy(),
                    tests.asImmutableCopy(),
                    location,
                    locationAliases);
        }
    }

    public static TestFrameworkRunBuilder builder() {
        return new TestFrameworkRunBuilder();
    }

    private Collection<URL> prerequisites;
    private Collection<URL> tests;
    private String location;
    private Multimap<String, String> locationAliases;

    public TestFrameworkRun(Collection<URL> prerequisites,
            Collection<URL> tests,
            String location,
            Multimap<String, String> locationAliases) {
        this.prerequisites = prerequisites;
        this.tests = tests;
        this.location = location;
        this.locationAliases = locationAliases;
    }

    public Collection<URL> prerequisites() {
        return prerequisites;
    }
    public Collection<URL> tests() {
        return tests;
    }
    public String location() {
        return location;
    }
    public Multimap<String, String> locationAliases() {
        return locationAliases;
    }
    
    @Override
    public String toString() {
        return tests.toString() + "@" + location;
    }
}
