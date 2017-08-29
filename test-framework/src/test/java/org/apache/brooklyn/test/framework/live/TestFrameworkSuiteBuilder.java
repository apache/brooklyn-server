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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.brooklyn.test.framework.live.TestFrameworkRun.TestFrameworkRunBuilder;
import org.apache.brooklyn.util.collections.MutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

public class TestFrameworkSuiteBuilder {
    private static final Logger log = LoggerFactory.getLogger(TestFrameworkSuiteBuilder.class);
    
    public static class TestFrameworkGroupBuilder {
        private TestFrameworkSuiteBuilder testFrameworkSuiteBuilder;
        private TestFrameworkGroupBuilder(TestFrameworkSuiteBuilder testFrameworkSuiteBuilder) {
            this.testFrameworkSuiteBuilder = testFrameworkSuiteBuilder;
        }

        private MutableList<URL> prerequisites = MutableList.of();
        private MutableList<URL> tests = MutableList.of();
        private MutableList<String> clouds = MutableList.of();
        private MutableList<String> distributions = MutableList.of();
        private Multimap<String, String> locationAliases = ArrayListMultimap.create();
        private MutableList<String> locations = MutableList.of();

        /**
         * Adds a resource reference to load generic catalog items from.
         * @param value points to a generic catalog bom resource. Could
         * be of any type, including locations.
         */
        public TestFrameworkGroupBuilder prerequisite(URL value) {
            prerequisites.add(checkNotNull(value, "value"));
            return this;
        }
        /**
         * Adds a resource reference to load catalog items from
         * @param resource points to a resource on the class path
         *                 visible to the brooklyn-test-framework bundle.
         */
        // TODO Support for OSGi (pass in a class from the bundle?)
        public TestFrameworkGroupBuilder prerequisiteResource(String resource) {
            return prerequisite(getResourceUrl(resource));
        }
        /**
         * Removes all previously added prerequisites.
         */
        public TestFrameworkGroupBuilder prerequisitesClear() {
            prerequisites.clear();
            return this;
        }
        /**
         * Adds resource references to load catalog items from.
         * @param value elements point to catalog bom resources.
         */
        public TestFrameworkGroupBuilder prerequisites(Iterable<URL> value) {
            Iterables.addAll(prerequisites, checkNotNull(value, "value"));
            return this;
        }
        /**
         * Adds a resource reference to load test catalog items from.
         * @param value points to a test catalog bom resource. The items
         *              in the resource will be use to launch tests with.
         *              This includes the catalog items from any included catalogs.
         */
        public TestFrameworkGroupBuilder test(URL value) {
            tests.add(value);
            return this;
        }
        /**
         * Removes all previously added test resources.
         */
        public TestFrameworkGroupBuilder testsClear() {
            tests.clear();
            return this;
        }
        /**
         * Adds resource references to load test catalog items from.
         * @param value elements point to test catalog bom resources.
         *              The catalog items in the resource will be use to launch tests with.
         *              This includes the catalog items from any included catalogs.
         */
        public TestFrameworkGroupBuilder tests(Iterable<URL> value) {
            Iterables.addAll(tests, checkNotNull(value, "value"));
            return this;
        }
        /**
         * Adds a resource reference to load test catalog items from.
         * @param resource points to a test resource on the class path
         *                 visible to the brooklyn-test-framework bundle.
         */
        // TODO Support for OSGi (pass in a class from the bundle?)
        public TestFrameworkGroupBuilder testResource(String resource) {
            return test(getResourceUrl(resource));
        }
        /**
         * Adds a cloud to the testing matrix. Assumes location boms on disk
         * follow the "cloud:distribution.bom" naming format.
         */
        public TestFrameworkGroupBuilder cloud(String value) {
            clouds.add(value);
            return this;
        }
        /**
         * Removes all previously added clouds.
         */
        public TestFrameworkGroupBuilder clouds() {
            clouds.clear();
            return this;
        }
        /**
         * Adds the clouds passed as argument to the testing matrix.
         * Assumes location boms on disk follow the "cloud:distribution.bom" naming format.
         */
        public TestFrameworkGroupBuilder clouds(Iterable<String> value) {
            Iterables.addAll(clouds, checkNotNull(value, "value"));
            return this;
        }
        /**
         * Adds a distribution to the testing matrix. Assumes location boms on disk
         * follow the "cloud:distribution.bom" naming format.
         */
        public TestFrameworkGroupBuilder distribution(String value) {
            distributions.add(value);
            return this;
        }
        /**
         * Removes all previously added distributions.
         */
        public TestFrameworkGroupBuilder distributionClear() {
            distributions.clear();
            return this;
        }
        /**
         * Adds the distributions passed as an argument to the testing matrix.
         * Assumes location boms on disk follow the "cloud:distribution.bom" naming format.
         */
        public TestFrameworkGroupBuilder distributions(Iterable<String> value) {
            Iterables.addAll(distributions, checkNotNull(value, "value"));
            return this;
        }
        /**
         * Adds a location -> alias pair. When executing the test the location will
         * be made available under the {@code alias} ID to the running test.
         */
        public TestFrameworkGroupBuilder locationAlias(String location, String alias) {
            locationAliases.put(
                    checkNotNull(location, "location"),
                    checkNotNull(alias, "alias"));
            return this;
        }
        /**
         * Removes all previously added location->alias pairs.
         */
        public TestFrameworkGroupBuilder locationAliasesClear() {
            locationAliases.clear();
            return this;
        }
        /**
         * Adds the location -> alias pairs. When executing the test the locations (the map keys)
         * will be made available under the alias ID (the map values) to the running test.
         */
        public TestFrameworkGroupBuilder locationAliases(Map<String, String> aliases) {
            checkNotNull(aliases, "aliases");
            for (Entry<String, String> alias : aliases.entrySet()) {
                locationAliases.put(alias.getKey(), alias.getValue());
            }
            return this;
        }
        /**
         * Adds a location to execute the test against. Before
         * starting a test run the code will load the
         * location from the location.bom file if one exists.
         */
        public TestFrameworkGroupBuilder location(String value) {
            locations.add(value);
            return this;
        }
        /**
         * Removes all previously added locations.
         */
        public TestFrameworkGroupBuilder locationsClear() {
            locations.clear();
            return this;
        }
        /**
         * Adds locations to execute the test against (once per location).
         * Before starting a test run the code will load the location
         * from the location.bom file if one exists.
         */
        public TestFrameworkGroupBuilder locations(Iterable<String> value) {
            Iterables.addAll(locations, value);
            return this;
        }

        /**
         * Creates a list of tests corresponding to the arguments passed to the
         * builder. Adds the cartesion product of all cloud + distribution pairs, 
         * concatenating them like "<cloud>:<distribution>" and adds the result
         * to the test locations. For each resulting location adds an element to
         * the returned list.
         */
        public Collection<TestFrameworkRun> build() {
            MutableList<TestFrameworkRun> runs = MutableList.of();
            Collection<String> allLocations = MutableList.of();
            for (String cloud : clouds) {
                for (String dist : distributions) {
                    // TODO add a generic template for the location names to handle custom schemes.
                    allLocations.add(dist + ":" + cloud);
                }
            }
            allLocations.addAll(locations);
            for (String location : allLocations) {
                runs.add(TestFrameworkRun.builder()
                    .prerequisites(getLocationResources(ImmutableList.of(location)))
                    .prerequisites(getLocationResources(locationAliases.keySet()))
                    .prerequisites(prerequisites)
                    .tests(tests)
                    // Handles locations with file names using ':' delimiter, but 
                    // using '_' for the ID.
                    // TODO add a generic file name -> ID transformer to handle custom schemes.
                    .location(location.replace(":", "_"))
                    // TODO Transform the aliases keys in a similar way to the locations
                    .locationAliases(locationAliases)
                    .build());
            }
            return runs.asImmutableCopy();
        }

        private URL getResourceUrl(String resource) {
            checkNotNull(resource, "resource");
            URL url = getClass().getClassLoader().getResource(resource);
            checkNotNull(url, "resource url for " + resource);
            return url;
        }

        private Collection<URL> getLocationResources(Collection<String> locations) {
            if (testFrameworkSuiteBuilder.locationsFolder != null) {
                MutableList<URL> resources = MutableList.of();
                for (String location : locations) {
                    File locationBom = new File(testFrameworkSuiteBuilder.locationsFolder, location + ".bom");
                    if (locationBom.exists()) {
                        try {
                            resources.add(locationBom.toURI().toURL());
                        } catch (MalformedURLException e) {
                            throw new IllegalStateException("Could not conert the path " + locationBom.getAbsolutePath() + " to URL", e);
                        }
                    } else {
                        log.info("Locationn file {} not found in {}. Assuming it's a location provided by the environment.",
                                location, testFrameworkSuiteBuilder.locationsFolder);
                    }
                }
                return resources.asImmutableCopy();
            } else {
                return ImmutableList.of();
            }
        }

        public TestFrameworkSuiteBuilder add() {
            testFrameworkSuiteBuilder.tests.addAll(build());
            return testFrameworkSuiteBuilder;
        }
    }

    private Collection<TestFrameworkRun> tests = MutableList.of();
    private File locationsFolder;

    public static TestFrameworkSuiteBuilder create() {
        return new TestFrameworkSuiteBuilder();
    }
    
    private TestFrameworkSuiteBuilder() {
        String locations = System.getProperty("test.framework.locations");
        if (locations != null) {
            locationsFolder = new File(locations);
        }
    }
    
    /**
     * Create a new test builder to describe a test run with.
     * Add to the test suite with {@link TestFrameworkRunBuilder#add}.
     */
    public TestFrameworkGroupBuilder test() {
        return new TestFrameworkGroupBuilder(this);
    }
    
    /**
     * Sets the folder to look for location boms in.
     * Defaults to the "test.framework.locations" system property.
     */
    public TestFrameworkSuiteBuilder locationsFolder(File locationsFolder) {
        this.locationsFolder = checkNotNull(locationsFolder, "locationsFolder");
        return this;
    }
    
    /**
     * Sets the folder to look for location boms in.
     * Defaults to the "test.framework.locations" system property.
     */
    public TestFrameworkSuiteBuilder locationsFolder(String locationsFolder) {
        return locationsFolder(new File(checkNotNull(locationsFolder, "locationsFolder")));
    }
    
    /**
     * Returns the list of all tests created in the suite.
     * To pause a test run on failure (don't tear down) specify
     * "test.framework.interactive=true" as a system property.
     */
    public Collection<TestFrameworkRun> build() {
        return ImmutableList.copyOf(tests);
    }
    
    /**
     * Same as {@link #build()} but returns a value suitable for a TestNG data provider.
     */
    public Object[][] buildProvider() {
        Object[][] runs = new Object[tests.size()][];
        int i = 0;
        for (TestFrameworkRun test : tests) {
            runs[i] = new Object[] {test};
            i++;
        }
        return runs;
    }
}
