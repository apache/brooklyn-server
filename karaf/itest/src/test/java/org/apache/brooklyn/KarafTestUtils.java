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
package org.apache.brooklyn;

import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.MavenUtils.asInProject;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.configureConsole;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;

import java.io.File;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.LogLevelOption.LogLevel;
import org.ops4j.pax.exam.options.MavenArtifactUrlReference;
import org.ops4j.pax.exam.options.MavenUrlReference;

import com.google.common.collect.ObjectArrays;

public class KarafTestUtils {
    public static final Option[] DEFAULT_OPTIONS = {
        karafDistributionConfiguration()
            .frameworkUrl(brooklynKarafDist())
            .unpackDirectory(new File("target/paxexam/unpack/"))
            .useDeployFolder(false),
        configureConsole().ignoreLocalConsole(),
        logLevel(LogLevel.INFO),
        features(karafStandardFeaturesRepository(), "eventadmin"),
        junitBundles()
    };

    public static MavenUrlReference karafStandardFeaturesRepository() {
        return maven()
                .groupId("org.apache.karaf.features")
                .artifactId("standard")
                .type("xml")
                .classifier("features")
                .version(asInProject());
    }


    public static MavenArtifactUrlReference brooklynKarafDist() {
        return maven()
                .groupId("org.apache.brooklyn")
                .artifactId("apache-brooklyn")
                .type("zip")
                .version(asInProject());
    }

    public static Option[] defaultOptionsWith(Option... options) {
        return ObjectArrays.concat(DEFAULT_OPTIONS, options, Option.class);
    }

    public static MavenUrlReference brooklynFeaturesRepository() {
        return maven()
                .groupId("org.apache.brooklyn")
                .artifactId("brooklyn-features")
                .type("xml")
                .classifier("features")
                .versionAsInProject();
    }
}
