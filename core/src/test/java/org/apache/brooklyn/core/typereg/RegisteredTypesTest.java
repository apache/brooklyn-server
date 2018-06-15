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
package org.apache.brooklyn.core.typereg;

import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.util.text.Strings;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import static org.testng.Assert.*;

public class RegisteredTypesTest {

    private static final String DRBD_YAML = Strings.lines(
            "brooklyn.catalog:",
            "  version: \"1.0.0-SNAPSHOT\" # BROOKLYN_DRBD_VERSION",
            "  items:",
            "  - \"https://github.com/brooklyncentral/common-catalog-utils/releases/download/v0.1.0/common.bom\"",
            "  - id: drbd-node");

    private static final String DRBD_TESTS_YAML = Strings.lines(
            "brooklyn.catalog:",
            "  version: \"1.0.0-SNAPSHOT\" # BROOKLYN_DRBD_VERSION",
            "  items:",
            "  - https://github.com/brooklyncentral/common-catalog-utils/releases/download/v0.1.0/common.tests.bom\n");

    private static final String DRBR_YAML_WITH_COMMENTS = Strings.lines(
            DRBD_YAML,
            "# this is a comment");

    private static final String DRBR_YAML_WITHOUT_QUOTES = Strings.lines(
            "brooklyn.catalog:",
            "  version: 1.0.0-SNAPSHOT # BROOKLYN_DRBD_VERSION",
            "  items:",
            "  - https://github.com/brooklyncentral/common-catalog-utils/releases/download/v0.1.0/common.bom",
            "  - id: drbd-node");

    @Test
    public void testIdenticalPlansAreEquivalent() {
        BasicRegisteredType planA = makeTypeWithYaml(DRBD_YAML);

        BasicRegisteredType planB = makeTypeWithYaml(DRBD_YAML);

        assertPlansEquivalent(planA, planB, "Identical plans");
    }

    @Test
    public void testDifferentPlansAreNotEquivalent() {
        BasicRegisteredType planA = makeTypeWithYaml(DRBD_YAML);
        BasicRegisteredType planB = makeTypeWithYaml(DRBD_TESTS_YAML);

        assertPlansDifferent(planA, planB, "Completely different plans");
    }

    @Test
    public void testComparisonIgnoresComments() {
        BasicRegisteredType planA = makeTypeWithYaml(DRBD_YAML);

        BasicRegisteredType planB = makeTypeWithYaml(DRBR_YAML_WITH_COMMENTS);

        assertPlansEquivalent(planA, planB, "Only difference comments");
    }

    @Test
    public void testComparisonIgnoresQuotedStrings() {
        BasicRegisteredType planA = makeTypeWithYaml(DRBD_YAML);

        BasicRegisteredType planB = makeTypeWithYaml(DRBR_YAML_WITHOUT_QUOTES);

        assertPlansEquivalent(planA, planB, "Compares same yaml with and without quotes");
    }

    private void assertPlansDifferent(BasicRegisteredType planA, BasicRegisteredType planB, String messageOnError) {
        assertFalse(RegisteredTypes.arePlansEquivalent(planA, planB), createErrorMessage("Plans equivalent: ", planA, planB, messageOnError));
    }

    private void assertPlansEquivalent(BasicRegisteredType planA, BasicRegisteredType planB, String messageOnError) {
        assertTrue(RegisteredTypes.arePlansEquivalent(planA, planB), createErrorMessage("Plans differ: ",planA, planB, messageOnError));
    }

    private String createErrorMessage(String issue, BasicRegisteredType planA, BasicRegisteredType planB, String messageOnError) {
        return Strings.lines(issue + messageOnError, planA.toString(), planB.toString());
    }

    BasicRegisteredType makeTypeWithYaml(final String yaml) {
        return new BasicRegisteredType(
                BrooklynTypeRegistry.RegisteredTypeKind.SPEC,
                "B",
                "1",
                new RegisteredType.TypeImplementationPlan() {
                    @Nullable
                    @Override
                    public String getPlanFormat() {
                        return null;
                    }

                    @Override
                    public Object getPlanData() {
                        return yaml;
                    }
                });
    }


}