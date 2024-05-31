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

import java.util.Arrays;

import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;


public class WorkflowParsingEdgeCasesTest extends BrooklynMgmtUnitTestSupport {

    private BasicApplication app;

    Object runSteps(String ...steps) {
        Pair<BasicApplication, Object> result = WorkflowBasicTest.runStepsInNewApp(mgmt(), Arrays.asList(steps));
        this.app = result.getLeft();
        return result.getRight();
    }

    @Test
    public void testBackslashEscaping() {
        final String BS = "\\";

        String UNQUOTED_INPUT = "double unquoted " + BS + BS;
        String QUOTED_INPUT_UNQUOTED = "quoted " + BS + BS;
        String QUOTED_INPUT = "\"" + QUOTED_INPUT_UNQUOTED + "\"";
        String QUOTED_INPUT_UNQUOTED_UNESCAPED = "quoted " + BS;

        // entire phrase quoted will be unquoted and unescaped
        Asserts.assertEquals(runSteps("return " + QUOTED_INPUT), QUOTED_INPUT_UNQUOTED_UNESCAPED);
        // but unquoted won't be
        Asserts.assertEquals(runSteps("return " + UNQUOTED_INPUT), UNQUOTED_INPUT);
        // and partial words won't be even if quoted
        Asserts.assertEquals(runSteps("return " + UNQUOTED_INPUT + " and " + QUOTED_INPUT),
            UNQUOTED_INPUT + " and " + QUOTED_INPUT);

        // however 'let' does do unquoting on word level (according to EP words)
        Asserts.assertEquals(runSteps("let x = "+UNQUOTED_INPUT+" and "+QUOTED_INPUT, "return ${x}"),
                UNQUOTED_INPUT + " and " + QUOTED_INPUT_UNQUOTED_UNESCAPED);
    }
    
}
