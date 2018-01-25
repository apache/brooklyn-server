/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.core.internal.ssh;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.math.MathPredicates;

@Beta
public class ExecCmdAsserts {

    public static void assertExecsContain(List<ExecCmd> actuals, List<String> expectedCmds) {
        String errMsg = "actuals="+actuals+"; expected="+expectedCmds;
        assertTrue(actuals.size() >= expectedCmds.size(), "actualSize="+actuals.size()+"; expectedSize="+expectedCmds.size()+"; "+errMsg);
        for (int i = 0; i < expectedCmds.size(); i++) {
            assertExecContains(actuals.get(i), expectedCmds.get(i), errMsg);
        }
    }

    public static void assertExecContains(ExecCmd actual, String expectedCmdRegex) {
        assertExecContains(actual, expectedCmdRegex, null);
    }
    
    public static void assertExecContains(ExecCmd actual, String expectedCmdRegex, String errMsg) {
        for (String cmd : actual.commands) {
            if (cmd.matches(expectedCmdRegex)) {
                return;
            }
        }
        fail(expectedCmdRegex + " not matched by any commands in " + actual+(errMsg != null ? "; "+errMsg : ""));
    }

    public static void assertExecContainsLiteral(ExecCmd actual, String literal) {
        for (String cmd : actual.commands) {
            if (cmd.contains(literal)) {
                return;
            }
        }
        fail("No match for '"+literal+"' in "+actual);
    }

    public static void assertExecsNotContains(List<? extends ExecCmd> actuals, List<String> expectedNotCmdRegexs) {
        for (ExecCmd actual : actuals) {
            assertExecNotContains(actual, expectedNotCmdRegexs);
        }
    }
    
    public static void assertExecNotContains(ExecCmd actual, List<String> expectedNotCmdRegexs) {
        for (String cmdRegex : expectedNotCmdRegexs) {
            for (String subActual : actual.commands) {
                if (subActual.matches(cmdRegex)) {
                    fail("Exec should not contain " + cmdRegex + ", but matched by " + actual);
                }
            }
        }
    }

    public static void assertExecsSatisfy(List<ExecCmd> actuals, List<? extends Predicate<? super ExecCmd>> expectedCmds) {
        String errMsg = "actuals="+actuals+"; expected="+expectedCmds;
        assertTrue(actuals.size() >= expectedCmds.size(), "actualSize="+actuals.size()+"; expectedSize="+expectedCmds.size()+"; "+errMsg);
        for (int i = 0; i < expectedCmds.size(); i++) {
            assertExecSatisfies(actuals.get(i), expectedCmds.get(i), errMsg);
        }
    }

    public static void assertExecSatisfies(ExecCmd actual, Predicate<? super ExecCmd> expected) {
        assertExecSatisfies(actual, expected, null);
    }
    
    public static void assertExecSatisfies(ExecCmd actual, Predicate<? super ExecCmd> expected, String errMsg) {
        if (!expected.apply(actual)) {
            fail(expected + " not matched by " + actual + (errMsg != null ? "; "+errMsg : ""));
        }
    }

    public static void assertExecHasNever(List<ExecCmd> actuals, String expectedCmd) {
        assertExecHasExactly(actuals, expectedCmd, 0);
    }

    public static void assertExecHasOnlyOnce(List<ExecCmd> actuals, String expectedCmd) {
        assertExecHasExactly(actuals, expectedCmd, 1);
    }

    public static void assertExecHasAtLeastOnce(List<ExecCmd> actuals, String expectedCmd) {
        assertExecHasExactly(actuals, expectedCmd, MathPredicates.greaterThanOrEqual(1));
    }

    public static void assertExecHasExactly(List<ExecCmd> actuals, String expectedCmd, int expectedCount) {
        assertExecHasExactly(actuals, expectedCmd, Predicates.equalTo(expectedCount));
    }

    public static void assertExecHasExactly(List<ExecCmd> actuals, String expectedCmd, Predicate<Integer> countChecker) {
        String errMsg = "actuals="+actuals+"; expected="+expectedCmd;
        int count = 0;
        for (ExecCmd actual : actuals) {
            for (String subActual : actual.commands) {
                if (subActual.matches(expectedCmd)) {
                    count++;
                    break;
                }
            }
        }
        assertTrue(countChecker.apply(count), "actualCount="+count+"; expectedCount="+countChecker+"; "+errMsg);
    }

    public static ExecCmd findExecContaining(List<ExecCmd> actuals, String cmdRegex) {
        for (ExecCmd actual : actuals) {
            for (String subActual : actual.commands) {
                if (subActual.matches(cmdRegex)) {
                    return actual;
                }
            }
        }
        fail("No match for '"+cmdRegex+"' in "+actuals);
        throw new IllegalStateException("unreachable code");
    }
}
