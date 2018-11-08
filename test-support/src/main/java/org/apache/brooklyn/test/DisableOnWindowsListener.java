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
package org.apache.brooklyn.test;

import org.apache.brooklyn.util.os.Os;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Scans all tests annotated with {@link DisableOnWindows} and, on Windows, sets {@code enabled=false} if the current
 * environment is Windows..
 */
public class DisableOnWindowsListener implements IAnnotationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(DisableOnWindowsListener.class);

    @Override
    public void transform(
            final ITestAnnotation annotation,
            final Class testClass,
            final Constructor testConstructor,
            final Method testMethod
    ) {
        if (testMethod != null ) {
            final DisableOnWindows disableOnWindows = testMethod.getAnnotation(DisableOnWindows.class);
            if (disableOnWindows != null && Os.isMicrosoftWindows()) {
                annotation.setEnabled(false);
                LOG.info(String.format("Disabled: %s.%s - %s",
                        testMethod.getDeclaringClass().getName(),
                        testMethod.getName(),
                        disableOnWindows.reason()));
            }
        }
    }

}
