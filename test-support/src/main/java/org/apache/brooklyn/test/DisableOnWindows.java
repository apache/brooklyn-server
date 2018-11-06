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

import java.lang.annotation.*;

/**
 * Used to indicate that a test should ne be executed on Windows.
 *
 * <p>You must add the following to the class where this annotation is being used:
 * {@code @Listeners(DisableOnWindows)}</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface DisableOnWindows {

    /**3
     * Explain the reason for the test being disabled.
     *
     * <p>e.g. "Needs an ssh server listening on port 22 on localhost."</p>
     */
    String reason();
}
