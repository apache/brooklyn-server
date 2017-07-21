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
package org.apache.brooklyn.launcher;

/**
 * A convenience for started the Brooklyn REST api and web-app in a test, so that one can visually
 * inspect the app that the test creates. This is intended as a read-only view (but it has the real
 * management context so one can perform actions through this UI).
 * 
 * An example of where this is very useful is when testing a blueprint where an effector hangs - 
 * one can visually inspect the app, drilling into the activities view to see what it is doing
 * and why it is blocked.
 *   
 * It must be configured with an existing {@link org.apache.brooklyn.api.mgmt.ManagementContext}.
 * 
 * Various other configuration options (e.g. {@link #application(String)) will be ignored.
 * 
 * Example usage is:
 * <pre>
 * {@code
 * protected ManagementContext managementContext;
 * protected BrooklynLauncher viewer;
 * 
 * public void setUp() throws Exception {
 *     managementContext = ...
 *     viewer = BrooklynViewerLauncher.newInstance()
 *             .managementContext(managementContext)
 *             .start();
 * }
 * 
 * public void tearDown() throws Exception {
 *     if (viewer != null) {
 *         viewer.terminate();
 *     }
 *     ...
 * }
 * </pre>
 */
public class BrooklynViewerLauncher extends BrooklynLauncher {

    public static BrooklynViewerLauncher newInstance() {
        return new BrooklynViewerLauncher();
    }

    /**
     * A cut-down start, which just does the web-apps (intended as a read-only view). It assumes 
     * that a fully initialised management context will have been registered.
     */
    @Override
    public BrooklynLauncher start() {
        if (started) throw new IllegalStateException("Cannot start() or launch() multiple times");
        started = true;

        if (getManagementContext() == null || !getManagementContext().isRunning()) {
            throw new IllegalStateException("Management context must be set, and running");
        }
        
        startingUp();
        markStartupComplete();
        
        initBrooklynNode();

        return this;
    }
}
