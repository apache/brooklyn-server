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
package org.apache.brooklyn.launcher.osgi;

/**
 * For use in Karaf, Brooklyn Launcher's startup is split into two parts:
 * <ol>
 *   <li>Part one (by {@code brooklyn-karaf-init}) creates the management context etc,
 *       and advertises the {@link OsgiLauncher} as a service.
 *   <li>Part two (by {@code brooklyn-karaf-start}) actually starts things up (binding to 
 *       persisted state, etc).
 *       
 * </ol>
 * 
 * This separation allows the other bundles, such as the {@code brooklyn-rest-resources}, to
 * satisfy their dependencies earlier and thus to start up.
 * 
 * This gives us the same behaviour as we have in classic, where the web-apps would be
 * started in the middle of {@link org.apache.brooklyn.launcher.common.BasicLauncher#start()}, 
 * in the overridden {@link org.apache.brooklyn.launcher.common.BasicLauncher#startingUp()}.
 * 
 * The web-console can thus show "starting", and the rest-api respond, when rebind() is taking
 * a long time.
 */
public interface OsgiLauncher {

    /**
     * Called by blueprint container of brooklyn-karaf-init.
     * 
     * Creates the management context, etc.
     */
    public void initOsgi();

    /**
     * Called by blueprint container of brooklyn-karaf-start.
     * 
     * Starts the management context properly ()binding to persisted state, etc).
     */
    public void startOsgi();

    /**
     * Called by blueprint container brooklyn-karaf-init.
     * 
     * Destroys the management context.
     */
    public void destroyOsgi();
}
