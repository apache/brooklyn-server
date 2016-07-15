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
package org.apache.brooklyn.entity.software.base;

import com.google.common.annotations.Beta;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.location.ssh.SshMachineLocation;

/**
 * An entity that has the same functionality as {@link VanillaSoftwareProcess}
 * but starts in the same {@link SshMachineLocation machine} as its parent.
 * <p>
 * This allows configuring child processes with thir own lifecycles in a more
 * intuitive fashion than {@link SameServerEntity} since the semantics of the
 * parent-child relationship are well known.
 * <p>
 * This example shows a service entity with a child agent entity, that will be
 * started on the same machine as the parent service, once it is running.
 * <pre>
 * services:
 *   - type: org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess
 *     brooklyn.config:
 *       install.command: |
 *         wget http://example.org/service.tgz
 *         tar zxf service.tgz
 *       launch.command: |
 *         service.sh
 *     brooklyn.children:
 *       - type: org.apache.brooklyn.entity.software.base.ChildSoftwareProcess
 *         brooklyn.config:
 *           install.command: |
 *             wget http://example.org/agent.tgz
 *             tar zxf agent.tgz
 *           launch.command: |
 *             agent.sh
 * </pre>
 */
@Beta
@ImplementedBy(ChildSoftwareProcessImpl.class)
public interface ChildSoftwareProcess extends VanillaSoftwareProcess {
}
