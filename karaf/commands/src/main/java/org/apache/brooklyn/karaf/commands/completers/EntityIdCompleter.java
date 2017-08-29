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
package org.apache.brooklyn.karaf.commands.completers;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.api.console.CommandLine;
import org.apache.karaf.shell.api.console.Completer;
import org.apache.karaf.shell.api.console.Session;
import org.apache.karaf.shell.support.completers.StringsCompleter;
import java.util.List;

@Service
public class EntityIdCompleter implements Completer {

    @Reference
    private ManagementContext managementContext;

    @Override
    public int complete(final Session session, final CommandLine commandLine, final List<String> candidates) {
        final StringsCompleter delegate = new StringsCompleter();
        managementContext.getEntityManager().getEntities().forEach(entity -> delegate.getStrings().add(entity.getId()));
        return delegate.complete(session, commandLine, candidates);
    }
}
