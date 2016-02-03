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
package org.apache.brooklyn.cm.salt.impl;

import com.google.common.annotations.Beta;
import org.apache.brooklyn.cm.salt.SaltConfig;
import org.apache.brooklyn.cm.salt.SaltEntity;
import org.apache.brooklyn.entity.stock.EffectorStartableImpl;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

@Beta
public class SaltEntityImpl extends EffectorStartableImpl implements SaltEntity {
    private static final Logger LOG = LoggerFactory.getLogger(SaltEntityImpl.class);

    public SaltEntityImpl() {
        super();
    }

    @Override
    public void init() {
        super.init();

        final Set<? extends String> runList = getConfig(SaltConfig.START_STATES);
        if (0 == runList.size()) {
            throw new IllegalArgumentException("Must have configuration values for 'start_states' ("
                + SaltConfig.START_STATES + ")");
        }

        LOG.debug("Run list size is {}", runList.size());
        for (String state : runList) {
            LOG.debug("Runlist state: {} ", state);
        }

        final Set<? extends String> formulas = getConfig(SaltConfig.SALT_FORMULAS);
        LOG.debug("Formulas size: {}", formulas.size());
        for (String formula : formulas) {
            LOG.debug("Formula configured:  {}", formula);
        }

        SaltConfig.SaltMode mode = getConfig(SaltConfig.SALT_MODE);
        LOG.debug("Initialize SaltStack {} mode", mode.name());
        new SaltLifecycleEffectorTasks().attachLifecycleEffectors(this);
    }

    @Override
    public void populateServiceNotUpDiagnostics() {
        // TODO: noop for now;
    }

    @Override
    public String saltCall(String spec) {
        final ProcessTaskWrapper<Integer> command = DynamicTasks.queue(SaltSshTasks.saltCall(spec));
        command.asTask().blockUntilEnded();
        if (0 == command.getExitCode()) {
            return command.getStdout();
        } else {
            throw new RuntimeException("Command (" + spec + ")  failed with stderr:\n" + command.getStderr() + "\n");
        }
    }
}
