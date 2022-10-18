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

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.entity.software.base.lifecycle.ScriptHelper;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WorkflowSoftwareProcessSshDriver extends AbstractSoftwareProcessSshDriver implements WorkflowSoftwareProcessDriver {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowSoftwareProcessSshDriver.class);

    public WorkflowSoftwareProcessSshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    /**
     * Needed because the workflow is likely different for different workflows.
     * This is particularly true for YAML entities. We take a hash of the install workflow by default.
     */
    @Override
    protected String getInstallLabelExtraSalt() {
        // run non-blocking in case a value set later is used (e.g. a port)
        Integer hash = hashCodeIfResolved(WorkflowSoftwareProcess.INSTALL_WORKFLOW);

        // if any of the above blocked then we must make a unique install label,
        // as other yet-unknown config is involved
        if (hash==null) return Identifiers.makeRandomId(8);

        // a user-friendly hash is nice, but tricky since it would have to be short;
        // go with a random one unless it's totally blank
        if (hash==0) return "default";
        return Identifiers.makeIdFromHash(hash);
    }

    private Integer hashCodeIfResolved(ConfigKey<?>...keys) {
        int hash = 0;
        for (ConfigKey<?> k: keys) {
            Maybe<?> value = ((BrooklynObjectInternal.ConfigurationSupportInternal)getEntity().config()).getNonBlocking(k);
            if (value.isPresent()) {
                hash = hash*31 + (value.get()==null ? 0 : value.get().hashCode());
            }
        }
        return hash;
    }

    @Override
    public Map<String, String> getShellEnvironment() {
        return MutableMap.copyOf(super.getShellEnvironment()).add("PID_FILE", getPidFile());
    }

    public String getPidFile() {
        // TODO see note in VanillaSoftwareProcess about PID_FILE as a config key
        // if (getEntity().getConfigRaw(PID_FILE, includeInherited)) ...
        return Os.mergePathsUnix(getRunDir(), PID_FILENAME);
    }

    protected Maybe<Object> runWorkflow(ConfigKey<CustomWorkflowStep> key) {
        CustomWorkflowStep workflow = entity.getConfig(key);
        if (workflow==null) return Maybe.absent();

        WorkflowExecutionContext workflowContext = workflow.newWorkflowExecution(entity, key.getName().toLowerCase(),
                null /* could getInput from workflow, and merge shell environment here */);

        return Maybe.of(DynamicTasks.queue( workflowContext.getTask(true).get() ).getUnchecked());
    }

    @Override
    public void install() {
        runWorkflow(WorkflowSoftwareProcess.INSTALL_WORKFLOW);
    }

    @Override
    public void customize() {
        runWorkflow(WorkflowSoftwareProcess.CUSTOMIZE_WORKFLOW);
    }

    @Override
    public void launch() {
        runWorkflow(WorkflowSoftwareProcess.LAUNCH_WORKFLOW);
    }

    @Override
    public boolean isRunning() {
        try {
            Maybe<Object> wr = runWorkflow(WorkflowSoftwareProcess.CHECK_RUNNING_WORKFLOW);
            if (wr.isAbsent()) {
                if (Boolean.TRUE.equals(entity.config().get(WorkflowSoftwareProcess.USE_PID_FILE))) {
                    ScriptHelper script = newScript(MutableMap.of(USE_PID_FILE, getPidFile()), CHECK_RUNNING);
                    return script.execute() == 0;

                } else {
                    LOG.warn("No workflow to check if " + entity + " is running; will assume it is. But it is highly recommended to validate that the software is running.");
                    return true;
                }
            } else {
                Maybe<Boolean> success = TypeCoercions.tryCoerce(wr.get(), Boolean.class);
                if (success.isPresentAndNonNull()) {
                    return success.get();
                } else {
                    LOG.debug("No workflow to check if " + entity + " is running returned " + wr.get() + "; treating this as true, but it is recommended to return true or false explicitly");
                    return true;
                }
            }
        } catch (Throwable t) {
            Exceptions.propagateIfFatal(t);
            LOG.warn("Workflow for check-running at "+entity+" failed, assuming software not running: "+t);
            LOG.debug("Workflow error trace for: "+t, t);
            return false;
        }
    }

    @Override
    public void stop() {
        runWorkflow(WorkflowSoftwareProcess.STOP_WORKFLOW);
    }


}
