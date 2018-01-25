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
package org.apache.brooklyn.core.sensor.windows;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddSensor;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.http.HttpRequestSensor;
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.feed.ssh.SshValueFunctions;
import org.apache.brooklyn.feed.windows.CmdFeed;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.internal.winrm.WinRmTool;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/** 
 * Configurable {@link EntityInitializer} which adds an WinRm sensor feed running the <code>command</code> supplied
 * in order to populate the sensor with the indicated <code>name</code>. Note that the <code>targetType</code> is ignored,
 * and always set to {@link String}.
 * NB!! Consider using small length for command and rundir.
 *
 *
 * @see HttpRequestSensor
 */
@Beta
public final class WinRmCommandSensor<T> extends AddSensor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(WinRmCommandSensor.class);

    public static final ConfigKey<String> SENSOR_COMMAND = ConfigKeys.newStringConfigKey("command", "SSH command to execute for sensor");
    public static final ConfigKey<String> SENSOR_EXECUTION_DIR = ConfigKeys.newStringConfigKey("executionDir", "Directory where the command should run; "
        + "if not supplied, executes in the entity's run dir (or home dir if no run dir is defined); "
        + "use '~' to always execute in the home dir, or 'custom-feed/' to execute in a custom-feed dir relative to the run dir");
    public static final ConfigKey<Map<String, String>> SENSOR_ENVIRONMENT = WinRmTool.ENVIRONMENT;

    public static final ConfigKey<Boolean> SUPPRESS_DUPLICATES = ConfigKeys.newBooleanConfigKey(
            "suppressDuplicates", 
            "Whether to publish the sensor value again, if it is the same as the previous value",
            Boolean.FALSE);

    protected final String command;
    protected final String executionDir;
    protected final Map<String,String> sensorEnv;

    public WinRmCommandSensor(final ConfigBag params) {
        super(params);

        // TODO create a supplier for the command string to support attribute embedding
        command = Preconditions.checkNotNull(params.get(SENSOR_COMMAND), "SSH command must be supplied when defining this sensor");
        executionDir = params.get(SENSOR_EXECUTION_DIR);
        sensorEnv = params.get(SENSOR_ENVIRONMENT);
    }

    @Override
    public void apply(final EntityLocal entity) {
            super.apply(entity);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding WinRM sensor {} to {}", name, entity);
        }

        final Boolean suppressDuplicates = EntityInitializers.resolve(getRememberedParams(), SUPPRESS_DUPLICATES);

        Supplier<Map<String,String>> envSupplier = new Supplier<Map<String,String>>() {
            @Override
            public Map<String, String> get() {
                Map<String, String> env = MutableMap.copyOf(entity.getConfig(SENSOR_ENVIRONMENT));

                // Add the shell environment entries from our configuration
                if (sensorEnv != null) env.putAll(sensorEnv);

                // Try to resolve the configuration in the env Map
                try {
                    env = (Map<String, String>) Tasks.resolveDeepValue(env, String.class, ((EntityInternal) entity).getExecutionContext());
                } catch (InterruptedException | ExecutionException e) {
                    Exceptions.propagateIfFatal(e);
                }

                // Convert the environment into strings with the serializer
                ShellEnvironmentSerializer serializer = new ShellEnvironmentSerializer(((EntityInternal) entity).getManagementContext());
                return serializer.serialize(env);
            }
        };

        Supplier<String> commandSupplier = new Supplier<String>() {
            @Override
            public String get() {
                return makeCommandExecutingInDirectory(command, executionDir, entity);
            }
        };

        CommandPollConfig<T> pollConfig = new CommandPollConfig<T>(sensor)
                .period(period)
                .env(envSupplier)
                .command(commandSupplier)
                .suppressDuplicates(Boolean.TRUE.equals(suppressDuplicates))
                .checkSuccess(SshValueFunctions.exitStatusEquals(0))
                .onFailureOrException(Functions.constant((T) null))
                .onSuccess(Functions.compose(new Function<String, T>() {
                        @Override
                        public T apply(String input) {
                            return TypeCoercions.coerce(Strings.trimEnd(input), (Class<T>) sensor.getType());
                        }}, SshValueFunctions.stdout()));

        CmdFeed feed = CmdFeed.builder()
                .entity(entity)
                .onlyIfServiceUp()
                .poll(pollConfig)
                .build();

        entity.addFeed(feed);
    }

    @Beta
    public static String makeCommandExecutingInDirectory(String command, String executionDir, Entity entity) {
        String finalCommand = command;
        String execDir = executionDir;
        if (Strings.isNonBlank(execDir) && !"~".equals(execDir)) {
            finalCommand = "(if exist \"" + execDir + "\" (rundll32) else (mkdir \""+execDir+"\")) && cd \""+execDir+"\" && "+finalCommand;
        }
        return finalCommand;
    }

}
