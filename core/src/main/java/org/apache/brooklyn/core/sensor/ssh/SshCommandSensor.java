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
package org.apache.brooklyn.core.sensor.ssh;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.AbstractAddSensorFeed;
import org.apache.brooklyn.core.sensor.http.HttpRequestSensor;
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.feed.ssh.SshFeed;
import org.apache.brooklyn.feed.ssh.SshValueFunctions;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.StringFunctions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

/** 
 * Configurable {@link EntityInitializer} which adds an SSH sensor feed running the <code>command</code> supplied
 * in order to populate the sensor with the indicated <code>name</code>. Note that the <code>targetType</code> is ignored,
 * and always set to {@link String}.
 *
 * @see HttpRequestSensor
 */
@Beta
public final class SshCommandSensor<T> extends AbstractAddSensorFeed<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SshCommandSensor.class);

    public static final ConfigKey<String> SENSOR_COMMAND = ConfigKeys.newStringConfigKey("command", "SSH command to execute for sensor");
    public static final ConfigKey<String> SENSOR_EXECUTION_DIR = ConfigKeys.newStringConfigKey("executionDir", "Directory where the command should run; "
        + "if not supplied, executes in the entity's run dir (or home dir if no run dir is defined); "
        + "use '~' to always execute in the home dir, or 'custom-feed/' to execute in a custom-feed dir relative to the run dir");
    public static final ConfigKey<Object> VALUE_ON_ERROR = ConfigKeys.newConfigKey(Object.class, "value.on.error",
            "Value to be used if an error occurs whilst executing the ssh command", null);
    public static final MapConfigKey<Object> SENSOR_SHELL_ENVIRONMENT = BrooklynConfigKeys.SHELL_ENVIRONMENT;

    // Fields are kept for deserialization purposes; however will rely on the values being
    // re-computed from the config map, rather than being restored from persistence.
    @SuppressWarnings("unused")
    private String command;
    @SuppressWarnings("unused")
    private String executionDir;
    @SuppressWarnings("unused")
    private Map<String,Object> sensorEnv;
    
    public SshCommandSensor(final ConfigBag params) {
        super(params);
    }

    @Override
    public void apply(final EntityLocal entity) {
        super.apply(entity);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding SSH sensor {} to {}", name, entity);
        }

        final Boolean suppressDuplicates = EntityInitializers.resolve(params, SUPPRESS_DUPLICATES);
        final Duration logWarningGraceTimeOnStartup = EntityInitializers.resolve(params, LOG_WARNING_GRACE_TIME_ON_STARTUP);
        final Duration logWarningGraceTime = EntityInitializers.resolve(params, LOG_WARNING_GRACE_TIME);

        Supplier<Map<String,String>> envSupplier = new EnvSupplier(entity, params);
        
        Supplier<String> commandSupplier = new CommandSupplier(entity, params);

        CommandPollConfig<T> pollConfig = new CommandPollConfig<T>(sensor)
                .period(period)
                .env(envSupplier)
                .command(commandSupplier)
                .suppressDuplicates(Boolean.TRUE.equals(suppressDuplicates))
                .checkSuccess(SshValueFunctions.exitStatusEquals(0))
                .onFailureOrException(Functions.constant((T)params.get(VALUE_ON_ERROR)))
                .onSuccess(Functionals.chain(
                        SshValueFunctions.stdout(),
                        StringFunctions.trimEnd(),
                        TypeCoercions.function((Class<T>) sensor.getType())))
                .logWarningGraceTimeOnStartup(logWarningGraceTimeOnStartup)
                .logWarningGraceTime(logWarningGraceTime);

        SshFeed feed = SshFeed.builder()
                .entity(entity)
                .onlyIfServiceUp()
                .poll(pollConfig)
                .build();

        entity.addFeed(feed);
        
        // Deprecated; kept for backwards compatibility with historic persisted state
        new Supplier<Map<String,String>>() {
            @Override
            public Map<String, String> get() {
                if (entity == null) return ImmutableMap.of(); // See BROOKLYN-568
                
                Map<String, Object> env = MutableMap.copyOf(entity.getConfig(BrooklynConfigKeys.SHELL_ENVIRONMENT));

                // Add the shell environment entries from our configuration
                Map<String,Object> sensorEnv = params.get(SENSOR_SHELL_ENVIRONMENT);
                if (sensorEnv != null) env.putAll(sensorEnv);

                // Try to resolve the configuration in the env Map
                try {
                    env = (Map<String, Object>) Tasks.resolveDeepValue(env, Object.class, ((EntityInternal) entity).getExecutionContext());
                } catch (InterruptedException | ExecutionException e) {
                    Exceptions.propagateIfFatal(e);
                }

                // Convert the environment into strings with the serializer
                ShellEnvironmentSerializer serializer = new ShellEnvironmentSerializer(((EntityInternal) entity).getManagementContext());
                return serializer.serialize(env);
            }
        };

        // Deprecated; kept for backwards compatibility with historic persisted state
        new Supplier<String>() {
            @Override
            public String get() {
                // Note that entity may be null during rebind (e.g. if this SshFeed is orphaned, with no associated entity):
                // See https://issues.apache.org/jira/browse/BROOKLYN-568.
                // We therefore guard against null in makeCommandExecutingInDirectory.
                String command = Preconditions.checkNotNull(EntityInitializers.resolve(params, SENSOR_COMMAND));
                String dir = EntityInitializers.resolve(params, SENSOR_EXECUTION_DIR);
                return makeCommandExecutingInDirectory(command, dir, entity);
            }
        };

        // Deprecated; kept for backwards compatibility with historic persisted state
        new Function<String, T>() {
            @Override public T apply(String input) {
                return TypeCoercions.coerce(Strings.trimEnd(input), (Class<T>) sensor.getType());
            }
        };
    }

    @Beta
    public static String makeCommandExecutingInDirectory(String command, String executionDir, Entity entity) {
        String finalCommand = command;
        String execDir = executionDir;
        if (Strings.isBlank(execDir)) {
            // default to run dir
            execDir = (entity != null) ? entity.getAttribute(BrooklynConfigKeys.RUN_DIR) : null;
            // if no run dir, default to home
            if (Strings.isBlank(execDir)) {
                execDir = "~";
            }
        } else if (!Os.isAbsolutish(execDir)) {
            // relative paths taken wrt run dir
            String runDir = (entity != null) ? entity.getAttribute(BrooklynConfigKeys.RUN_DIR) : null;
            if (!Strings.isBlank(runDir)) {
                execDir = Os.mergePaths(runDir, execDir);
            }
        }
        if (!"~".equals(execDir)) {
            finalCommand = "mkdir -p '"+execDir+"' && cd '"+execDir+"' && "+finalCommand;
        }
        return finalCommand;
    }

    private static class EnvSupplier implements Supplier<Map<String,String>> {
        private final Entity entity;
        private final Object rawSensorShellEnv;
        
        EnvSupplier(Entity entity, ConfigBag params) {
            this.entity = entity;
            this.rawSensorShellEnv = params.getAllConfigRaw().getOrDefault(SENSOR_SHELL_ENVIRONMENT.getName(), SENSOR_SHELL_ENVIRONMENT.getDefaultValue());
        }
        
        @Override
        public Map<String, String> get() {
            if (entity == null) return ImmutableMap.of(); // See BROOKLYN-568
            
            Map<String, Object> env = MutableMap.copyOf(entity.getConfig(BrooklynConfigKeys.SHELL_ENVIRONMENT));

            // Add the shell environment entries from our configuration
            if (rawSensorShellEnv != null) {
                env.putAll(TypeCoercions.coerce(rawSensorShellEnv, new TypeToken<Map<String,Object>>() {}));
            }

            // Try to resolve the configuration in the env Map
            try {
                env = (Map<String, Object>) Tasks.resolveDeepValue(env, Object.class, ((EntityInternal) entity).getExecutionContext());
            } catch (InterruptedException | ExecutionException e) {
                Exceptions.propagateIfFatal(e);
            }

            // Convert the environment into strings with the serializer
            ShellEnvironmentSerializer serializer = new ShellEnvironmentSerializer(((EntityInternal) entity).getManagementContext());
            return serializer.serialize(env);
        }
    }

    private static class CommandSupplier implements Supplier<String> {
        private final Entity entity;
        private final Object rawSensorCommand;
        private final Object rawSensorExecDir;
        
        CommandSupplier(Entity entity, ConfigBag params) {
            this.entity = entity;
            this.rawSensorCommand = params.getAllConfigRaw().get(SENSOR_COMMAND.getName());
            this.rawSensorExecDir = params.getAllConfigRaw().get(SENSOR_EXECUTION_DIR.getName());
        }
        
        @Override
        public String get() {
            // Note that entity may be null during rebind (e.g. if this SshFeed is orphaned, with no associated entity):
            // See https://issues.apache.org/jira/browse/BROOKLYN-568.
            // We therefore guard against null in makeCommandExecutingInDirectory.
            ConfigBag params = ConfigBag.newInstance();
            if (rawSensorCommand != null) {
                params.putStringKey(SENSOR_COMMAND.getName(), rawSensorCommand);
            }
            if (rawSensorExecDir != null) {
                params.putStringKey(SENSOR_EXECUTION_DIR.getName(), rawSensorExecDir);
            }
            String command = Preconditions.checkNotNull(EntityInitializers.resolve(params, SENSOR_COMMAND));
            String dir = EntityInitializers.resolve(params, SENSOR_EXECUTION_DIR);
            return makeCommandExecutingInDirectory(command, dir, entity);
        }
    }
}
