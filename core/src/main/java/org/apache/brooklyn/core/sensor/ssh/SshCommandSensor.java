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

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.sensor.AbstractAddSensorFeed;
import org.apache.brooklyn.core.sensor.AbstractAddTriggerableSensor;
import org.apache.brooklyn.core.sensor.http.HttpRequestSensor;
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.feed.ssh.SshFeed;
import org.apache.brooklyn.feed.ssh.SshValueFunctions;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.StringFunctions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** 
 * Configurable {@link EntityInitializer} which adds an SSH sensor feed running the <code>command</code> supplied
 * in order to populate the sensor with the indicated <code>name</code>. Note that the <code>targetType</code> is ignored,
 * and always set to {@link String}.
 *
 * @see HttpRequestSensor
 */
@Beta
public final class SshCommandSensor<T> extends AbstractAddTriggerableSensor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SshCommandSensor.class);

    public static final ConfigKey<String> SENSOR_COMMAND = ConfigKeys.newStringConfigKey("command", "SSH command to execute for sensor");
    public static final ConfigKey<String> SENSOR_COMMAND_URL = ConfigKeys.newStringConfigKey("commandUrl", "Remote SSH command to execute for sensor (takes precedence over command)");
    public static final ConfigKey<String> SENSOR_EXECUTION_DIR = ConfigKeys.newStringConfigKey("executionDir", "Directory where the command should run; "
        + "if not supplied, executes in the entity's run dir (or home dir if no run dir is defined); "
        + "use '~' to always execute in the home dir, or 'custom-feed/' to execute in a custom-feed dir relative to the run dir; not compatible with commandUrl");
    public static final ConfigKey<Object> VALUE_ON_ERROR = ConfigKeys.newConfigKey(Object.class, "value.on.error",
            "Value to be used if an error occurs whilst executing the ssh command", null);
    public static final MapConfigKey<Object> SENSOR_SHELL_ENVIRONMENT = BrooklynConfigKeys.SHELL_ENVIRONMENT;
    public static final ConfigKey<String> FORMAT = ConfigKeys.newStringConfigKey("format",
                    "Format to expect for the output; default to auto which will attempt a yaml/json parse for complex types, falling back to string, then coerce; " +
                    "other options are just 'string' (previous default) or 'yaml'", "auto");
    public static final ConfigKey<Boolean> LAST_YAML_DOCUMENT = ConfigKeys.newBooleanConfigKey("useLastYaml",
                    "Whether to trim the output ignoring everything up to and before the last `---` line if present when expecting yaml; " +
                    "useful if the script has quite a lot of output which should be ignored prior, with the value to be used for the sensor output last; " +
                    "default true (ignored if format is 'string')", true);

    final private AtomicBoolean commandUrlInstalled = new AtomicBoolean(false);

    public SshCommandSensor() {}
    public SshCommandSensor(ConfigBag params) {
        super(params);
    }

    @Override
    public void apply(final EntityLocal entity) {
        ConfigBag params = initParams();

        // previously if a commandUrl was used we would listen for the install dir to be set; but that doesn't survive rebind;
        // now we install on first run as part of the SshFeed
        apply(entity, params);
    }

    private void apply(final EntityLocal entity, final ConfigBag params) {

        AttributeSensor<T> sensor = addSensor(entity);

        String name = initParam(SENSOR_NAME);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding SSH sensor {} to {}", name, entity);
        }

        Supplier<Map<String,String>> envSupplier = new EnvSupplier(entity, params);
        CommandSupplier commandSupplier = new CommandSupplier(entity, params);

        CommandPollConfig<T> pollConfig = new CommandPollConfig<T>(sensor)
                .env(envSupplier)
                .command(commandSupplier)
                .checkSuccess(SshValueFunctions.exitStatusEquals(0))
                .onFailureOrException(Functions.constant((T)params.get(VALUE_ON_ERROR)))
                .onSuccess(Functionals.chain(SshValueFunctions.stdout(), new CoerceOutputFunction<>(sensor.getTypeToken(), initParam(FORMAT), initParam(LAST_YAML_DOCUMENT))));

        standardPollConfig(entity, initParams(), pollConfig);

        SshFeed.Builder feedBuilder = SshFeed.builder()
                .entity(entity)
                .onlyIfServiceUp(Maybe.ofDisallowingNull(EntityInitializers.resolve(params, ONLY_IF_SERVICE_UP)).or(true))
                .poll(pollConfig);

        String commandUrl = EntityInitializers.resolve(initParams(), SENSOR_COMMAND_URL);
        if (commandUrl!=null) {
            feedBuilder.commandUrlToInstallAndRun(commandUrl);
            // commandSupplier above will be ignored
            if (commandSupplier.rawSensorCommand!=null || commandSupplier.rawSensorExecDir!=null) {
                throw new IllegalArgumentException("commandUrl is not compatible with command or executionDir");
            }
        }

        SshFeed feed = feedBuilder.build();
        entity.addFeed(feed);
        
        // Deprecated; kept for backwards compatibility with historic persisted state
        new Supplier<Map<String,String>>() {
            @Override
            public Map<String, String> get() {
                if (entity == null) return ImmutableMap.of(); // See BROOKLYN-568
                
                Map<String, Object> env = MutableMap.copyOf(entity.getConfig(BrooklynConfigKeys.SHELL_ENVIRONMENT));

                // Add the shell environment entries from our configuration
                Map<String,Object> sensorEnv = initParams().get(SENSOR_SHELL_ENVIRONMENT);
                if (sensorEnv != null) env.putAll(sensorEnv);

                // Try to resolve the configuration in the env Map
                try {
                    env = (Map<String, Object>) Tasks.resolveDeepValueWithoutCoercion(env, ((EntityInternal) entity).getExecutionContext());
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
                String command = Preconditions.checkNotNull(EntityInitializers.resolve(initParams(), SENSOR_COMMAND));
                String dir = EntityInitializers.resolve(initParams(), SENSOR_EXECUTION_DIR);
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
    public static class CoerceOutputFunction<T> implements Function<String,T> {
        final TypeToken<T> typeToken;
        final String format;
        final Boolean useLastYamlDocument;

        public CoerceOutputFunction(TypeToken<T> typeToken, String format, Boolean useLastYamlDocument) {
            this.typeToken = typeToken;
            this.format = format;
            this.useLastYamlDocument = useLastYamlDocument;
        }

        public T apply(String input) {
            boolean doYaml = !"string".equalsIgnoreCase(format);
            boolean doString = !"yaml".equalsIgnoreCase(format);

            if ("auto".equalsIgnoreCase(format)) {
                if (String.class.equals(typeToken.getRawType()) || Boxing.isPrimitiveOrBoxedClass(typeToken.getRawType())) {
                    // don't do yaml if we want a string or a primitive
                    doYaml = false;
                }
            }

            Maybe<T> result1 = null;

            if (doYaml) {
                try {
                    String yamlInS = input;
                    if (!Boolean.FALSE.equals(useLastYamlDocument)) {
                        yamlInS = Yamls.lastDocumentFunction().apply(yamlInS);
                    }
                    Object yamlInO = Iterables.getOnlyElement(Yamls.parseAll(yamlInS));
                    result1 = TypeCoercions.tryCoerce(yamlInO, typeToken);
                    if (result1.isPresent()) doString = false;
                } catch (Exception e) {
                    if (result1==null) result1 = Maybe.absent(e);
                }
            }

            if (doString) {
                try {
                    return (T) Functionals.chain(StringFunctions.trimEnd(), TypeCoercions.function(typeToken.getRawType())).apply(input);
                } catch (Exception e) {
                    if (result1==null) result1 = Maybe.absent(e);
                }
            }

            if (result1.isAbsent() && Strings.isNonBlank(input)) {
                LOG.warn("Unable to convert to "+typeToken+": "+Maybe.Absent.getException(result1)+"\n"+input);
            }
            return result1.get();
        }
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
                env = (Map<String, Object>) Tasks.resolveDeepValueWithoutCoercion(env, ((EntityInternal) entity).getExecutionContext());
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

    private String command;
    private String executionDir;
    private Map<String,Object> sensorEnv;
    // introduced in 1.1 for legacy compatibility
    protected Object readResolve() {
        super.readResolve();
        initFromConfigBag(ConfigBag.newInstance()
                .putIfAbsentAndNotNull(SENSOR_COMMAND, command)
                .putIfAbsentAndNotNull(SENSOR_EXECUTION_DIR, executionDir)
                .putIfAbsentAndNotNull(SENSOR_SHELL_ENVIRONMENT, sensorEnv)
        );
        command = null;
        executionDir = null;
        sensorEnv = null;

        return this;
    }

}
