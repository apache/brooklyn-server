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
package org.apache.brooklyn.feed;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.feed.AbstractFeed;
import org.apache.brooklyn.core.feed.AttributePollHandler;
import org.apache.brooklyn.core.feed.DelegatingPollHandler;
import org.apache.brooklyn.core.feed.Poller;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.feed.ssh.SshPollValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

/**
 * Provides a feed of attribute values, by polling over Command Line Shell Interface.
 * 
 * Example usage (e.g. in an entity that extends SoftwareProcessImpl):
 * <pre>
 * {@code
 * private AbstractCommandFeed feed;
 * 
 * //@Override
 * protected void connectSensors() {
 *   super.connectSensors();
 *   
 *   feed = SshFeed.builder()
 *       .entity(this)
 *       .machine(mySshMachineLachine)
 *       .poll(new CommandPollConfig<Boolean>(SERVICE_UP)
 *           .command("rabbitmqctl -q status")
 *           .onSuccess(new Function<SshPollValue, Boolean>() {
 *               public Boolean apply(SshPollValue input) {
 *                 return (input.getExitStatus() == 0);
 *               }}))
 *       .build();
 * }
 * 
 * {@literal @}Override
 * protected void disconnectSensors() {
 *   super.disconnectSensors();
 *   if (feed != null) feed.stop();
 * }
 * }
 * </pre>
 * 
 */
public abstract class AbstractCommandFeed extends AbstractFeed {

    public static final Logger log = LoggerFactory.getLogger(AbstractCommandFeed.class);
    
    @SuppressWarnings("serial")
    public static final ConfigKey<Supplier<MachineLocation>> MACHINE = ConfigKeys.newConfigKey(
            new TypeToken<Supplier<MachineLocation>>() {},
            "machine");

    public static final ConfigKey<Boolean> EXEC_AS_COMMAND = ConfigKeys.newBooleanConfigKey("execAsCommand");
    
    @SuppressWarnings("serial")
    public static final ConfigKey<SetMultimap<CommandPollIdentifier, CommandPollConfig<?>>> POLLS = ConfigKeys.newConfigKey(
            new TypeToken<SetMultimap<CommandPollIdentifier, CommandPollConfig<?>>>() {},
            "polls");
    
    public static abstract class Builder<T extends AbstractCommandFeed, B extends Builder<T, B>> {
        private Entity entity;
        private boolean onlyIfServiceUp = false;
        private Supplier<MachineLocation> machine;
        private Duration period = Duration.of(500, TimeUnit.MILLISECONDS);
        private boolean execAsCommand = false;
        private String uniqueTag;
        private volatile boolean built;
        
        public B entity(Entity val) {
            this.entity = val;
            return self();
        }
        public B onlyIfServiceUp() { return onlyIfServiceUp(true); }
        public B onlyIfServiceUp(boolean onlyIfServiceUp) {
            this.onlyIfServiceUp = onlyIfServiceUp; 
            return self();
        }
        /** optional, to force a machine; otherwise it is inferred from the entity */
        public B machine(MachineLocation val) { return machine(Suppliers.ofInstance(val)); }
        /** optional, to force a machine; otherwise it is inferred from the entity */
        public B machine(Supplier<MachineLocation> val) {
            this.machine = val;
            return self();
        }
        public B period(Duration period) {
            this.period = period;
            return self();
        }
        public B period(long millis) {
            return period(Duration.of(millis, TimeUnit.MILLISECONDS));
        }
        public B period(long val, TimeUnit units) {
            return period(Duration.of(val, units));
        }
        public abstract B poll(CommandPollConfig<?> config);
        public abstract List<CommandPollConfig<?>> getPolls();

        public B execAsCommand() {
            execAsCommand = true;
            return self();
        }
        public B execAsScript() {
            execAsCommand = false;
            return self();
        }
        public B uniqueTag(String uniqueTag) {
            this.uniqueTag = uniqueTag;
            return self();
        }
        
        protected abstract B self();
        
        protected abstract T instantiateFeed();

        public T build() {
            built = true;
            T result = instantiateFeed();
            result.setEntity(checkNotNull((EntityLocal)entity, "entity"));
            result.start();
            return result;
        }

        @Override
        protected void finalize() {
            if (!built) log.warn("SshFeed.Builder created, but build() never called");
        }
    }
    
    protected static class CommandPollIdentifier {
        final Supplier<String> command;
        final Supplier<Map<String, String>> env;

        protected CommandPollIdentifier(Supplier<String> command, Supplier<Map<String, String>> env) {
            this.command = checkNotNull(command, "command");
            this.env = checkNotNull(env, "env");
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(command, env);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CommandPollIdentifier)) {
                return false;
            }
            CommandPollIdentifier o = (CommandPollIdentifier) other;
            return Objects.equal(command, o.command) &&
                    Objects.equal(env, o.env);
        }
    }

    /**
     * For rebind; do not call directly; use builder
     */
    public AbstractCommandFeed() {
    }

    protected AbstractCommandFeed(final Builder builder) {
        config().set(ONLY_IF_SERVICE_UP, builder.onlyIfServiceUp);
        config().set(MACHINE, builder.machine);
        config().set(EXEC_AS_COMMAND, builder.execAsCommand);
        
        SetMultimap<CommandPollIdentifier, CommandPollConfig<?>> polls = HashMultimap.<CommandPollIdentifier,CommandPollConfig<?>>create();
        for (CommandPollConfig<?> config : (List<CommandPollConfig<?>>)builder.getPolls()) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            CommandPollConfig<?> configCopy = new CommandPollConfig(config);
            if (configCopy.getPeriod() < 0) configCopy.period(builder.period);
            polls.put(new CommandPollIdentifier(config.getCommandSupplier(), config.getEnvSupplier()), configCopy);
        }
        config().set(POLLS, polls);
        initUniqueTag(builder.uniqueTag, polls.values());
    }

    protected MachineLocation getMachine() {
        Supplier<MachineLocation> supplier = config().get(MACHINE);
        if (supplier != null) {
            return supplier.get();
        } else {
            return Locations.findUniqueMachineLocation(entity.getLocations()).get();
        }
    }
    
    @Override
    protected void preStart() {
        SetMultimap<CommandPollIdentifier, CommandPollConfig<?>> polls = config().get(POLLS);
        
        for (final CommandPollIdentifier pollInfo : polls.keySet()) {
            Set<CommandPollConfig<?>> configs = polls.get(pollInfo);
            long minPeriod = Integer.MAX_VALUE;
            Set<AttributePollHandler<? super SshPollValue>> handlers = Sets.newLinkedHashSet();

            for (CommandPollConfig<?> config : configs) {
                handlers.add(new AttributePollHandler<SshPollValue>(config, entity, this));
                if (config.getPeriod() > 0) minPeriod = Math.min(minPeriod, config.getPeriod());
            }
            
            getPoller().scheduleAtFixedRate(
                    new Callable<SshPollValue>() {
                        @Override
                        public SshPollValue call() throws Exception {
                            return exec(pollInfo.command.get(), pollInfo.env.get());
                        }}, 
                    new DelegatingPollHandler<SshPollValue>(handlers),
                    minPeriod);
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    protected Poller<SshPollValue> getPoller() {
        return (Poller<SshPollValue>) super.getPoller();
    }
    
    protected abstract SshPollValue exec(String command, Map<String,String> env) throws IOException;
}
