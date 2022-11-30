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
package org.apache.brooklyn.feed.function;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.feed.AbstractFeed;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.TypeToken;

/**
 * Provides a feed of attribute values, by periodically invoking functions.
 * 
 * Example usage (e.g. in an entity that extends SoftwareProcessImpl):
 * <pre>
 * {@code
 * private FunctionFeed feed;
 * 
 * //@Override
 * protected void connectSensors() {
 *   super.connectSensors();
 *   
 *   feed = FunctionFeed.builder()
 *     .entity(this)
 *     .poll(new FunctionPollConfig<Object, Boolean>(SERVICE_UP)
 *         .period(500, TimeUnit.MILLISECONDS)
 *         .callable(new Callable<Boolean>() {
 *             public Boolean call() throws Exception {
 *               return getDriver().isRunning();
 *             }
 *         })
 *         .onExceptionOrFailure(Functions.constant(Boolan.FALSE))
 *     .build();
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
 * @author aled
 */
public class FunctionFeed extends AbstractFeed {

    private static final Logger log = LoggerFactory.getLogger(FunctionFeed.class);

    // Treat as immutable once built
    @SuppressWarnings("serial")
    public static final ConfigKey<SetMultimap<FunctionPollIdentifier, FunctionPollConfig<?,?>>> POLLS = ConfigKeys.newConfigKey(
            new TypeToken<SetMultimap<FunctionPollIdentifier, FunctionPollConfig<?,?>>>() {},
            "polls");

    public static Builder builder() {
        return new Builder();
    }
    
    public static Builder builder(String uniqueTag) {
        return new Builder().uniqueTag(uniqueTag);
    }
    
    public static class Builder {
        private Entity entity;
        private boolean onlyIfServiceUp = false;
        private long period = 500;
        private TimeUnit periodUnits = TimeUnit.MILLISECONDS;
        private List<FunctionPollConfig<?,?>> polls = Lists.newArrayList();
        private String name;
        private String uniqueTag;
        private volatile boolean built;

        public Builder entity(Entity val) {
            this.entity = val;
            return this;
        }
        public Builder onlyIfServiceUp() { return onlyIfServiceUp(true); }
        public Builder onlyIfServiceUp(boolean onlyIfServiceUp) { 
            this.onlyIfServiceUp = onlyIfServiceUp; 
            return this; 
        }
        public Builder period(Duration d) {
            return period(d.toMilliseconds(), TimeUnit.MILLISECONDS);
        }
        public Builder period(long millis) {
            return period(millis, TimeUnit.MILLISECONDS);
        }
        public Builder period(long val, TimeUnit units) {
            this.period = val;
            this.periodUnits = units;
            return this;
        }
        public Builder poll(FunctionPollConfig<?,?> config) {
            polls.add(config);
            return this;
        }
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        public Builder uniqueTag(String uniqueTag) {
            this.uniqueTag = uniqueTag;
            return this;
        }
        public FunctionFeed build() {
            return build(false);
        }
        /** builds the feed, optionally registering it on entity. that should be done unless either the caller does it with addFeed (but no hard in adding twice),
         * or if it should not be persisted and it is guaranteed to be re-added on rebind, as is done with SoftwareProcess connectSensors */
        public FunctionFeed build(boolean registerOnEntity) {
            built = true;
            return AbstractFeed.initAndMaybeStart(new FunctionFeed(this), entity, registerOnEntity);
        }
        @Override
        protected void finalize() {
            if (!built) log.warn("FunctionFeed.Builder created, but build() never called");
        }
    }
    
    private static class FunctionPollIdentifier {
        final Callable<?> job;

        private FunctionPollIdentifier(Callable<?> job) {
            this.job = checkNotNull(job, "job");
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(job);
        }
        
        @Override
        public boolean equals(Object other) {
            return (other instanceof FunctionPollIdentifier) && Objects.equal(job, ((FunctionPollIdentifier)other).job);
        }
    }

    /**
     * For rebind; do not call directly; use builder
     */
    public FunctionFeed() {
    }
    
    protected FunctionFeed(Builder builder) {
        config().set(ONLY_IF_SERVICE_UP, builder.onlyIfServiceUp);
        
        SetMultimap<FunctionPollIdentifier, FunctionPollConfig<?,?>> polls = HashMultimap.<FunctionPollIdentifier,FunctionPollConfig<?,?>>create();
        for (FunctionPollConfig<?,?> config : builder.polls) {
            if (!config.isEnabled()) continue;
            @SuppressWarnings({ "rawtypes", "unchecked" })
            FunctionPollConfig<?,?> configCopy = new FunctionPollConfig(config);
            if (configCopy.getPeriod() < 0) configCopy.period(builder.period, builder.periodUnits);
            Callable<?> job = config.getCallable();
            polls.put(new FunctionPollIdentifier(job), configCopy);
        }

        if (builder.name!=null) setDisplayName(builder.name);
        else if (builder.uniqueTag!=null) setDisplayName(builder.uniqueTag);

        config().set(POLLS, polls);
        initUniqueTag(builder.uniqueTag, polls.values());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void preStart() {
        getPoller().scheduleFeed(this, getConfig(POLLS), pollInfo -> pollInfo.job);
    }
}
