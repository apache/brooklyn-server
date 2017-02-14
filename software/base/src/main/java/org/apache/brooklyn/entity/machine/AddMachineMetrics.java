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
package org.apache.brooklyn.entity.machine;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.primitives.Doubles;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.enricher.stock.PercentageEnricher;
import org.apache.brooklyn.enricher.stock.YamlRollingTimeWindowMeanEnricher;
import org.apache.brooklyn.enricher.stock.YamlTimeWeightedDeltaEnricher;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.feed.ssh.SshFeed;
import org.apache.brooklyn.feed.ssh.SshPollConfig;
import org.apache.brooklyn.feed.ssh.SshPollValue;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

/**
 * Adds a {@link SSHFeed feed} with sensors returning details about the machine the entity is running on.
 * <p>
 * The machine must be SSHable and running Linux.
 *
 * @since 0.10.0
 */
@Beta
public class AddMachineMetrics implements EntityInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(AddMachineMetrics.class);

    static {
        MachineAttributes.init();
    }

    @Override
    public void apply(EntityLocal entity) {
        SshFeed machineMetricsFeed = createMachineMetricsFeed(entity);
        ((EntityInternal) entity).feeds().add(machineMetricsFeed);
        addMachineMetricsEnrichers(entity);
        LOG.info("Configured machine metrics feed and enrichers on {}", entity);
    }

    public static void addMachineMetricsEnrichers(Entity entity) {
        entity.enrichers().add(EnricherSpec.create(YamlTimeWeightedDeltaEnricher.class)
                .configure(YamlTimeWeightedDeltaEnricher.SOURCE_SENSOR, MachineAttributes.USED_MEMORY)
                .configure(YamlTimeWeightedDeltaEnricher.TARGET_SENSOR, MachineAttributes.USED_MEMORY_DELTA_PER_SECOND_LAST));
        entity.enrichers().add(EnricherSpec.create(YamlRollingTimeWindowMeanEnricher.class)
                .configure(YamlRollingTimeWindowMeanEnricher.SOURCE_SENSOR, MachineAttributes.USED_MEMORY_DELTA_PER_SECOND_LAST)
                .configure(YamlRollingTimeWindowMeanEnricher.TARGET_SENSOR, MachineAttributes.USED_MEMORY_DELTA_PER_SECOND_IN_WINDOW));

        entity.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, MachineAttributes.USED_MEMORY)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, MachineAttributes.TOTAL_MEMORY)
                .configure(PercentageEnricher.TARGET_SENSOR, MachineAttributes.USED_MEMORY_PERCENT)
                .configure(PercentageEnricher.SUPPRESS_DUPLICATES, true));

    }

    public static SshFeed createMachineMetricsFeed(Entity entity) {
        boolean retrieveUsageMetrics = entity.config().get(SoftwareProcess.RETRIEVE_USAGE_METRICS);
        return SshFeed.builder()
                .uniqueTag("machineMetricsFeed")
                .period(Duration.THIRTY_SECONDS)
                .entity(entity)
                .poll(SshPollConfig.forSensor(MachineAttributes.UPTIME)
                        .command("cat /proc/uptime")
                        .enabled(retrieveUsageMetrics)
                        .onFailureOrException(Functions.<Duration>constant(null))
                        .onSuccess(new Function<SshPollValue, Duration>() {
                            @Override
                            public Duration apply(SshPollValue input) {
                                return Duration.seconds(Double.valueOf(Strings.getFirstWord(input.getStdout())));
                            }
                        }))
                .poll(SshPollConfig.forSensor(MachineAttributes.LOAD_AVERAGE)
                        .command("uptime")
                        .enabled(retrieveUsageMetrics)
                        .onFailureOrException(Functions.<Double>constant(null))
                        .onSuccess(new Function<SshPollValue, Double>() {
                            @Override
                            public Double apply(SshPollValue input) {
                                String loadAverage = Strings.getFirstWordAfter(input.getStdout(), "load average:").replace(",", "");
                                return Double.valueOf(loadAverage);
                            }
                        }))
                .poll(SshPollConfig.forSensor(MachineAttributes.CPU_USAGE)
                        .command("ps -A -o pcpu")
                        .enabled(retrieveUsageMetrics)
                        .onFailureOrException(Functions.<Double>constant(null))
                        .onSuccess(new Function<SshPollValue, Double>() {
                            @Override
                            public Double apply(SshPollValue input) {
                                Double cpu = 0d;
                                Iterable<String> stdout = Splitter.on(CharMatcher.BREAKING_WHITESPACE).omitEmptyStrings().split(input.getStdout());
                                for (Double each : FluentIterable.from(stdout).skip(1).transform(Doubles.stringConverter())) { cpu += each; }
                                return cpu / 100d;
                            }
                        }))
                .poll(SshPollConfig.forSensor(MachineAttributes.USED_MEMORY)
                        .command("free | grep Mem:")
                        .enabled(retrieveUsageMetrics)
                        .onFailureOrException(Functions.<Long>constant(null))
                        .onSuccess(new Function<SshPollValue, Long>() {
                            @Override
                            public Long apply(SshPollValue input) {
                                List<String> memoryData = Splitter.on(" ").omitEmptyStrings().splitToList(Strings.getFirstLine(input.getStdout()));
                                return Long.parseLong(memoryData.get(2));
                            }
                        }))
                .poll(SshPollConfig.forSensor(MachineAttributes.FREE_MEMORY)
                        .command("free | grep Mem:")
                        .enabled(retrieveUsageMetrics)
                        .onFailureOrException(Functions.<Long>constant(null))
                        .onSuccess(new Function<SshPollValue, Long>() {
                            @Override
                            public Long apply(SshPollValue input) {
                                List<String> memoryData = Splitter.on(" ").omitEmptyStrings().splitToList(Strings.getFirstLine(input.getStdout()));
                                return Long.parseLong(memoryData.get(3));
                            }
                        }))
                .poll(SshPollConfig.forSensor(MachineAttributes.TOTAL_MEMORY)
                        .command("free | grep Mem:")
                        .enabled(retrieveUsageMetrics)
                        .onFailureOrException(Functions.<Long>constant(null))
                        .onSuccess(new Function<SshPollValue, Long>() {
                            @Override
                            public Long apply(SshPollValue input) {
                                List<String> memoryData = Splitter.on(" ").omitEmptyStrings().splitToList(Strings.getFirstLine(input.getStdout()));
                                return Long.parseLong(memoryData.get(1));
                            }
                        }))
                .build();
    }
}
