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

import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.AttributeSensor.SensorPersistenceMode;
import org.apache.brooklyn.core.config.render.RendererHints;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.math.MathFunctions;
import org.apache.brooklyn.util.text.ByteSizeStrings;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.base.Function;

public class MachineAttributes {

    /**
     * Do not instantiate.
     */
    private MachineAttributes() {}

    /*
     * Sensor attributes for machines.
     */

    public static final AttributeSensor<Duration> UPTIME = Sensors.builder(Duration.class, "machine.uptime")
            .description("Current uptime")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Double> LOAD_AVERAGE = Sensors.builder(Double.class, "machine.loadAverage")
            .description("Current load average")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Double> CPU_USAGE = Sensors.builder(Double.class, "machine.cpu")
            .description("Current CPU usage")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Double> AVERAGE_CPU_USAGE = Sensors.builder(Double.class, "cpu.average")
            .description("Average CPU usage across the cluster")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Long> FREE_MEMORY = Sensors.builder(Long.class, "machine.memory.free")
            .description("Current free memory")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Long> TOTAL_MEMORY = Sensors.builder(Long.class, "machine.memory.total")
            .description("Total memory")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Long> USED_MEMORY = Sensors.builder(Long.class, "machine.memory.used")
            .description("Current memory usage")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Double> USED_MEMORY_DELTA_PER_SECOND_LAST = Sensors.builder(Double.class, "memory.used.delta")
            .description("Change in memory usage per second")
            .persistence(SensorPersistenceMode.NONE)
            .build();
    
    public static final AttributeSensor<Double> USED_MEMORY_DELTA_PER_SECOND_IN_WINDOW = Sensors.builder(Double.class, "memory.used.windowed")
            .description("Average change in memory usage over 30s")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Double> USED_MEMORY_PERCENT = Sensors.builder(Double.class, "memory.used.percent")
            .description("The percentage of memory used")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    public static final AttributeSensor<Double> AVERAGE_USED_MEMORY_PERCENT = Sensors.builder(Double.class, "memory.used.percent.average")
            .description("Average percentage of memory used across the cluster")
            .persistence(SensorPersistenceMode.NONE)
            .build();

    private static AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Setup renderer hints.
     */
    public static void init() {
        if (initialized.getAndSet(true)) return;

        final Function<Double, Long> longValue = new Function<Double, Long>() {
            @Override
            public Long apply(@Nullable Double input) {
                if (input == null) return null;
                return input.longValue();
            }
        };

        RendererHints.register(CPU_USAGE, RendererHints.displayValue(MathFunctions.percent(2)));
        RendererHints.register(AVERAGE_CPU_USAGE, RendererHints.displayValue(MathFunctions.percent(2)));

        RendererHints.register(USED_MEMORY_PERCENT, RendererHints.displayValue(MathFunctions.percent(2)));
        RendererHints.register(AVERAGE_USED_MEMORY_PERCENT, RendererHints.displayValue(MathFunctions.percent(2)));

        RendererHints.register(FREE_MEMORY, RendererHints.displayValue(Functionals.chain(MathFunctions.times(1000L), ByteSizeStrings.metric())));
        RendererHints.register(TOTAL_MEMORY, RendererHints.displayValue(Functionals.chain(MathFunctions.times(1000L), ByteSizeStrings.metric())));
        RendererHints.register(USED_MEMORY, RendererHints.displayValue(Functionals.chain(MathFunctions.times(1000L), ByteSizeStrings.metric())));
        RendererHints.register(USED_MEMORY_DELTA_PER_SECOND_LAST, RendererHints.displayValue(Functionals.chain(longValue, ByteSizeStrings.metric())));
        RendererHints.register(USED_MEMORY_DELTA_PER_SECOND_IN_WINDOW, RendererHints.displayValue(Functionals.chain(longValue, ByteSizeStrings.metric())));
    }

    static {
        init();
    }
}
