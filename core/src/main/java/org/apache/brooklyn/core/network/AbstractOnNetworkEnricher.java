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
package org.apache.brooklyn.core.network;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.TypeToken;

@Beta
public abstract class AbstractOnNetworkEnricher extends AbstractEnricher {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractOnNetworkEnricher.class);

    @SuppressWarnings("serial")
    public static ConfigKey<Collection<? extends AttributeSensor<?>>> SENSORS = ConfigKeys.newConfigKey(
            new TypeToken<Collection<? extends AttributeSensor<?>>>() {}, 
            "sensors",
            "The multiple sensors whose mapped values are to be re-published (with suffix \"mapped.networkName\"); "
                    + "if 'sensors' is not specified, defaults to 'mapMatching'");

    public static ConfigKey<String> MAP_MATCHING = ConfigKeys.newStringConfigKey(
            "mapMatching",
            "Whether to map all, based on a sensor naming convention (re-published with suffix \"mapped.networkName\"); "
                    + "if 'sensors' is not specified, defaults to matching case-insensitive suffix of "
                    + "'port', 'uri', 'url', 'endpoint' or 'hostAndPort'",
            "(?i)(.+\\.)?(port|uri|url|endpoint|hostandport)");

    @SuppressWarnings("serial")
    public static ConfigKey<Function<? super String, String>> SENSOR_NAME_CONVERTER = ConfigKeys.newConfigKey(
            new TypeToken<Function<? super String, String>>() {},
            "sensorNameConverter",
            "The converter to use, to map from the original sensor name to the re-published sensor name");

    public static class SensorNameConverter implements Function<String, String> {
        private final String network;
        
        public SensorNameConverter(String network) {
            this.network = network;
        }
        
        @Override
        public String apply(String input) {
            if (input == null) throw new NullPointerException("Sensor name must not be null");
            String lowerInput = input.toLowerCase();
            if (lowerInput.endsWith("uri")) {
                return input + ".mapped." + network;
            } else if (lowerInput.endsWith("url")) {
                return input + ".mapped." + network;
            } else if (lowerInput.endsWith("endpoint")) {
                return input + ".mapped." + network;
            } else if (lowerInput.endsWith("hostandport")) {
                return input + ".mapped." + network;
            } else if (lowerInput.endsWith("port")) {
                String prefix = input.substring(0, input.length() - "port".length());
                boolean includeDot = (!prefix.isEmpty() && !prefix.endsWith("."));
                return prefix + (includeDot ? "." : "") + "endpoint.mapped." + network;
            } else {
                return input + ".mapped." + network;
            }
        }
        
        @Override
        public String toString() {
            return getClass().getSimpleName()+"("+network+")";
        }
    }

    protected Collection<AttributeSensor<?>> sensors;
    protected Optional<Predicate<Sensor<?>>> mapMatching;
    protected Function<? super String, String> sensorNameConverter;
    protected PortForwardManager.AssociationListener pfmListener;
    
    protected abstract Optional<HostAndPort> getMappedEndpoint(Entity source, MachineLocation machine, int port);
    
    @Override
    public void setEntity(final EntityLocal entity) {
        super.setEntity(entity);
        
        checkConfig();
        sensors = resolveSensorsConfig();
        if (sensors.isEmpty()) {
            mapMatching = Optional.of(resolveMapMatchingConfig());
        } else {
            mapMatching = Optional.absent();
        }
        sensorNameConverter = getRequiredConfig(SENSOR_NAME_CONVERTER);
        
        subscriptions().subscribe(entity, AbstractEntity.LOCATION_ADDED, new SensorEventListener<Location>() {
            @Override public void onEvent(SensorEvent<Location> event) {
                LOG.debug("{} attempting transformations, triggered by location-added {}, to {}", new Object[] {AbstractOnNetworkEnricher.this, event.getValue(), entity});
                tryTransformAll();
            }});

        for (AttributeSensor<?> sensor : sensors) {
            subscriptions().subscribe(entity, sensor, new SensorEventListener<Object>() {
                @Override public void onEvent(SensorEvent<Object> event) {
                    LOG.debug("{} attempting transformations, triggered by sensor-event {}->{}, to {}", 
                            new Object[] {AbstractOnNetworkEnricher.this, event.getSensor().getName(), event.getValue(), entity});
                    tryTransform((AttributeSensor<?>)event.getSensor());
                }});
        }
        if (mapMatching.isPresent()) {
            Sensor<?> wildcardSensor = null;
            subscriptions().subscribe(entity, wildcardSensor, new SensorEventListener<Object>() {
                @Override public void onEvent(SensorEvent<Object> event) {
                    if (mapMatching.get().apply(event.getSensor())) {
                        LOG.debug("{} attempting transformations, triggered by sensor-event {}->{}, to {}", 
                                new Object[] {AbstractOnNetworkEnricher.this, event.getSensor().getName(), event.getValue(), entity});
                        tryTransform((AttributeSensor<?>)event.getSensor());
                    }
                }});
        }

        tryTransformAll();
    }

    protected void tryTransformAll() {
        if (!isRunning()) {
            return;
        }
        Maybe<MachineLocation> machine = getMachine();
        if (machine.isAbsent()) {
            return;
        }
        for (AttributeSensor<?> sensor : sensors) {
            try {
                tryTransform(machine.get(), sensor);
            } catch (Exception e) {
                // TODO Avoid repeated logging
                Exceptions.propagateIfFatal(e);
                LOG.warn("Problem transforming sensor "+sensor+" of "+entity, e);
            }
        }
        if (mapMatching.isPresent()) {
            for (Sensor<?> sensor : entity.getEntityType().getSensors()) {
                if (sensor instanceof AttributeSensor && mapMatching.get().apply(sensor)) {
                    try {
                        tryTransform(machine.get(), (AttributeSensor<?>)sensor);
                    } catch (Exception e) {
                        // TODO Avoid repeated logging
                        Exceptions.propagateIfFatal(e);
                        LOG.warn("Problem transforming sensor "+sensor+" of "+entity, e);
                    }
                }
            }
        }
    }

    protected void tryTransform(AttributeSensor<?> sensor) {
        if (!isRunning()) {
            return;
        }
        Maybe<MachineLocation> machine = getMachine();
        if (machine.isAbsent()) {
            return;
        }
        tryTransform(machine.get(), sensor);
    }
    
    protected void tryTransform(MachineLocation machine, AttributeSensor<?> sensor) {
        Object sensorVal = entity.sensors().get(sensor);
        if (sensorVal == null) {
            return;
        }
        Maybe<String> newVal = transformVal(machine, sensor, sensorVal);
        if (newVal.isAbsent()) {
            return;
        }
        AttributeSensor<String> mappedSensor = Sensors.newStringSensor(sensorNameConverter.apply(sensor.getName()));
        if (newVal.get().equals(entity.sensors().get(mappedSensor))) {
            // ignore duplicate
            return;
        }
        LOG.debug("{} publishing value {} (original sensor value {}) for mapped sensor {}, of entity {}", 
                new Object[] {this, newVal.get(), sensorVal, mappedSensor, entity});
        entity.sensors().set(mappedSensor, newVal.get());
    }
    
    protected Maybe<String> transformVal(MachineLocation machine, AttributeSensor<?> sensor, Object sensorVal) {
        if (sensorVal == null) {
            return Maybe.absent();
        } else if (isPort(sensorVal)) {
            int port = toInteger(sensorVal);
            return transformPort(entity, machine, port);
        } else if (isUri(sensorVal)) {
            return transformUri(entity, machine, sensorVal.toString());
        } else if (isHostAndPort(sensorVal)) {
            return transformHostAndPort(entity, machine, sensorVal.toString());
        } else {
            // no-op; unrecognised type
            return Maybe.absent();
        }
    }

    protected boolean isUri(Object sensorVal) {
        if (sensorVal instanceof URI || sensorVal instanceof URL) {
            return true;
        }
        try {
            URI uri = new URI(sensorVal.toString());
            return uri.getScheme() != null;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    protected boolean isPort(Object sensorVal) {
        if (sensorVal instanceof Integer || sensorVal instanceof Long) {
            return Networking.isPortValid(((Number)sensorVal).intValue());
        } else if (sensorVal instanceof CharSequence) {
            return sensorVal.toString().trim().matches("[0-9]+");
        } else {
            return false;
        }
    }

    protected int toInteger(Object sensorVal) {
        if (sensorVal instanceof Number) {
            return ((Number)sensorVal).intValue();
        } else if (sensorVal instanceof CharSequence) {
            return Integer.parseInt(sensorVal.toString().trim());
        } else {
            throw new IllegalArgumentException("Expected number but got "+sensorVal+" of type "+(sensorVal != null ? sensorVal.getClass() : null));
        }
    }

    protected boolean isHostAndPort(Object sensorVal) {
        if (sensorVal instanceof HostAndPort) {
            return true;
        } else if (sensorVal instanceof String) {
            try {
                HostAndPort hostAndPort = HostAndPort.fromString((String)sensorVal);
                return hostAndPort.hasPort();
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        return false;
    }

    protected Maybe<String> transformUri(Entity source, MachineLocation machine, String sensorVal) {
        URI uri = URI.create(sensorVal);
        int port = uri.getPort();
        if (port == -1 && "http".equalsIgnoreCase(uri.getScheme())) port = 80;
        if (port == -1 && "https".equalsIgnoreCase(uri.getScheme())) port = 443;

        if (port != -1) {
            Optional<HostAndPort> mappedEndpoint = getMappedEndpoint(source, machine, port);
            if (!mappedEndpoint.isPresent()) {
                LOG.trace("network-facing enricher not transforming {} URI {}, because no port-mapping for {}", new Object[] {source, sensorVal, machine});
                return Maybe.absent();
            }
            if (!mappedEndpoint.get().hasPort()) {
                LOG.debug("network-facing enricher not transforming {} URI {}, because no port in target {} for {}", new Object[] {source, sensorVal, mappedEndpoint, machine});
                return Maybe.absent();
            }
            URI result;
            try {
                result = new URI(uri.getScheme(), uri.getUserInfo(), mappedEndpoint.get().getHostText(), mappedEndpoint.get().getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
            } catch (URISyntaxException e) {
                LOG.debug("Error transforming URI "+uri+", using target "+mappedEndpoint+"; rethrowing");
                throw Exceptions.propagateAnnotated("Error transforming URI "+uri+", using target "+mappedEndpoint, e);
            }
            return Maybe.of(result.toString());
        } else {
            LOG.debug("sensor mapper not transforming URI "+uri+" because no port defined");
            return Maybe.absent();
        }
    }

    protected Maybe<String> transformHostAndPort(Entity source, MachineLocation machine, String sensorVal) {
        HostAndPort hostAndPort = HostAndPort.fromString(sensorVal);
        if (hostAndPort.hasPort()) {
            int port = hostAndPort.getPort();
            Optional<HostAndPort> mappedEndpoint = getMappedEndpoint(source, machine, port);
            if (!mappedEndpoint.isPresent()) {
                LOG.debug("network-facing enricher not transforming {} host-and-port {}, because no port-mapping for {}", new Object[] {source, sensorVal, machine});
                return Maybe.absent();
            }
            if (!mappedEndpoint.get().hasPort()) {
                LOG.debug("network-facing enricher not transforming {} host-and-port {}, because no port in target {} for {}", new Object[] {source, sensorVal, mappedEndpoint, machine});
                return Maybe.absent();
            }
            return Maybe.of(mappedEndpoint.get().toString());
        } else {
            LOG.debug("network-facing enricher not transforming {} host-and-port {} because defines no port", source, hostAndPort);
            return Maybe.absent();
        }
    }

    protected Maybe<String> transformPort(Entity source, MachineLocation machine, int sensorVal) {
        if (Networking.isPortValid(sensorVal)) {
            Optional<HostAndPort> mappedEndpoint = getMappedEndpoint(source, machine, sensorVal);
            if (!mappedEndpoint.isPresent()) {
                LOG.debug("network-facing enricher not transforming {} port {}, because no port-mapping for {}", new Object[] {source, sensorVal, machine});
                return Maybe.absent();
            }
            if (!mappedEndpoint.get().hasPort()) {
                LOG.debug("network-facing enricher not transforming {} port {}, because no port in target {} for {}", new Object[] {source, sensorVal, mappedEndpoint, machine});
                return Maybe.absent();
            }
            return Maybe.of(mappedEndpoint.get().toString());
        } else {
            LOG.debug("network-facing enricher not transforming {} port {} because not a valid port", source, sensorVal);
            return Maybe.absent();
        }
    }

    protected Maybe<MachineLocation> getMachine() {
        return Machines.findUniqueMachineLocation(entity.getLocations());
    }
    
    protected void checkConfig() {
        Collection<? extends AttributeSensor<?>> sensors = getConfig(SENSORS);
        Maybe<Object> rawMapMatching = config().getRaw(MAP_MATCHING);
        String mapMatching = config().get(MAP_MATCHING);
        
        if (sensors == null || sensors.isEmpty()) {
            if (Strings.isBlank(mapMatching)) {
                throw new IllegalStateException(this+" requires 'sensors' config (when 'mapMatching' is explicitly blank)");
            }
        } else if (rawMapMatching.isPresent()) {
            throw new IllegalStateException(this+" must not have explicit 'mapMatching' and 'sensors' config");
        }
    }
    
    protected Collection<AttributeSensor<?>> resolveSensorsConfig() {
        Collection<? extends AttributeSensor<?>> sensors = getConfig(SENSORS);

        Collection<AttributeSensor<?>> result = Lists.newArrayList();
        if (sensors != null) {
            for (Object s : sensors) {
                AttributeSensor<?> coercedSensor = TypeCoercions.coerce(s, AttributeSensor.class);
                AttributeSensor<?> typedSensor = (AttributeSensor<?>) entity.getEntityType().getSensor(coercedSensor.getName());
                result.add(typedSensor != null ? typedSensor : coercedSensor);
            }
        }
        return result;
    }
    
    protected Predicate<Sensor<?>> resolveMapMatchingConfig() {
        String regex = getConfig(MAP_MATCHING);
        final Predicate<CharSequence> namePredicate = StringPredicates.matchesRegex(regex);
        return new Predicate<Sensor<?>>() {
            @Override public boolean apply(Sensor<?> input) {
                return input != null && namePredicate.apply(input.getName());
            }
        };
    }
}
