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
package org.apache.brooklyn.policy.location;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

/**
 * Policy that is attached to an entity to create a location configured
 * using its sensor data. The created location is added to the catalog
 * and will be removed when the entity is no longer running, based on
 * the entity {@link Startable#SERVICE_UP} sensor status.
 * <p>
 * The policy definition includes configuration for the id, name and
 * type of the location, as well as the {@link #LOCATION_CONFIG} map
 * of config keys and the sensors used to define them.
 * <p>
 * The YAML below shows a policy that creates a {@code jclouds:aws-ec2}
 * location with the region defined by a sensor on the entity it is
 * attached to:
 * <pre>{@code
 * name: My App
 * brooklyn.policies:
 *   - type: org.apache.brooklyn.policy.location.CreateLocationPolicy
 *     id: create-my-cloud-location
 *     brooklyn.config:
 *       location.catalogId: my-cloud
 *       location.displayName: "My Cloud"
 *       location.type: jclouds:aws-ec2
 *       location.config:
 *         region: $brooklyn:sensor("aws.region")
 * }</pre>
 *
 */
@Beta
public class CreateLocationPolicy extends AbstractPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(CreateLocationPolicy.class);

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String name;
        private AttributeSensor<Boolean> status;
        private Map<String,AttributeSensor<?>> configuration;
        private String catalogId;
        private String displayName;
        private String type;
        private Set<String> tags;

        public Builder id(String val) {
            this.id = val; return this;
        }
        public Builder name(String val) {
            this.name = val; return this;
        }
        public Builder status(AttributeSensor<Boolean> val) {
            this.status = val; return this;
        }
        public Builder configuration(Map<String,AttributeSensor<?>> val) {
            this.configuration = val; return this;
        }
        public Builder catalogId(String val) {
            this.catalogId = val; return this;
        }
        public Builder displayName(String val) {
            this.displayName = val; return this;
        }
        public Builder type(String val) {
            this.type = val; return this;
        }
        public Builder tags(Set<String> val) {
            this.tags = val; return this;
        }
        public CreateLocationPolicy build() {
            return new CreateLocationPolicy(toFlags());
        }
        public PolicySpec<CreateLocationPolicy> buildSpec() {
            return PolicySpec.create(CreateLocationPolicy.class)
                    .configure(toFlags());
        }
        private Map<String,?> toFlags() {
            return MutableMap.<String,Object>builder()
                    .putIfNotNull("id", id)
                    .putIfNotNull("name", name)
                    .putIfNotNull("location.status", status)
                    .putIfNotNull("location.config", configuration)
                    .putIfNotNull("location.catalogId", catalogId)
                    .putIfNotNull("location.displayName", displayName)
                    .putIfNotNull("location.type", type)
                    .putIfNotNull("location.tags", tags)
                    .build();
        }
    }

    @SuppressWarnings("serial")
    public static final ConfigKey<AttributeSensor<Boolean>> LOCATION_STATUS = ConfigKeys.builder(new TypeToken<AttributeSensor<Boolean>>() {})
            .name("location.status")
            .description("Sensor on the entity to trigger location creation; defaults to service.isUp")
            .defaultValue(Startable.SERVICE_UP)
            .build();

    @SuppressWarnings("serial")
    public static final ConfigKey<Map<String,AttributeSensor<?>>> LOCATION_CONFIG = ConfigKeys.builder(new TypeToken<Map<String,AttributeSensor<?>>>() {})
            .name("location.config")
            .description("Map of location configuration keys to sensors on the entity that provide their values")
            .defaultValue(ImmutableMap.of())
            .build();

    public static final ConfigKey<String> LOCATION_CATALOG_ID = ConfigKeys.builder(String.class)
            .name("location.catalogId")
            .description("The catalog item ID to use for the created location")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<String> LOCATION_DISPLAY_NAME = ConfigKeys.builder(String.class)
            .name("location.displayName")
            .description("The display name for the created location")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<String> LOCATION_TYPE = ConfigKeys.builder(String.class)
            .name("location.type")
            .description("The type of location to create, i.e. 'jclouds:aws-ec2'")
            .constraint(Predicates.notNull())
            .build();

    @SuppressWarnings("serial")
    public static final ConfigKey<Set<String>> LOCATION_TAGS = ConfigKeys.builder(new TypeToken<Set<String>>() {})
            .name("location.tags")
            .description("Tags for the created location")
            .defaultValue(ImmutableSet.of())
            .build();

    private AtomicBoolean created = new AtomicBoolean(false);
    private Object mutex = new Object[0];
    private CatalogItem<?,?> catalogItem;
    private AtomicInteger version = new AtomicInteger(0);

    public CreateLocationPolicy() {
        this(MutableMap.<String,Object>of());
    }

    public CreateLocationPolicy(Map<String,?> props) {
        super(props);
    }

    protected AttributeSensor<Boolean> getStatusSensor() {
        return config().get(LOCATION_STATUS);
    }

    protected Map<String,AttributeSensor<?>> getLocationConfiguration() {
        return config().get(LOCATION_CONFIG);
    }

    protected String getLocationCatalogItemId() {
        return config().get(LOCATION_CATALOG_ID);
    }

    protected String getLocationDisplayName() {
        return config().get(LOCATION_DISPLAY_NAME);
    }

    protected String getLocationType() {
        return config().get(LOCATION_TYPE);
    }

    protected Set<String> getLocationTags() {
        return config().get(LOCATION_TAGS);
    }

    @Override
    protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
        throw new UnsupportedOperationException("reconfiguring "+key+" unsupported for "+this);
    }

    private final SensorEventListener<Boolean> lifecycleEventHandler = new SensorEventListener<Boolean>() {
        @Override
        public void onEvent(SensorEvent<Boolean> event) {
            synchronized (mutex) {
                Boolean status = event.getValue();
                if (status) {
                    if (created.compareAndSet(false, true)) {
                        LOG.info("Creating new location {} of type {}", getLocationCatalogItemId(), getLocationType());

                        ImmutableList.Builder<String> builder = ImmutableList.<String>builder().add(
                                "brooklyn.catalog:",
                                "  id: " + getLocationCatalogItemId(),
                                "  version: " + version.incrementAndGet(),
                                "  itemType: location",
                                "  item:",
                                "    type: " + getLocationType(),
                                "    brooklyn.config:",
                                "      displayName: " + getLocationDisplayName());

                        for (Map.Entry<String, AttributeSensor<?>> entry : getLocationConfiguration().entrySet()) {
                            AttributeSensor<?> sensor = getLocationConfiguration().get(entry.getKey());
                            Object value = entity.sensors().get(sensor);
                            builder.add("      " + entry.getKey() + ": " + value);
                        }
                        if (getLocationTags().size() > 0) {
                            builder.add("      tags:");
                            for (String tag : getLocationTags()) {
                                builder.add("        - " + tag);
                            }
                        }
                        String blueprint = Joiner.on("\n").join(builder.build());
                        LOG.debug("Creating location {} from YAML\n{}", getLocationCatalogItemId(), blueprint);

                        Iterable<? extends CatalogItem<?,?>> catalogItems = getManagementContext().getCatalog().addItems(blueprint, true);
                        int items = Iterables.size(catalogItems);
                        if (items > 1) {
                            LOG.warn("Got {} catalog items for location {}, using first: {}",
                                    new Object[] { items, getLocationCatalogItemId(), Iterables.transform(catalogItems, item -> item.getCatalogItemId()) });
                        } else if (items == 0) {
                            throw new IllegalStateException("No catalog items returned for location: " + getLocationCatalogItemId());
                        }
                        catalogItem = Iterables.get(catalogItems, 0);
                    }
                } else {
                    if (created.compareAndSet(true, false) && catalogItem != null) {
                        String symbolicName = catalogItem.getSymbolicName();
                        String catalogVersion = catalogItem.getVersion();
                        try {
                            LOG.info("Deleting location id {}: {}:{}",
                                    new Object[] { catalogItem.getId(), symbolicName, catalogVersion });
                            getManagementContext().getCatalog().deleteCatalogItem(symbolicName, catalogVersion);
                        } catch (NoSuchElementException nsee) {
                            LOG.warn("Could not find location {}:{} in catalog to delete", symbolicName, catalogVersion);
                        }
                        catalogItem = null;
                    }
                }
            }
        }
    };

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);
        subscriptions().subscribe(entity, getStatusSensor(), lifecycleEventHandler);
    }
}