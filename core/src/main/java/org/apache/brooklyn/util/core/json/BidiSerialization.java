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
package org.apache.brooklyn.util.core.json;

import java.io.IOException;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Optional;

public class BidiSerialization {

    protected final static ThreadLocal<Boolean> STRICT_SERIALIZATION = new ThreadLocal<Boolean>(); 

    /**
     * Sets strict serialization on, or off (the default), for the current thread.
     * Recommended to be used in a <code>try { ... } finally { ... }</code> block
     * with {@link #clearStrictSerialization()} at the end.
     * <p>
     * With strict serialization, classes must have public fields or annotated fields, else they will not be serialized.
     */
    public static void setStrictSerialization(Boolean value) {
        STRICT_SERIALIZATION.set(value);
    }

    public static void clearStrictSerialization() {
        STRICT_SERIALIZATION.remove();
    }

    public static boolean isStrictSerialization() {
        Boolean result = STRICT_SERIALIZATION.get();
        if (result!=null) return result;
        return false;
    }


    public abstract static class AbstractWithManagementContextSerialization<T> {

        protected class Serializer extends JsonSerializer<T> {
            @Override
            public void serialize(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
                AbstractWithManagementContextSerialization.this.serialize(value, jgen, provider);
            }
        }
        
        protected class Deserializer extends JsonDeserializer<T> {
            @Override
            public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
                return AbstractWithManagementContextSerialization.this.deserialize(jp, ctxt);
            }
        }
        
        protected final Serializer serializer = new Serializer();
        protected final Deserializer deserializer = new Deserializer();
        protected final Class<T> type;
        protected final ManagementContext mgmt;
        
        public AbstractWithManagementContextSerialization(Class<T> type, ManagementContext mgmt) {
            this.type = type;
            this.mgmt = mgmt;
        }
        
        public JsonSerializer<T> getSerializer() {
            return serializer;
        }
        
        public JsonDeserializer<T> getDeserializer() {
            return deserializer;
        }

        public void serialize(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            writeBody(value, jgen, provider);
            jgen.writeEndObject();
        }

        protected void writeBody(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStringField("type", customType(value));
            customWriteBody(value, jgen, provider);
        }

        protected String customType(T value) throws IOException {
            return value.getClass().getCanonicalName();
        }

        public abstract void customWriteBody(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException;

        public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            @SuppressWarnings("unchecked")
            Map<Object,Object> values = jp.readValueAs(Map.class);
            String type = (String) values.get("type");
            return customReadBody(type, values, jp, ctxt);
        }

        protected abstract T customReadBody(String type, Map<Object, Object> values, JsonParser jp, DeserializationContext ctxt) throws IOException;

        public void install(SimpleModule module) {
            module.addSerializer(type, serializer);
            module.addDeserializer(type, deserializer);
        }
    }
    
    public static class ManagementContextSerialization extends AbstractWithManagementContextSerialization<ManagementContext> {
        public ManagementContextSerialization(ManagementContext mgmt) { super(ManagementContext.class, mgmt); }
        @Override
        protected String customType(ManagementContext value) throws IOException {
            return type.getCanonicalName();
        }
        @Override
        public void customWriteBody(ManagementContext value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        }
        @Override
        protected ManagementContext customReadBody(String type, Map<Object, Object> values, JsonParser jp, DeserializationContext ctxt) throws IOException {
            return mgmt;
        }
    }
    
    public abstract static class AbstractBrooklynObjectSerialization<T extends BrooklynObject> extends AbstractWithManagementContextSerialization<T> {
        public AbstractBrooklynObjectSerialization(Class<T> type, ManagementContext mgmt) { 
            super(type, mgmt);
        }
        @Override
        protected String customType(T value) throws IOException {
            return type.getCanonicalName();
        }
        @Override
        public void customWriteBody(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStringField("id", value.getId());
        }
        @Override
        protected T customReadBody(String type, Map<Object, Object> values, JsonParser jp, DeserializationContext ctxt) throws IOException {
            return getInstanceFromId((String) values.get("id"));
        }
        protected abstract T getInstanceFromId(String id);
    }

    public abstract static class AbstractBrooklynAdjunctSerialization<T extends BrooklynObject & EntityAdjunct> extends AbstractWithManagementContextSerialization<T> {
        public AbstractBrooklynAdjunctSerialization(Class<T> type, ManagementContext mgmt) { 
            super(type, mgmt);
        }
        @Override
        protected String customType(T value) throws IOException {
            return type.getCanonicalName();
        }
        @Override
        public void customWriteBody(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            Optional<String> entityId = getEntityId(value);
            jgen.writeStringField("id", value.getId());
            if (entityId.isPresent()) jgen.writeStringField("entityId", entityId.get());
        }
        @Override
        protected T customReadBody(String type, Map<Object, Object> values, JsonParser jp, DeserializationContext ctxt) throws IOException {
            Optional<String> entityId = Optional.fromNullable((String) values.get("entityId"));
            Optional<Entity> entity = Optional.fromNullable(entityId.isPresent() ? null: mgmt.getEntityManager().getEntity(entityId.get()));
            String id = (String) values.get("id");
            return getInstanceFromId(entity, id);
        }
        protected Optional<String> getEntityId(T value) {
            if (value instanceof AbstractEntityAdjunct) {
                Entity entity = ((AbstractEntityAdjunct)value).getEntity();
                return Optional.fromNullable(entity == null ? null : entity.getId());
            }
            return Optional.absent();
        }
        protected abstract T getInstanceFromId(Optional<Entity> entity, String id);
    }

    public static class EntitySerialization extends AbstractBrooklynObjectSerialization<Entity> {
        public EntitySerialization(ManagementContext mgmt) { super(Entity.class, mgmt); }
        @Override protected Entity getInstanceFromId(String id) { return mgmt.getEntityManager().getEntity(id); }
    }
    
    public static class LocationSerialization extends AbstractBrooklynObjectSerialization<Location> {
        public LocationSerialization(ManagementContext mgmt) { super(Location.class, mgmt); }
        @Override protected Location getInstanceFromId(String id) { return mgmt.getLocationManager().getLocation(id); }
    }
    
    public static class PolicySerialization extends AbstractBrooklynAdjunctSerialization<Policy> {
        public PolicySerialization(ManagementContext mgmt) {
            super(Policy.class, mgmt);
        }
        @Override protected Policy getInstanceFromId(Optional<Entity> entity, String id) {
            if (id == null || !entity.isPresent()) return null;
            for (Policy policy : entity.get().policies()) {
                if (id.equals(policy.getId())) {
                    return policy;
                }
            }
            return null;
        }
    }
    
    public static class EnricherSerialization extends AbstractBrooklynAdjunctSerialization<Enricher> {
        public EnricherSerialization(ManagementContext mgmt) {
            super(Enricher.class, mgmt);
        }
        @Override protected Enricher getInstanceFromId(Optional<Entity> entity, String id) {
            if (id == null || !entity.isPresent()) return null;
            for (Enricher enricher : entity.get().enrichers()) {
                if (id.equals(enricher.getId())) {
                    return enricher;
                }
            }
            return null;
        }
    }
    
    public static class FeedSerialization extends AbstractBrooklynAdjunctSerialization<Feed> {
        public FeedSerialization(ManagementContext mgmt) {
            super(Feed.class, mgmt);
        }
        @Override protected Feed getInstanceFromId(Optional<Entity> entity, String id) {
            if (id == null || !entity.isPresent()) return null;
            for (Feed feed : ((EntityInternal)entity).feeds().getFeeds()) {
                if (id.equals(feed.getId())) {
                    return feed;
                }
            }
            return null;
        }
    }
    
    public static class TaskSerialization extends AbstractWithManagementContextSerialization<Task<?>> {
        @SuppressWarnings("unchecked")
        public TaskSerialization(ManagementContext mgmt) { 
            super((Class<Task<?>>)(Class<?>)Task.class, mgmt);
        }
        @Override
        protected String customType(Task<?> value) throws IOException {
            return type.getCanonicalName();
        }
        @Override
        public void customWriteBody(Task<?> value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStringField("id", value.getId());
            jgen.writeStringField("displayName", value.getDisplayName());
        }
        @Override
        protected Task<?> customReadBody(String type, Map<Object, Object> values, JsonParser jp, DeserializationContext ctxt) throws IOException {
            return mgmt.getExecutionManager().getTask((String) values.get("id"));
        }
    }
}
