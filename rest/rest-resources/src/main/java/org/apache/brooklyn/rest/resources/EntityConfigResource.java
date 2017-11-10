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
package org.apache.brooklyn.rest.resources;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.internal.EntityConfigMap;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.EntityAndItem;
import org.apache.brooklyn.rest.api.EntityConfigApi;
import org.apache.brooklyn.rest.domain.ConfigSummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.ConfigTransformer;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@HaHotStateRequired
public class EntityConfigResource extends AbstractBrooklynRestResource implements EntityConfigApi {

    private static final Logger LOG = LoggerFactory.getLogger(EntityConfigResource.class);

    @Override
    public List<ConfigSummary> list(final String application, final String entityToken) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }

        // TODO merge with keys which have values:
        //      ((EntityInternal) entity).config().getBag().getAllConfigAsConfigKeyMap();
        List<ConfigSummary> result = Lists.newArrayList();
        
        for (ConfigKey<?> key : entity.getEntityType().getConfigKeys()) {
            // Exclude config that user is not allowed to see
            if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CONFIG, new EntityAndItem<String>(entity, key.getName()))) {
                LOG.trace("User {} not authorized to see config {} of entity {}; excluding from ConfigKey list results", 
                        new Object[] {Entitlements.getEntitlementContext().user(), key.getName(), entity});
                continue;
            }
            result.add(ConfigTransformer.of(key).on(entity).includeLinks(ui.getBaseUriBuilder(), true, true).transform());
        }
        
        return result;
    }

    // TODO support parameters  ?show=value,summary&name=xxx &format={string,json,xml}
    // (and in sensors class)
    @Override
    public Map<String, Object> batchConfigRead(String application, String entityToken, Boolean raw) {
        // TODO: add test
        Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }

        // wrap in a task for better runtime view
        return Entities.submit(entity, Tasks.<Map<String,Object>>builder().displayName("REST API batch config read")
            .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
            .body(new BatchConfigRead(mgmt(), this, entity, raw)).build()).getUnchecked();
    }
    
    private static class BatchConfigRead implements Callable<Map<String,Object>> {
        private final ManagementContext mgmt;
        private final EntityConfigResource resource;
        private final Entity entity;
        private final Boolean raw;

        public BatchConfigRead(ManagementContext mgmt, EntityConfigResource resource, Entity entity, Boolean raw) {
            this.mgmt = mgmt;
            this.resource = resource;
            this.entity = entity;
            this.raw = raw;
        }

        @Override
        public Map<String, Object> call() throws Exception {
            // TODO on API for this, and other config (location, policy, etc), support requesting local v inherited and resolved v raw
            Map<ConfigKey<?>, Object> source = ( (EntityConfigMap)((EntityInternal) entity).config().getInternalConfigMap() ).getAllConfigInheritedRawValuesIgnoringErrors();
            Map<String, Object> result = Maps.newLinkedHashMap();
            for (Map.Entry<ConfigKey<?>, Object> ek : source.entrySet()) {
                ConfigKey<?> key = ek.getKey();
                Object value = ek.getValue();
                
                // Exclude config that user is not allowed to see
                if (!Entitlements.isEntitled(mgmt.getEntitlementManager(), Entitlements.SEE_CONFIG, new EntityAndItem<String>(entity, ek.getKey().getName()))) {
                    LOG.trace("User {} not authorized to see sensor {} of entity {}; excluding from current-state results", 
                            new Object[] {Entitlements.getEntitlementContext().user(), ek.getKey().getName(), entity});
                    continue;
                }
                result.put(key.getName(), 
                    resource.resolving(value, mgmt).preferJson(true).asJerseyOutermostReturnValue(false).raw(raw).context(entity).timeout(Duration.ZERO).renderAs(key).resolve()); 
            }
            return result;
        }
    }

    @Override
    public Object get(String application, String entityToken, String configKeyName, Boolean raw) {
        return get(true, application, entityToken, configKeyName, raw);
    }

    @Override
    public String getPlain(String application, String entityToken, String configKeyName, Boolean raw) {
        return Strings.toString(get(false, application, entityToken, configKeyName, raw));
    }

    public Object get(boolean preferJson, String application, String entityToken, String configKeyName, Boolean raw) {
        Entity entity = brooklyn().getEntity(application, entityToken);
        ConfigKey<?> ck = findConfig(entity, configKeyName);
        
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CONFIG, new EntityAndItem<String>(entity, ck.getName()))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s' config '%s'",
                    Entitlements.getEntitlementContext().user(), entity, ck.getName());
        }
        
        Object value = ((EntityInternal)entity).config().getRaw(ck).orNull();
        return resolving(value).preferJson(preferJson).asJerseyOutermostReturnValue(true).raw(raw).context(entity).immediately(true).renderAs(ck).resolve();
    }

    private ConfigKey<?> findConfig(Entity entity, String configKeyName) {
        ConfigKey<?> ck = entity.getEntityType().getConfigKey(configKeyName);
        if (ck == null)
            ck = new BasicConfigKey<Object>(Object.class, configKeyName);
        return ck;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void setFromMap(String application, String entityToken, Boolean recurse, Map newValues) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }

        if (LOG.isDebugEnabled())
            LOG.debug("REST user " + Entitlements.getEntitlementContext() + " setting configs " + newValues);
        for (Object entry : newValues.entrySet()) {
            String configName = Strings.toString(((Map.Entry) entry).getKey());
            Object newValue = ((Map.Entry) entry).getValue();

            ConfigKey ck = findConfig(entity, configName);
            ((EntityInternal) entity).config().set(ck, TypeCoercions.coerce(newValue, ck.getTypeToken()));
            if (Boolean.TRUE.equals(recurse)) {
                for (Entity e2 : Entities.descendantsWithoutSelf(entity)) {
                    ((EntityInternal) e2).config().set(ck, newValue);
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void set(String application, String entityToken, String configName, Boolean recurse, Object newValue) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }

        ConfigKey ck = findConfig(entity, configName);
        LOG.debug("REST setting config " + configName + " on " + entity + " to " + newValue);
        ((EntityInternal) entity).config().set(ck, TypeCoercions.coerce(newValue, ck.getTypeToken()));
        if (Boolean.TRUE.equals(recurse)) {
            for (Entity e2 : Entities.descendantsWithoutSelf(entity)) {
                ((EntityInternal) e2).config().set(ck, newValue);
            }
        }
    }
}
