/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.core.json;

import org.apache.brooklyn.api.mgmt.ManagementContext;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class BrooklynObjectsJsonMapper {
    public static ObjectMapper newMapper(ManagementContext mgmt) {
        ConfigurableSerializerProvider sp = new ConfigurableSerializerProvider();
        sp.setUnknownTypeSerializer(new ErrorAndToStringUnknownTypeSerializer());

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializerProvider(sp);
        mapper.setVisibility(new PossiblyStrictPreferringFieldsVisibilityChecker());

        SimpleModule mapperModule = new SimpleModule("Brooklyn", new Version(0, 0, 0, "ignored", null, null));

        new BidiSerialization.ManagementContextSerialization(mgmt).install(mapperModule);
        new BidiSerialization.EntitySerialization(mgmt).install(mapperModule);
        new BidiSerialization.LocationSerialization(mgmt).install(mapperModule);
        new BidiSerialization.PolicySerialization(mgmt).install(mapperModule);
        new BidiSerialization.EnricherSerialization(mgmt).install(mapperModule);
        new BidiSerialization.FeedSerialization(mgmt).install(mapperModule);
        new BidiSerialization.TaskSerialization(mgmt).install(mapperModule);
        new BidiSerialization.ClassLoaderSerialization(mgmt).install(mapperModule);

        mapperModule.addSerializer(new MultimapSerializer());
        mapper.registerModule(mapperModule);
        return mapper;
    }
}
