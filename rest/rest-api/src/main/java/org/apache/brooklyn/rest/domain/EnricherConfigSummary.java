/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.rest.domain;

import java.net.URI;
import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;

/** @deprecated since 0.13.0 no different to ConfigSummary, use that */
@Deprecated
public class EnricherConfigSummary extends ConfigSummary {

    private static final long serialVersionUID = 4339330833863794513L;

    @SuppressWarnings("unused") // json deserialization
    private EnricherConfigSummary() {}
    
    public EnricherConfigSummary(ConfigKey<?> config, String label, Double priority, Map<String, URI> links) {
        super(config, label, priority, null, links);
    }

}
