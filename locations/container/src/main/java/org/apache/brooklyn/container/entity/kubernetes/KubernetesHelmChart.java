/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.container.entity.kubernetes;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.util.core.ResourcePredicates;

@ImplementedBy(KubernetesHelmChartImpl.class)
public interface KubernetesHelmChart extends SoftwareProcess {

   ConfigKey<String> CHART_NAME = ConfigKeys.builder(String.class)
           .name("chartName")
           .description("Helm Chart name")
           .build();

   ConfigKey<String> CHART_VERSION = ConfigKeys.builder(String.class)
           .name("chartVersion")
           .description("Helm Chart version")
           .build();
}
