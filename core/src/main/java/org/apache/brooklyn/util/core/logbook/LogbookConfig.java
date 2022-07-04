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
package org.apache.brooklyn.util.core.logbook;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

public class LogbookConfig {
    public final static String BASE_NAME_LOGBOOK = "brooklyn.logbook";

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_CLASSNAME = ConfigKeys.builder(String.class, BASE_NAME_LOGBOOK + ".logStore")
            .description("Log store implementation class name")
            .defaultValue("org.apache.brooklyn.util.core.logbook.file.FileLogStore")
            .build();
    public final static ConfigKey<LogStore> LOGBOOK_LOG_STORE_INSTANCE = ConfigKeys.builder(LogStore.class, LOGBOOK_LOG_STORE_CLASSNAME.getName() + ".internal.instance")
            .description("Instance of a pre-configured log store")
            .build();
    public final static ConfigKey<Integer> LOGBOOK_MAX_RECURSIVE_TASKS = ConfigKeys.builder(Integer.class, BASE_NAME_LOGBOOK + ".maxTasks")
            .description("Maximum number of recursive tasks")
            .defaultValue(100)
            .build();

}
