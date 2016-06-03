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
package org.apache.brooklyn.util.collections;

import java.util.Map;

public class CollectionHelpers {
    public static Map<String, Object> mergeMapsAndTheirInnerMapValues(Map<String, Object> defaultEntries, Map<String, Object> overrideWith) {
        return mergeMapsAndTheirInnerMapValues(defaultEntries, overrideWith, null);
    }

    /**
     * Does a shallow merge of two maps.
     */
    public static Map<String, Object> mergeMapsAndTheirInnerMapValues(Map<String, Object> defaultEntries, Map<String, Object> overrideWith, String keyToWhichShallowMergeIsRestricted) {
        Map<String, Object> result = MutableMap.of();
        result.putAll(defaultEntries);
        for (Map.Entry<String, Object> newEntry: overrideWith.entrySet()) {
            if (!defaultEntries.containsKey(newEntry.getKey())) {
                result.put(newEntry.getKey(), newEntry.getValue());
            } else if (newEntry.getValue() instanceof Map) {
                Map<Object, Object> mergedValue = MutableMap.of();
                if (defaultEntries.get(newEntry.getKey()) instanceof Map) {
                    if (keyToWhichShallowMergeIsRestricted == null || keyToWhichShallowMergeIsRestricted.equals(newEntry.getKey())) {
                        mergedValue.putAll((Map) defaultEntries.get(newEntry.getKey()));
                    }
                }
                mergedValue.putAll((Map<Object,Object>) newEntry.getValue());
                result.put(newEntry.getKey(), mergedValue);
            } else {
                result.put(newEntry.getKey(), newEntry.getValue());
            }
        }
        return result;
    }
}
