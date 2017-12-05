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
package org.apache.brooklyn.enricher.stock.aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.sensor.BasicAttributeSensorAndConfigKey;

public final class AggregationJob implements Runnable {

    public static BasicAttributeSensorAndConfigKey<Map<String, String>> DASHBOARD_COST_PER_MONTH = new BasicAttributeSensorAndConfigKey(Map.class, "dashboard.costPerMonth", "A map of VM ID to monthly cost. Please note if the same VM ID is reported more than once, only one of the sensors will be propagated");
    public static BasicAttributeSensorAndConfigKey<List<Map<String, String>>> DASHBOARD_HA_PRIMARIES = new BasicAttributeSensorAndConfigKey(List.class, "dashboard.ha.primaries", "A list of high availability primaries "); //TODO: find out what this is and add a better description
    public static BasicAttributeSensorAndConfigKey<List<Map<String, String>>> DASHBOARD_LICENSES = new BasicAttributeSensorAndConfigKey(List.class, "dashboard.licenses", "The licences in use for the running entities. This is a list of maps. The map should contain two keys: product and key");
    public static BasicAttributeSensorAndConfigKey<Map<String, List<Map<String, Object>>>> DASHBOARD_LOCATIONS = new BasicAttributeSensorAndConfigKey(Map.class, "dashboard.locations", "Locations in which the VMs are running. A map where the key is category of location (e.g. server) and the value is a list of maps containing name/icon/count");
    public static BasicAttributeSensorAndConfigKey<List<Map<String, String>>> DASHBOARD_POLICY_HIGHLIGHTS = new BasicAttributeSensorAndConfigKey(List.class, "dashboard.policyHighlights", "Highlights from policies. List of Masps, where each map should contain text and category");

    private final Entity entity;

    private static final String NAME_STRING = "name";
    private static final String COUNT_STRING = "count";

    public AggregationJob(Entity entity) {
        this.entity = entity;
    }

    private void updateFromConfig(Entity entity, BasicAttributeSensorAndConfigKey<List<Map<String, String>>> sensorAndConfigKey, List<Map<String, String>> list) {
        List<Map<String, String>> listFromEntity = entity.sensors().get(sensorAndConfigKey);
        if (listFromEntity == null || listFromEntity.isEmpty()) {
            listFromEntity = entity.config().get(sensorAndConfigKey);
        }

        if (listFromEntity != null) {
            list.addAll(listFromEntity);
        }
    }

    @Override
    public void run() {
        HashMap<String, String> costPerMonth = new HashMap<>();
        List<Map<String, String>> haPrimaries = new ArrayList<>();
        List<Map<String, String>> dashboardLicences = new ArrayList<>();
        Map<String, List<Map<String, Object>>> dashboardLocations = new HashMap<>();
        List<Map<String, String>> dashboardPolicyHighlights = new ArrayList<>();

        entity.getChildren().forEach(childEntity -> scanEntity(childEntity, costPerMonth, haPrimaries, dashboardLicences, dashboardLocations, dashboardPolicyHighlights));
        entity.sensors().set(DASHBOARD_COST_PER_MONTH, costPerMonth);
        entity.sensors().set(DASHBOARD_HA_PRIMARIES, haPrimaries);
        entity.sensors().set(DASHBOARD_LICENSES, dashboardLicences);
        entity.sensors().set(DASHBOARD_LOCATIONS, dashboardLocations);
        entity.sensors().set(DASHBOARD_POLICY_HIGHLIGHTS, dashboardPolicyHighlights);
    }

    protected void scanEntity(Entity entity,
                              Map<String, String> costPerMonth,
                              List<Map<String, String>> haPrimaries,
                              List<Map<String, String>> licences,
                              Map<String, List<Map<String, Object>>> locations,
                              List<Map<String, String>> policyHighlights) {

        updateFromConfig(entity, DASHBOARD_HA_PRIMARIES, haPrimaries);
        updateFromConfig(entity, DASHBOARD_LICENSES, licences);
        updateFromConfig(entity, DASHBOARD_POLICY_HIGHLIGHTS, policyHighlights);

        //Cost per month
        Map<String, String> costPerMonthFromEntity = entity.sensors().get(DASHBOARD_COST_PER_MONTH);
        if (costPerMonthFromEntity == null || costPerMonthFromEntity.isEmpty()) {
            costPerMonthFromEntity = entity.config().get(DASHBOARD_COST_PER_MONTH);
        }

        if (costPerMonthFromEntity != null) {
            costPerMonth.putAll(costPerMonthFromEntity);
        }

        //Locations merge
        //We are merging a Map, of Lists of Maps
        //The outer map is location type, e.g. servers to a list of data for that type
        //The inner map should contain the data e.g. name/icon/count.

        //Further complicating this is that the Map you get back from sensors/config doesn't conform to the generic expectations.
        //I.E. even if you type this as Map<String, List<Map<String, String>>> you will still get back an integer from
        //the inner map- hence all the weird casting bellow.
        Map<String, List<Map<String, Object>>> outerLocationMapFromEntity = entity.sensors().get(DASHBOARD_LOCATIONS);
        if (outerLocationMapFromEntity == null || outerLocationMapFromEntity.isEmpty()) {
            outerLocationMapFromEntity = entity.config().get(DASHBOARD_LOCATIONS);
        }

        if (outerLocationMapFromEntity != null) {
            //loop through outer maps
            outerLocationMapFromEntity.forEach((outerLocationMapFromEntityKey, outerLocationMapFromEntityValue) -> {
                boolean found = false;
                for (Map.Entry<String, List<Map<String, Object>>> outerLocationMapFromMethodParam : locations.entrySet()) {
                    if (StringUtils.equals(outerLocationMapFromMethodParam.getKey(), outerLocationMapFromEntityKey)) {
                        found = true;

                        //loop through list
                        Iterator<Map<String, Object>> listIteratorFromEntity = outerLocationMapFromEntityValue.iterator();
                        while (listIteratorFromEntity.hasNext()) {
                            Map<String, Object> innerMapFromEntity = listIteratorFromEntity.next();
                            boolean foundInner = false;

                            //loop through inner map and merge
                            for (Map<String, Object> innerMapFromMethodParam : outerLocationMapFromMethodParam.getValue()) {
                                if (StringUtils.equals((String) innerMapFromEntity.get(NAME_STRING), (String) innerMapFromMethodParam.get(NAME_STRING))) {

                                    innerMapFromMethodParam.put(COUNT_STRING, (int) innerMapFromEntity.get(COUNT_STRING) + (int) innerMapFromMethodParam.get(COUNT_STRING));
                                    foundInner = true;
                                    break;
                                }
                            }

                            //If the entity has a "name" not found in the method param then add it to the method param
                            if (!foundInner) {
                                outerLocationMapFromMethodParam.getValue().add(new HashMap<>(innerMapFromEntity));
                            }
                        }
                    }

                }

                //If the entity has an entry in the outer map that isn't in the method param, then add it
                if (!found) {
                    ArrayList clonedList = new ArrayList();
                    outerLocationMapFromEntityValue.forEach(mapToClone -> {
                        clonedList.add(new HashMap<>(mapToClone));

                    });
                    locations.put(outerLocationMapFromEntityKey, clonedList);
                }

            });
        }

        entity.getChildren().forEach(childEntity -> scanEntity(childEntity, costPerMonth, haPrimaries, licences, locations, policyHighlights));
    }
}
