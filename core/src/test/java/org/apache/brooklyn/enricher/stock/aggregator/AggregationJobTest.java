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

import static org.apache.brooklyn.enricher.stock.aggregator.AggregationJob.DASHBOARD_COST_PER_MONTH;
import static org.apache.brooklyn.enricher.stock.aggregator.AggregationJob.DASHBOARD_HA_PRIMARIES;
import static org.apache.brooklyn.enricher.stock.aggregator.AggregationJob.DASHBOARD_LICENSES;
import static org.apache.brooklyn.enricher.stock.aggregator.AggregationJob.DASHBOARD_LOCATIONS;
import static org.apache.brooklyn.enricher.stock.aggregator.AggregationJob.DASHBOARD_POLICY_HIGHLIGHTS;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;

public class AggregationJobTest {

    TestApplication testApplication;
    TestEntity parentEntity;
    TestEntity childEntityOne;
    TestEntity childEntityTwo;

    AggregationJob aggregationJob;

    LocalManagementContext localManagementContext = LocalManagementContextForTests.newInstance();

    @BeforeMethod
    public void testSetup() {
        testApplication = TestApplication.Factory.newManagedInstanceForTests(localManagementContext);

        parentEntity = testApplication.createAndManageChild(EntitySpec.create(TestEntity.class));
        childEntityOne = parentEntity.createAndManageChild(EntitySpec.create(TestEntity.class));
        childEntityTwo = parentEntity.createAndManageChild(EntitySpec.create(TestEntity.class));

        aggregationJob = new AggregationJob(parentEntity);
    }

    @Test
    public void policyHighlightTest() {
        //Setup
        Map<String, String> policyHighlight = new HashMap<>();
        policyHighlight.put("text", "TEST TEXT");
        policyHighlight.put("category", "lastAction");

        ArrayList<Map<String, String>> policyHighlights = new ArrayList<>();
        policyHighlights.add(policyHighlight);

        childEntityOne.sensors().set(DASHBOARD_POLICY_HIGHLIGHTS, policyHighlights);

        //Test
        aggregationJob.run();

        //Verification
        List<Map<String, String>> policyHighlightsFromParent = parentEntity.sensors().get(DASHBOARD_POLICY_HIGHLIGHTS);
        assertEquals(1, policyHighlightsFromParent.size());
        Map<String, String> policyHighlightFromParent = policyHighlightsFromParent.get(0);
        assertEquals(policyHighlightFromParent.get("text"), "TEST TEXT");
        assertEquals(policyHighlightFromParent.get("category"), "lastAction");
    }

    @Test
    public void policyHighlightTestMultipleChildren() {
        //Setup
        Map<String, String> policyHighlightOne = new HashMap<>();
        policyHighlightOne.put("text", "TEST TEXT ONE");
        policyHighlightOne.put("category", "lastAction");

        ArrayList<Map<String, String>> policyHighlightsOne = new ArrayList<>();
        policyHighlightsOne.add(policyHighlightOne);

        childEntityOne.sensors().set(DASHBOARD_POLICY_HIGHLIGHTS, policyHighlightsOne);

        Map<String, String> policyHighlightTwo = new HashMap<>();
        policyHighlightTwo.put("text", "TEST TEXT TWO");
        policyHighlightTwo.put("category", "lastAction");

        ArrayList<Map<String, String>> policyHighlightsTwo = new ArrayList<>();
        policyHighlightsTwo.add(policyHighlightTwo);

        childEntityOne.sensors().set(DASHBOARD_POLICY_HIGHLIGHTS, policyHighlightsOne);
        childEntityTwo.sensors().set(DASHBOARD_POLICY_HIGHLIGHTS, policyHighlightsTwo);

        //Test
        aggregationJob.run();

        //Verification
        List<Map<String, String>> policyHighlightsFromParent = parentEntity.sensors().get(DASHBOARD_POLICY_HIGHLIGHTS);
        assertEquals(2, policyHighlightsFromParent.size());
        Map<String, String> policyHighlightFromParentOne = policyHighlightsFromParent.get(0);
        assertEquals(policyHighlightFromParentOne.get("text"), "TEST TEXT ONE");
        assertEquals(policyHighlightFromParentOne.get("category"), "lastAction");

        Map<String, String> policyHighlightFromParentTwo = policyHighlightsFromParent.get(1);
        assertEquals(policyHighlightFromParentTwo.get("text"), "TEST TEXT TWO");
        assertEquals(policyHighlightFromParentTwo.get("category"), "lastAction");
    }

    @Test
    public void costPerMonthTest() {
        //Setup
        Map<String, String> costPerMonth = new HashMap<>();
        costPerMonth.put("VM1", "200 USD");

        childEntityOne.sensors().set(DASHBOARD_COST_PER_MONTH, costPerMonth);

        //Test
        aggregationJob.run();

        //Verification
        Map<String, String> costPerMonthFromParent = parentEntity.sensors().get(DASHBOARD_COST_PER_MONTH);
        assertEquals(costPerMonthFromParent.get("VM1"), "200 USD");
    }

    @Test
    public void costPerMonthMultipleChildrenTest() {
        //Setup
        Map<String, String> costPerMonthOne = new HashMap<>();
        costPerMonthOne.put("VM1", "200 USD");

        Map<String, String> costPerMonthTwo = new HashMap<>();
        costPerMonthOne.put("VM2", "300 USD");

        childEntityOne.sensors().set(DASHBOARD_COST_PER_MONTH, costPerMonthOne);
        childEntityTwo.sensors().set(DASHBOARD_COST_PER_MONTH, costPerMonthTwo);

        //Test
        aggregationJob.run();

        //Verification
        Map<String, String> costPerMonthFromParent = parentEntity.sensors().get(DASHBOARD_COST_PER_MONTH);
        assertEquals(costPerMonthFromParent.get("VM1"), "200 USD");
        assertEquals(costPerMonthFromParent.get("VM2"), "300 USD");
    }

    //Our implementation is quite basic. If the same VM id shows up twice we overwrite
    @Test
    public void costPerMonthMultipleChildrenOverwriteTest() {
        //Setup
        Map<String, String> costPerMonthOne = new HashMap<>();
        costPerMonthOne.put("VM1", "200 USD");

        Map<String, String> costPerMonthTwo = new HashMap<>();
        costPerMonthOne.put("VM1", "300 USD");

        childEntityOne.sensors().set(DASHBOARD_COST_PER_MONTH, costPerMonthOne);
        childEntityTwo.sensors().set(DASHBOARD_COST_PER_MONTH, costPerMonthTwo);

        //Test
        aggregationJob.run();

        //Verification
        Map<String, String> costPerMonthFromParent = parentEntity.sensors().get(DASHBOARD_COST_PER_MONTH);
        assertEquals(1, costPerMonthFromParent.size());
        assertEquals(costPerMonthFromParent.get("VM1"), "300 USD");
    }

    @Test
    public void haPrimariesTest() {
        //Setup
        Map<String, String> haPrimary = new HashMap<>();
        haPrimary.put("parent", "Regions");
        haPrimary.put("primary", "US East");

        ArrayList<Map<String, String>> haPrimaries = new ArrayList<>();
        haPrimaries.add(haPrimary);

        childEntityOne.sensors().set(DASHBOARD_HA_PRIMARIES, haPrimaries);

        //Test
        aggregationJob.run();

        //Verification
        List<Map<String, String>> haPrimariesFromParent = parentEntity.sensors().get(DASHBOARD_HA_PRIMARIES);
        System.out.println(haPrimariesFromParent);
        assertEquals(1, haPrimariesFromParent.size());
        Map<String, String> haPrimaryFromParent = haPrimariesFromParent.get(0);
        assertEquals(haPrimaryFromParent.get("parent"), "Regions");
        assertEquals(haPrimaryFromParent.get("primary"), "US East");
    }

    @Test
    public void haPrimariesMultipleChildrenTest() {
        //Setup
        Map<String, String> haPrimaryOne = new HashMap<>();
        haPrimaryOne.put("parent", "Regions");
        haPrimaryOne.put("primary", "US East");

        ArrayList<Map<String, String>> haPrimariesOne = new ArrayList<>();
        haPrimariesOne.add(haPrimaryOne);

        childEntityOne.sensors().set(DASHBOARD_HA_PRIMARIES, haPrimariesOne);

        Map<String, String> haPrimaryTwo = new HashMap<>();
        haPrimaryTwo.put("parent", "Regions 2");
        haPrimaryTwo.put("primary", "US East 2");

        ArrayList<Map<String, String>> haPrimariesTwo = new ArrayList<>();
        haPrimariesTwo.add(haPrimaryTwo);

        childEntityTwo.sensors().set(DASHBOARD_HA_PRIMARIES, haPrimariesTwo);

        //Test
        aggregationJob.run();

        //Verification
        List<Map<String, String>> haPrimariesFromParent = parentEntity.sensors().get(DASHBOARD_HA_PRIMARIES);
        assertEquals(2, haPrimariesFromParent.size());
        Map<String, String> haPrimaryFromParent = haPrimariesFromParent.get(0);
        assertEquals(haPrimaryFromParent.get("parent"), "Regions");
        assertEquals(haPrimaryFromParent.get("primary"), "US East");

        Map<String, String> haPrimaryFromParentTwo = haPrimariesFromParent.get(1);
        assertEquals(haPrimaryFromParentTwo.get("parent"), "Regions 2");
        assertEquals(haPrimaryFromParentTwo.get("primary"), "US East 2");
    }

    @Test
    public void licencesTest() {
        //Setup
        Map<String, String> licencesHighlight = new HashMap<>();
        licencesHighlight.put("RHEL", "1234");

        ArrayList<Map<String, String>> licencesHighlights = new ArrayList<>();
        licencesHighlights.add(licencesHighlight);

        childEntityOne.sensors().set(DASHBOARD_LICENSES, licencesHighlights);

        //Test
        aggregationJob.run();

        //Verification
        List<Map<String, String>> licencesHighlightsFromParent = parentEntity.sensors().get(DASHBOARD_LICENSES);
        assertEquals(1, licencesHighlightsFromParent.size());
        Map<String, String> licencesHighlightFromParent = licencesHighlightsFromParent.get(0);
        assertEquals(licencesHighlightFromParent.get("RHEL"), "1234");
    }

    @Test
    public void licencesMultipleChildrenTest() {
        //Setup
        Map<String, String> licencesHighlightOne = new HashMap<>();
        licencesHighlightOne.put("RHEL", "1234");

        ArrayList<Map<String, String>> licencesHighlightsOne = new ArrayList<>();
        licencesHighlightsOne.add(licencesHighlightOne);

        childEntityOne.sensors().set(DASHBOARD_LICENSES, licencesHighlightsOne);

        Map<String, String> licencesHighlightTwo = new HashMap<>();
        licencesHighlightTwo.put("RHEL", "ABC1");

        ArrayList<Map<String, String>> licencesHighlightsTwo = new ArrayList<>();
        licencesHighlightsTwo.add(licencesHighlightTwo);

        childEntityTwo.sensors().set(DASHBOARD_LICENSES, licencesHighlightsTwo);

        //Test
        aggregationJob.run();

        //Verification
        List<Map<String, String>> licencesHighlightsFromParent = parentEntity.sensors().get(DASHBOARD_LICENSES);
        assertEquals(2, licencesHighlightsFromParent.size());

        Map<String, String> licencesHighlightFromParent = licencesHighlightsFromParent.get(0);
        assertEquals(licencesHighlightFromParent.get("RHEL"), "1234");
        Map<String, String> licencesHighlightFromParentTwo = licencesHighlightsFromParent.get(1);
        assertEquals(licencesHighlightFromParentTwo.get("RHEL"), "ABC1");

    }

    @Test
    public void locationsTest() {
        //Setup
        Map<String, Object> location = new HashMap<>();
        location.put("name", "AWS US East 2");
        location.put("icon", "aws-ec2");
        location.put("count", 3);

        ArrayList<Map<String, Object>> serverLocations = new ArrayList<>();
        serverLocations.add(location);

        Map<String, List<Map<String, Object>>> locations = new HashMap<>();
        locations.put("servers", serverLocations);

        childEntityOne.sensors().set(DASHBOARD_LOCATIONS, locations);

        //Test
        aggregationJob.run();

        //Verification
        Map<String, List<Map<String, Object>>> locationsFromParent = parentEntity.sensors().get(DASHBOARD_LOCATIONS);
        assertEquals(1, locationsFromParent.size());

        List<Map<String, Object>> serverLocationsFromParent = locationsFromParent.get("servers");
        assertEquals(1, serverLocationsFromParent.size());

        Map<String, Object> locationFromParent = serverLocationsFromParent.get(0);
        assertEquals(locationFromParent.get("name"), "AWS US East 2");
        assertEquals(locationFromParent.get("icon"), "aws-ec2");
        assertEquals(locationFromParent.get("count"), 3);
    }

    @Test
    public void locationsMultipleChildrenTest() {
        //Setup
        Map<String, Object> locationOne = new HashMap<>();
        locationOne.put("name", "AWS US East 2");
        locationOne.put("icon", "aws-ec2");
        locationOne.put("count", 3);

        ArrayList<Map<String, Object>> serverLocationsOne = new ArrayList<>();
        serverLocationsOne.add(locationOne);

        Map<String, List<Map<String, Object>>> locationsOne = new HashMap<>();
        locationsOne.put("servers", serverLocationsOne);

        childEntityOne.sensors().set(DASHBOARD_LOCATIONS, locationsOne);

        Map<String, Object> locationTwo = new HashMap<>();
        locationTwo.put("name", "AWS US West");
        locationTwo.put("icon", "aws-ec2");
        locationTwo.put("count", 5);

        ArrayList<Map<String, Object>> serverLocationsTwo = new ArrayList<>();
        serverLocationsTwo.add(locationTwo);

        Map<String, List<Map<String, Object>>> locationsTwo = new HashMap<>();
        locationsTwo.put("servers", serverLocationsTwo);

        childEntityTwo.sensors().set(DASHBOARD_LOCATIONS, locationsTwo);

        //Test
        aggregationJob.run();

        //Verification
        Map<String, List<Map<String, Object>>> locationsFromParent = parentEntity.sensors().get(DASHBOARD_LOCATIONS);
        assertEquals(1, locationsFromParent.size());

        List<Map<String, Object>> serverLocationsFromParent = locationsFromParent.get("servers");
        assertEquals(2, serverLocationsFromParent.size());

        Map<String, Object> locationFromParentOne = serverLocationsFromParent.get(0);
        assertEquals(locationFromParentOne.get("name"), "AWS US East 2");
        assertEquals(locationFromParentOne.get("icon"), "aws-ec2");
        assertEquals(locationFromParentOne.get("count"), 3);

        Map<String, Object> locationFromParentTwo = serverLocationsFromParent.get(1);
        assertEquals(locationFromParentTwo.get("name"), "AWS US West");
        assertEquals(locationFromParentTwo.get("icon"), "aws-ec2");
        assertEquals(locationFromParentTwo.get("count"), 5);
    }

    @Test
    public void locationsMultipleChildrenMergeTest() {
        //Setup
        Map<String, Object> locationOne = new HashMap<>();
        locationOne.put("name", "AWS US East 2");
        locationOne.put("icon", "aws-ec2");
        locationOne.put("count", 3);

        ArrayList<Map<String, Object>> serverLocationsOne = new ArrayList<>();
        serverLocationsOne.add(locationOne);

        Map<String, List<Map<String, Object>>> locationsOne = new HashMap<>();
        locationsOne.put("servers", serverLocationsOne);

        childEntityOne.sensors().set(DASHBOARD_LOCATIONS, locationsOne);

        Map<String, Object> locationTwo = new HashMap<>();
        locationTwo.put("name", "AWS US East 2");
        locationTwo.put("icon", "aws-ec2");
        locationTwo.put("count", 5);

        ArrayList<Map<String, Object>> serverLocationsTwo = new ArrayList<>();
        serverLocationsTwo.add(locationTwo);

        Map<String, List<Map<String, Object>>> locationsTwo = new HashMap<>();
        locationsTwo.put("servers", serverLocationsTwo);

        childEntityTwo.sensors().set(DASHBOARD_LOCATIONS, locationsTwo);

        //Test
        aggregationJob.run();

        //Verification
        Map<String, List<Map<String, Object>>> locationsFromParent = parentEntity.sensors().get(DASHBOARD_LOCATIONS);
        assertEquals(1, locationsFromParent.size());

        List<Map<String, Object>> serverLocationsFromParent = locationsFromParent.get("servers");
        assertEquals(1, serverLocationsFromParent.size());

        Map<String, Object> locationFromParent = serverLocationsFromParent.get(0);
        assertEquals(locationFromParent.get("name"), "AWS US East 2");
        assertEquals(locationFromParent.get("icon"), "aws-ec2");
        assertEquals(locationFromParent.get("count"), 8);
    }
}