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
package org.apache.brooklyn.util.core.logbook.opensearch;

import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.util.core.logbook.LogBookQueryParams;
import org.junit.Test;

import static org.apache.brooklyn.test.Asserts.assertEquals;

public class OpenSearchLogStoreTest {

    @Test
    public void queryWithTimeRange() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setReverseOrder(false);
        p.setInitTime("2021-06-01T13:18:48,482");
        p.setFinalTime("2021-06-01T13:28:48,482");
        p.setLevels(ImmutableList.of());
        String query = cut.getJSONQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"timestamp\":{\"gte\":\"2021-06-01T13:18:48,482\",\"lte\":\"2021-06-01T13:28:48,482\"}}}]}}}");
    }

    @Test
    public void queryWithTimeInitialTime() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setReverseOrder(false);
        p.setInitTime("2021-01-01T00:00:00,001");
        p.setLevels(ImmutableList.of());
        String query = cut.getJSONQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"timestamp\":{\"gte\":\"2021-01-01T00:00:00,001\"}}}]}}}");
    }

    @Test
    public void queryWithTimeFinalTime() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setReverseOrder(false);
        p.setFinalTime("2021-01-01T00:00:00,001");
        p.setLevels(ImmutableList.of());
        String query = cut.getJSONQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"timestamp\":{\"lte\":\"2021-01-01T00:00:00,001\"}}}]}}}");
    }

    @Test
    public void queryWithTwoLogLevel() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setReverseOrder(false);
        p.setLevels(ImmutableList.of("WARN", "DEBUG"));
        String query = cut.getJSONQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"terms\":{\"level\":[\"warn\",\"debug\"]}}]}}}");
    }

    @Test
    public void queryWithOneLogLevelAndRange() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setReverseOrder(false);
        p.setInitTime("2021-06-01T13:18:48,482");
        p.setFinalTime("2021-06-01T13:28:48,482");
        p.setLevels(ImmutableList.of("DEBUG"));
        String query = cut.getJSONQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"terms\":{\"level\":[\"debug\"]}},{\"range\":{\"timestamp\":{\"gte\":\"2021-06-01T13:18:48,482\",\"lte\":\"2021-06-01T13:28:48,482\"}}}]}}}");
    }

    @Test
    public void queryWithTwoLogLevelAndRange() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setReverseOrder(false);
        p.setInitTime("2021-06-01T13:18:48,482");
        p.setFinalTime("2021-06-01T13:28:48,482");
        p.setLevels(ImmutableList.of("WARN", "DEBUG"));
        String query = cut.getJSONQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"terms\":{\"level\":[\"warn\",\"debug\"]}},{\"range\":{\"timestamp\":{\"gte\":\"2021-06-01T13:18:48,482\",\"lte\":\"2021-06-01T13:28:48,482\"}}}]}}}");
    }
}