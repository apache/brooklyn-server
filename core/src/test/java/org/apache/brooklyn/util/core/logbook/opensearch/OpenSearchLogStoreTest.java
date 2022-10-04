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
        p.setTail(false);
        p.setDateTimeFrom("2021-06-01T13:18:48,482");
        p.setDateTimeTo("2021-06-01T13:28:48,482");
        p.setLevels(ImmutableList.of());
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"timestamp\":{\"gte\":\"2021-06-01T13:18:48,482\",\"lte\":\"2021-06-01T13:28:48,482\"}}}]}}}");
    }

    @Test
    public void queryWithTailRequest() {
        String EXPECTED_QUERY_HEAD = "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"match_all\":{}}}";
        String EXPECTED_QUERY_TAIL = "{\"sort\":{\"timestamp\":\"desc\"},\"size\":10,\"query\":{\"match_all\":{}}}";

        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setLevels(ImmutableList.of());

        // Tail disabled
        p.setTail(false);
        String query = cut.getJsonQuery(p);
        assertEquals(query, EXPECTED_QUERY_HEAD);

        // Tail enabled - expect query with 'desc' sorting order, instead of 'asc'. Output is reversed back after 'desc' query.
        p.setTail(true);
        query = cut.getJsonQuery(p);
        assertEquals(query, EXPECTED_QUERY_TAIL);
    }

    @Test
    public void queryWithDateTimeFrom() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setDateTimeFrom("2021-01-01T00:00:00,001");
        p.setLevels(ImmutableList.of());
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"timestamp\":{\"gte\":\"2021-01-01T00:00:00,001\"}}}]}}}");
    }

    @Test
    public void queryWithDateTimeTo() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setDateTimeTo("2021-01-01T00:00:00,001");
        p.setLevels(ImmutableList.of());
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"timestamp\":{\"lte\":\"2021-01-01T00:00:00,001\"}}}]}}}");
    }

    @Test
    public void queryWithTwoLogLevel() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setLevels(ImmutableList.of("WARN", "DEBUG"));
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"terms\":{\"level\":[\"warn\",\"debug\"]}}]}}}");
    }

    @Test
    public void queryWithOneLogLevelAndRange() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setDateTimeFrom("2021-06-01T13:18:48,482");
        p.setDateTimeTo("2021-06-01T13:28:48,482");
        p.setLevels(ImmutableList.of("DEBUG"));
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"terms\":{\"level\":[\"debug\"]}},{\"range\":{\"timestamp\":{\"gte\":\"2021-06-01T13:18:48,482\",\"lte\":\"2021-06-01T13:28:48,482\"}}}]}}}");
    }

    @Test
    public void queryWithTwoLogLevelAndRange() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setDateTimeFrom("2021-06-01T13:18:48,482");
        p.setDateTimeTo("2021-06-01T13:28:48,482");
        p.setLevels(ImmutableList.of("WARN", "DEBUG"));
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"terms\":{\"level\":[\"warn\",\"debug\"]}},{\"range\":{\"timestamp\":{\"gte\":\"2021-06-01T13:18:48,482\",\"lte\":\"2021-06-01T13:28:48,482\"}}}]}}}");
    }

    @Test
    public void queryWithSearchSinglePhrase() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setLevels(ImmutableList.of());
        p.setSearchPhrase("some phrase");
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"match_phrase\":{\"message\":\"some phrase\"}}]}}}");
    }

    @Test
    public void queryWithEntityId() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setLevels(ImmutableList.of());
        p.setEntityId("entityIdxx");
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"entityIds\":\"entityIdxx\"}},{\"match_phrase\":{\"message\":\"entityIdxx\"}}]}}]}}}");
    }

    @Test
    public void queryWithTaskId() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setLevels(ImmutableList.of());
        p.setTaskId("taskIdxxxx");
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"taskId\":\"taskIdxxxx\"}},{\"match_phrase\":{\"message\":\"taskIdxxxx\"}}]}}]}}}");
    }

    @Test
    public void queryWithEntityIdAndPhrase() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setLevels(ImmutableList.of());
        p.setEntityId("entityIdxx");
        p.setSearchPhrase("some phrase");
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"entityIds\":\"entityIdxx\"}},{\"match_phrase\":{\"message\":\"entityIdxx\"}}]}},{\"match_phrase\":{\"message\":\"some phrase\"}}]}}}");
    }

    @Test
    public void queryWithTaskIdAndPhrase() {
        OpenSearchLogStore cut = new OpenSearchLogStore();
        LogBookQueryParams p = new LogBookQueryParams();
        p.setNumberOfItems(10);
        p.setTail(false);
        p.setLevels(ImmutableList.of());
        p.setTaskId("taskIdxxxx");
        p.setSearchPhrase("some phrase");
        String query = cut.getJsonQuery(p);
        assertEquals(query, "{\"sort\":{\"timestamp\":\"asc\"},\"size\":10,\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"taskId\":\"taskIdxxxx\"}},{\"match_phrase\":{\"message\":\"taskIdxxxx\"}}]}},{\"match_phrase\":{\"message\":\"some phrase\"}}]}}}");
    }
}