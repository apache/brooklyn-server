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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;

import java.util.List;

/**
 * This class models the data returned by the ElasticSearch search method. Only the required fields for the implemented
 * behaviour are present.
 * The original doc can be found in: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-api-response-body
 */
class BrooklynOpenSearchModel {
    OpenSearchHitsWrapper hits;
    Integer took;
    @JsonProperty("timed_out")
    Boolean timedOut;

    public OpenSearchHitsWrapper getHits() {
        return hits;
    }

    public void setHits(OpenSearchHitsWrapper hits) {
        this.hits = hits;
    }

    public Integer getTook() {
        return took;
    }

    public void setTook(Integer took) {
        this.took = took;
    }

    public Boolean getTimedOut() {
        return timedOut;
    }

    public void setTimedOut(Boolean timedOut) {
        this.timedOut = timedOut;
    }

    static class OpenSearchHitsWrapper {
        List<OpenSearchHit> hits;

        public List<OpenSearchHit> getHits() {
            return hits;
        }

        public void setHits(List<OpenSearchHit> hits) {
            this.hits = hits;
        }
    }

    static class OpenSearchHit {
        @JsonProperty("_index")
        String index;

        @JsonProperty("_type")
        String type;

        @JsonProperty("_source")
        BrooklynLogEntry source;

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public BrooklynLogEntry getSource() {
            return source;
        }

        public void setSource(BrooklynLogEntry source) {
            this.source = source;
        }

    }

}
