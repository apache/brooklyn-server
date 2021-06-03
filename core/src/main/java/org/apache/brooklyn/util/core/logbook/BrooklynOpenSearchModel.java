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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.List;

class BrooklynOpenSearchModel {
    OpenSearchHitsWrapper hits;
    Integer took;
    Boolean timed_out;

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

    public Boolean getTimed_out() {
        return timed_out;
    }

    public void setTimed_out(Boolean timed_out) {
        this.timed_out = timed_out;
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
        String _index;
        String _type;
        BrooklynHit _source;

        public String get_index() {
            return _index;
        }

        public void set_index(String _index) {
            this._index = _index;
        }

        public String get_type() {
            return _type;
        }

        public void set_type(String _type) {
            this._type = _type;
        }

        public BrooklynHit get_source() {
            return _source;
        }

        public void set_source(BrooklynHit _source) {
            this._source = _source;
        }

        public BrooklynHit getSource() {
            return _source;
        }
    }

    static class BrooklynHit {
        String timestamp;
        String level;
        String bundle_id;
        @JsonProperty("class")
        String clazz;
        String thread_name;
        String message;

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }

        public String getBundle_id() {
            return bundle_id;
        }

        public void setBundle_id(String bundle_id) {
            this.bundle_id = bundle_id;
        }

        public String getClazz() {
            return clazz;
        }

        public void setClazz(String clazz) {
            this.clazz = clazz;
        }

        public String getThread_name() {
            return thread_name;
        }

        public void setThread_name(String thread_name) {
            this.thread_name = thread_name;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String toJsonString() {
            String json = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                json = objectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                Exceptions.propagate(e);
            }
            return json;
        }

        @Override
        public String toString() {
            return "{" +
                    "timestamp='" + timestamp + '\'' +
                    ", level='" + level + '\'' +
                    ", bundle_id='" + bundle_id + '\'' +
                    ", class='" + clazz + '\'' +
                    ", thread_name='" + thread_name + '\'' +
                    ", message='" + message + '\'' +
                    '}';
        }
    }
}
