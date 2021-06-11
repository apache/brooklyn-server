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

import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;

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
        BrooklynLogEntry _source;

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

        public BrooklynLogEntry get_source() {
            return _source;
        }

        public void set_source(BrooklynLogEntry _source) {
            this._source = _source;
        }

        public BrooklynLogEntry getSource() {
            return _source;
        }
    }

}
