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

import com.google.common.collect.ImmutableList;

import java.util.List;

public class StaticLogStore implements LogStore {

    private final List<String> logData;


    public StaticLogStore() {
        this.logData = ImmutableList.<String>builder()
                .add("Log line 01")
                .add("Log line 02")
                .add("Log line 03")
                .add("Log line 04")
                .add("Log line 05")
                .add("Log line 06")
                .add("Log line 07")
                .add("Log line 08")
                .add("Log line 09")
                .add("Log line 10")
                .add("Log line 11")
                .add("Log line 12")
                .add("Log line 13")
                .add("Log line 14")
                .add("Log line 15")
                .build();
    }

    @Override
    public List<String> query(LogBookQueryParams params) {
        return logData;
    }

    @Override
    public List<String> getEntries(Integer from, Integer numberOfItems) {
        if (from >= 0) {
            return logData.subList(from, logData.size() > from + numberOfItems ? from + numberOfItems : logData.size());
        } else {
            int upperLimit = logData.size() + from - numberOfItems;// total+from-num
            int lowLimit = logData.size() + from; // total+from

            return logData.subList(upperLimit, lowLimit);
        }
    }
}
