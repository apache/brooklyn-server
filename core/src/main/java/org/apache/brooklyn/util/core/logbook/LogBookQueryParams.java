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

import java.util.List;

/**
 * The logbook query parameters.
 */
public class LogBookQueryParams {

    /** The number of log items to query. Note, one log item can be a multi-line one, e.g. a stacktrace */
    private Integer numberOfItems;

    /** The indicator whether to return last number of items (tail) or not */
    private Boolean tail;

    /** The indicator whether to list sub-task items (recursively) or not */
    private Boolean recursive = false;

    /** The log levels: INFO, FATAL, ERROR, DEBUG or WARNING */
    private List<String> levels;

    /** The date-time to look log items from */
    private String dateTimeFrom;

    /** The date-time to look log items up to */
    private String dateTimeTo;

    /** The search phrase to look log items with */
    private String searchPhrase;

    private String taskId;

    private String entityId;

    public Integer getNumberOfItems() {
        return numberOfItems;
    }

    public void setNumberOfItems(Integer numberOfItems) {
        this.numberOfItems = numberOfItems;
    }

    public Boolean isTail() {
        return tail;
    }

    public void setTail(Boolean tail) {
        this.tail = tail;
    }

    public Boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(Boolean recursive) {
        this.recursive = recursive;
    }

    public List<String> getLevels() {
        return levels;
    }

    public void setLevels(List<String> levels) {
        this.levels = levels;
    }

    public String getDateTimeFrom() {
        return dateTimeFrom;
    }

    public void setDateTimeFrom(String dateTimeFrom) {
        this.dateTimeFrom = dateTimeFrom;
    }

    public String getDateTimeTo() {
        return dateTimeTo;
    }

    public void setDateTimeTo(String dateTimeTo) {
        this.dateTimeTo = dateTimeTo;
    }

    public String getSearchPhrase() {
        return searchPhrase;
    }

    public void setSearchPhrase(String searchPhrase) {
        this.searchPhrase = searchPhrase;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }
}
