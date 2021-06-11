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
package org.apache.brooklyn.util.core.logbook.file;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;
import org.apache.brooklyn.util.core.logbook.LogBookQueryParams;
import org.apache.brooklyn.util.core.logbook.LogStore;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Time;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.brooklyn.util.core.logbook.LogbookConfig.BASE_NAME_LOGBOOK;

/**
 * Implementation for expose log from an existing file to the logbook API
 */
public class FileLogStore implements LogStore {

    /*
    # Example config for local default implementation
    brooklyn.logbook.logStore = org.apache.brooklyn.util.core.logbook.file.FileLogStore
    brooklyn.logbook.fileLogStore.path = /var/logs/brooklyn/brooklyn.debug.log
    */
    public final static String BASE_NAME_FILE_LOG_STORE = BASE_NAME_LOGBOOK + ".fileLogStore";
    public final static ConfigKey<String> LOGBOOK_LOG_STORE_PATH = ConfigKeys.newStringConfigKey(
            BASE_NAME_FILE_LOG_STORE + ".path", "Log file path", "/var/logs/brooklyn/brooklyn.debug.log");

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_REGEX = ConfigKeys.newStringConfigKey(
            BASE_NAME_FILE_LOG_STORE + ".regexPattern",
            "Log entry regex pattern",
            "^(?<timestamp>\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}) (?<taskId>\\S+)?-(?<entityIds>\\S+)? (?<level>\\w{4} |\\w{5})\\W{1,4}(?<bundleId>\\d{1,3}) (?<class>(?:\\S\\.)*\\S*) \\[(?<threadName>\\S+)\\] (?<message>[\\s\\S]*?)\\n*(?=^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}|\\z)");

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_DATEFORMAT = ConfigKeys.newStringConfigKey(
            BASE_NAME_FILE_LOG_STORE + ".dateFormat", "Date format", "yyyy-MM-dd'T'HH:mm:ss,SSS");

    private final String filePath;
    private final Path path;
    private final String logLinePattern;
    private final String logDateFormat;

    public FileLogStore() {
        this.path = null;
        this.filePath = "";
        this.logLinePattern = LOGBOOK_LOG_STORE_REGEX.getDefaultValue();
        this.logDateFormat = LOGBOOK_LOG_STORE_DATEFORMAT.getDefaultValue();
    }

    public FileLogStore(ManagementContext mgmt) {
        this.filePath = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_PATH);
        this.logLinePattern = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_REGEX);
        this.logDateFormat = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_DATEFORMAT);
        Preconditions.checkNotNull(filePath, "Log file path must be set: " + LOGBOOK_LOG_STORE_PATH.getName());
        this.path = Paths.get(this.filePath);
    }

    @Override
    public List<BrooklynLogEntry> query(LogBookQueryParams params) {
        // TODO the read of the file needs to be improved, specially to implement reading the file backwards
        try (Stream<String> stream = Files.lines(path)) {
            Predicate<BrooklynLogEntry> filter = brooklynLogEntry -> {
                String initTime = params.getInitTime();
                String finalTime = params.getFinalTime();
                boolean levelFilter = true;
                boolean initTimeFilter = true;
                boolean finalTimeFilter = true;
                if (!params.getLevels().isEmpty() && !params.getLevels().contains("ALL"))
                    levelFilter = params.getLevels().contains(brooklynLogEntry.getLevel());
                if (Strings.isNonBlank(initTime) && brooklynLogEntry.getDatetime() != null) {
                    Date initDate = Time.parseDate(initTime);
                    initTimeFilter = brooklynLogEntry.getDatetime().compareTo(initDate) > 0;
                }
                if (Strings.isNonBlank(finalTime) && brooklynLogEntry.getDatetime() != null) {
                    Date finalDate = Time.parseDate(finalTime);
                    finalTimeFilter = brooklynLogEntry.getDatetime().compareTo(finalDate) < 0;
                }

                return levelFilter && initTimeFilter && finalTimeFilter;
            };
            return stream
                    .map(this::parseLogLine)
                    .filter(filter)
                    .limit(params.getNumberOfItems())
                    .sorted((o1, o2) -> {
                        if (params.getReverseOrder()) {
                            return o2.getDatetime().compareTo(o1.getDatetime());
                        } else {
                            return o1.getDatetime().compareTo(o2.getDatetime());
                        }
                    })
                    .collect(Collectors.toList());

        } catch (IOException e) {
            Exceptions.propagate(e);
        }
        return ImmutableList.of();
    }

    protected BrooklynLogEntry parseLogLine(String logLine) {
        Pattern p = Pattern.compile(this.logLinePattern);
        Matcher m = p.matcher(logLine);
        BrooklynLogEntry entry = new BrooklynLogEntry();
        assert m.find();
        if (m.matches()) {
            entry.setTimestampString(m.group("timestamp"));
            Maybe<Calendar> calendarMaybe = Time.parseCalendarFormat(entry.getTimestampString(), logDateFormat);
            if (calendarMaybe.isPresentAndNonNull()) {
                entry.setDatetime(calendarMaybe.get().getTime());
            }
            entry.setTaskId(m.group("taskId"));
            entry.setEntityIds(m.group("entityIds"));
            entry.setLevel(m.group("level"));
            entry.setBundleId(m.group("bundleId"));
            entry.setClazz(m.group("class"));
            entry.setThreadName(m.group("threadName"));
            entry.setMessage(m.group("message"));
        }
        return entry;
    }
}
