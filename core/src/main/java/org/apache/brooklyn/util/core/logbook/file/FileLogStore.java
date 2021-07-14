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

import com.google.common.annotations.VisibleForTesting;
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

    @VisibleForTesting
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

        // TODO: the read of the file needs to be improved, specially to implement reading the file backwards and
        //  do a correct multiline log reading
        try (Stream<String> stream = Files.lines(path)) {
            Predicate<BrooklynLogEntry> filter = brooklynLogEntry -> {

                // Excludes unrecognized items or items without a date, typically they are multiline log messages.
                if (brooklynLogEntry == null || brooklynLogEntry.getDatetime() == null) {
                    // TODO: fix the RegEx to process multiline log messages like stack-traces, and remove this condition.
                    return false;
                }

                // Initialize matching conditions as true by default.
                boolean isLogLevelMatch = true;
                boolean isDateTimeFromMatch = true;
                boolean isDateTimeToMatch = true;
                boolean isSearchPhraseMatch = true;
                // Re-evaluate conditions if requested in the query.

                // Check log levels.
                if (!params.getLevels().isEmpty() && !params.getLevels().contains("ALL")) {
                    isLogLevelMatch = params.getLevels().contains(brooklynLogEntry.getLevel());
                }

                // Check the date-time range.
                if (brooklynLogEntry.getDatetime() != null) {

                    // Date-time from.
                    if (Strings.isNonBlank(params.getDateTimeFrom())) {
                        Date initDate = Time.parseDate(params.getDateTimeFrom());
                        isDateTimeFromMatch = brooklynLogEntry.getDatetime().compareTo(initDate) >= 0;
                    }

                    // Date-time to.
                    if (Strings.isNonBlank(params.getDateTimeTo())) {
                        Date finalDate = Time.parseDate(params.getDateTimeTo());
                        isDateTimeToMatch = brooklynLogEntry.getDatetime().compareTo(finalDate) <= 0;
                    }
                }

                // Check the search phrase.
                if (Strings.isNonBlank(params.getSearchPhrase()) && Strings.isNonBlank(brooklynLogEntry.getMessage())) {
                    isSearchPhraseMatch = brooklynLogEntry.getMessage().contains(params.getSearchPhrase());
                }

                return isLogLevelMatch && isDateTimeFromMatch && isDateTimeToMatch && isSearchPhraseMatch;
            };

            // Filter result first, we need to know how many to skip go get the tail.
            List<BrooklynLogEntry> filteredQueryResult = stream
                    .map(this::parseLogLine)
                    .filter(filter)
                    .collect(Collectors.toList());

            // Now get 'tail' of the filtered query, if requested, or 'head' otherwise.
            Stream<BrooklynLogEntry> filteredStream;
            if (params.isTail()) {
                // Get 'tail' - last number of lines, with the stream().skip().
                filteredStream = filteredQueryResult.stream()
                        .skip(Math.max(0, filteredQueryResult.size() - params.getNumberOfItems()));
            } else {
                // Get 'head' - first number of lines, with the stream().limit()
                filteredStream = filteredQueryResult.stream().limit(params.getNumberOfItems());
            }

            // Collect the query result.
            return filteredStream.collect(Collectors.toList());

        } catch (IOException e) {
            Exceptions.propagate(e);
        }
        return ImmutableList.of();
    }

    protected BrooklynLogEntry parseLogLine(String logLine) {
        Pattern p = Pattern.compile(this.logLinePattern);
        Matcher m = p.matcher(logLine);
        BrooklynLogEntry entry = null;
        m.find();
        if (m.matches()) {
            entry = new BrooklynLogEntry();
            entry.setTimestampString(m.group("timestamp"));
            Maybe<Calendar> calendarMaybe = Time.parseCalendarFormat(entry.getTimestampString(), logDateFormat);
            if (calendarMaybe.isPresentAndNonNull()) {
                entry.setDatetime(calendarMaybe.get().getTime());
            }
            entry.setTaskId(m.group("taskId"));
            entry.setEntityIds(m.group("entityIds"));
            entry.setLevel(m.group("level").trim()); // Trim the log level key.
            entry.setBundleId(m.group("bundleId"));
            entry.setClazz(m.group("class"));
            entry.setThreadName(m.group("threadName"));
            entry.setMessage(m.group("message"));
        }
        return entry;
    }
}
