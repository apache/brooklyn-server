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

import com.google.common.collect.ImmutableList;
import junit.framework.TestCase;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;
import org.apache.brooklyn.util.core.logbook.LogBookQueryParams;
import org.junit.Test;
import org.testng.annotations.*;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.brooklyn.util.core.logbook.file.FileLogStore.LOGBOOK_LOG_STORE_DATEFORMAT;
import static org.apache.brooklyn.util.core.logbook.file.FileLogStore.LOGBOOK_LOG_STORE_PATH;

public class FileLogStoreTest extends TestCase {

    private final String UNEXPECTED_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss,SSS";
    private final String JAVA_LOG_SAMPLE_PATH = "brooklyn/util/core/logbook/file/log-sample.txt";
    private final String JAVA_LOG_LINE = "2021-05-27T11:36:59,251 - DEBUG 146 o.a.b.c.m.i.LocalManagementContext [qtp158784971-235] Top-level effector invocation: restart[] on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}";
    private final String JAVA_LOG_LINE_WITH_NO_DATETIME = " - DEBUG 146 o.a.b.c.m.i.LocalManagementContext [qtp158784971-235] Top-level effector invocation: restart[] on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}";
    private final String JAVA_LOG_LINE_WITH_EXTRA_SPACE = "2021-06-07T14:58:58,487 - INFO    6 o.o.p.l.s.s.EventAdminConfigurationNotifier [s4j.pax.logging)] Sending Event Admin notification (configuration successful) to org/ops4j/pax/logging/Configuration";
    private final String TASK_LOG_LINE = "2021-05-27T11:36:59,258 OGObOWJs-[gwpndj09r8] DEBUG 146 o.a.b.c.m.i.EffectorUtils [ager-WgxriwjB-43] Invoking effector restart on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}";
    private final String JAVA_LOG_MULTI_LINE_TEXT = "2021-07-05T12:38:09,351 - ERROR 293 o.a.b.u.m.ExternalUiModule [tures-3-thread-1] bundle org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules:1.1.0.SNAPSHOT (293)[org.apache.brooklyn.ui.modularity.ExternalUiModule] : Cannot register component\n" +
            "org.osgi.service.component.ComponentException: The component name 'org.apache.brooklyn.ui.external.module' has already been registered by Bundle 293 (org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules) as Component of Class org.apache.brooklyn.ui.modularity.ExternalUiModule\n" +
            "\tat org.apache.felix.scr.impl.ComponentRegistry.checkComponentName(ComponentRegistry.java:240) ~[?:?]\n" +
            "\tat org.apache.felix.scr.impl.BundleComponentActivator.validateAndRegister(BundleComponentActivator.java:443) ~[?:?]";

    private final AtomicInteger lineCount = new AtomicInteger();

    @BeforeTest
    public void setUp() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @BeforeMethod
    public void reSet() {
        lineCount.set(0);
    }

    @Test
    public void testParseLogJavaLine() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(JAVA_LOG_LINE, lineCount);
        assertNull(brooklynLogEntry.getTaskId());
        assertNull(brooklynLogEntry.getEntityIds());
        assertEquals("2021-05-27T11:36:59,251", brooklynLogEntry.getTimestampString());
        assertEquals("Thu May 27 11:36:59 UTC 2021", brooklynLogEntry.getDatetime().toString());
        assertEquals("DEBUG", brooklynLogEntry.getLevel());
        assertEquals("146", brooklynLogEntry.getBundleId());
        assertEquals("o.a.b.c.m.i.LocalManagementContext", brooklynLogEntry.getClazz());
        assertEquals("qtp158784971-235", brooklynLogEntry.getThreadName());
        assertEquals("Top-level effector invocation: restart[] on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}", brooklynLogEntry.getMessage());
        assertEquals(String.valueOf(1), brooklynLogEntry.getLineId());
    }

    @Test
    public void testParseLogJavaLineWithExtraSpace() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(JAVA_LOG_LINE_WITH_EXTRA_SPACE, lineCount);
        assertNull(brooklynLogEntry.getTaskId());
        assertNull(brooklynLogEntry.getEntityIds());
        assertEquals("2021-06-07T14:58:58,487", brooklynLogEntry.getTimestampString());
        assertEquals("Mon Jun 07 14:58:58 UTC 2021", brooklynLogEntry.getDatetime().toString());
        assertEquals("INFO", brooklynLogEntry.getLevel());
        assertEquals("6", brooklynLogEntry.getBundleId());
        assertEquals("o.o.p.l.s.s.EventAdminConfigurationNotifier", brooklynLogEntry.getClazz());
        assertEquals("s4j.pax.logging)", brooklynLogEntry.getThreadName());
        assertEquals("Sending Event Admin notification (configuration successful) to org/ops4j/pax/logging/Configuration", brooklynLogEntry.getMessage());
        assertEquals(String.valueOf(1), brooklynLogEntry.getLineId());
    }

    @Test
    public void testParseLogTaskLine() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(TASK_LOG_LINE, lineCount);
        assertEquals("2021-05-27T11:36:59,258", brooklynLogEntry.getTimestampString());
        assertEquals("Thu May 27 11:36:59 UTC 2021", brooklynLogEntry.getDatetime().toString());
        assertEquals("OGObOWJs", brooklynLogEntry.getTaskId());
        assertEquals("[gwpndj09r8]", brooklynLogEntry.getEntityIds());
        assertEquals("DEBUG", brooklynLogEntry.getLevel());
        assertEquals("146", brooklynLogEntry.getBundleId());
        assertEquals("o.a.b.c.m.i.EffectorUtils", brooklynLogEntry.getClazz());
        assertEquals("ager-WgxriwjB-43", brooklynLogEntry.getThreadName());
        assertEquals("Invoking effector restart on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}", brooklynLogEntry.getMessage());
        assertEquals(String.valueOf(1), brooklynLogEntry.getLineId());
    }

    @Test
    public void testParseMultiLineLog() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(JAVA_LOG_MULTI_LINE_TEXT, lineCount);
        assertNull(brooklynLogEntry.getTaskId());
        assertNull(brooklynLogEntry.getEntityIds());
        assertEquals("2021-07-05T12:38:09,351", brooklynLogEntry.getTimestampString());
        assertEquals("Mon Jul 05 12:38:09 UTC 2021", brooklynLogEntry.getDatetime().toString());
        assertEquals("ERROR", brooklynLogEntry.getLevel());
        assertEquals("293", brooklynLogEntry.getBundleId());
        assertEquals("o.a.b.u.m.ExternalUiModule", brooklynLogEntry.getClazz());
        assertEquals("tures-3-thread-1", brooklynLogEntry.getThreadName());
        assertEquals("bundle org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules:1.1.0.SNAPSHOT (293)[org.apache.brooklyn.ui.modularity.ExternalUiModule] : Cannot register component\n" +
                "org.osgi.service.component.ComponentException: The component name 'org.apache.brooklyn.ui.external.module' has already been registered by Bundle 293 (org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules) as Component of Class org.apache.brooklyn.ui.modularity.ExternalUiModule\n" +
                "\tat org.apache.felix.scr.impl.ComponentRegistry.checkComponentName(ComponentRegistry.java:240) ~[?:?]\n" +
                "\tat org.apache.felix.scr.impl.BundleComponentActivator.validateAndRegister(BundleComponentActivator.java:443) ~[?:?]", brooklynLogEntry.getMessage());
        assertEquals(String.valueOf(1), brooklynLogEntry.getLineId());
    }

    @Test
    public void testParseLogWithNoDateTime() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(JAVA_LOG_LINE_WITH_NO_DATETIME, lineCount);
        assertNull(brooklynLogEntry);
    }

    @Test
    public void testParseLogWithDateTimeFormatMismatch() {
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_DATEFORMAT.getName(), UNEXPECTED_DATE_TIME_FORMAT);
        FileLogStore cut = new FileLogStore(mgmt);
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(JAVA_LOG_LINE, lineCount);
        assertNull(brooklynLogEntry.getTaskId());
        assertNull(brooklynLogEntry.getEntityIds());
        assertNull(brooklynLogEntry.getDatetime());
        assertEquals("2021-05-27T11:36:59,251", brooklynLogEntry.getTimestampString());
        assertEquals("DEBUG", brooklynLogEntry.getLevel());
        assertEquals("146", brooklynLogEntry.getBundleId());
        assertEquals("o.a.b.c.m.i.LocalManagementContext", brooklynLogEntry.getClazz());
        assertEquals("qtp158784971-235", brooklynLogEntry.getThreadName());
        assertEquals("Top-level effector invocation: restart[] on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}", brooklynLogEntry.getMessage());
        assertEquals(String.valueOf(1), brooklynLogEntry.getLineId());
    }

    @Test
    public void testQueryLogSample() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(2); // Request first two only.
        logBookQueryParams.setTail(false);
        logBookQueryParams.setLevels(ImmutableList.of());
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);
        assertEquals(2, brooklynLogEntries.size());

        // Check first log line
        BrooklynLogEntry firstBrooklynLogEntry = brooklynLogEntries.get(0);
        assertNull(firstBrooklynLogEntry.getTaskId());
        assertNull(firstBrooklynLogEntry.getEntityIds());
        assertEquals("2021-05-27T11:36:59,251", firstBrooklynLogEntry.getTimestampString());
        assertEquals("Thu May 27 11:36:59 UTC 2021",firstBrooklynLogEntry.getDatetime().toString());
        assertEquals("DEBUG", firstBrooklynLogEntry.getLevel());
        assertEquals("146", firstBrooklynLogEntry.getBundleId());
        assertEquals("o.a.b.c.m.i.LocalManagementContext", firstBrooklynLogEntry.getClazz());
        assertEquals("qtp158784971-235", firstBrooklynLogEntry.getThreadName());
        assertEquals("Top-level effector invocation: restart[] on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}", firstBrooklynLogEntry.getMessage());
        assertEquals(String.valueOf(1), firstBrooklynLogEntry.getLineId());

        // Check second log line. NOTE, this is the same multiline example.
        BrooklynLogEntry secondBrooklynLogEntry = brooklynLogEntries.get(1);
        assertNull(secondBrooklynLogEntry.getTaskId());
        assertNull(secondBrooklynLogEntry.getEntityIds());
        assertEquals("2021-07-05T12:38:09,351", secondBrooklynLogEntry.getTimestampString());
        assertEquals("Mon Jul 05 12:38:09 UTC 2021", secondBrooklynLogEntry.getDatetime().toString());
        assertEquals("ERROR", secondBrooklynLogEntry.getLevel());
        assertEquals("293", secondBrooklynLogEntry.getBundleId());
        assertEquals("o.a.b.u.m.ExternalUiModule", secondBrooklynLogEntry.getClazz());
        assertEquals("tures-3-thread-1", secondBrooklynLogEntry.getThreadName());
        assertEquals(String.valueOf(2), secondBrooklynLogEntry.getLineId());

        // TODO: this log message is expected to be a multi-line one. Fix log-store RegEx to support this.
        //       The second assertion below is the expected one, not the following one:
        assertEquals("bundle org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules:1.1.0.SNAPSHOT (293)[org.apache.brooklyn.ui.modularity.ExternalUiModule] : Cannot register component", secondBrooklynLogEntry.getMessage());
        //assertEquals("bundle org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules:1.1.0.SNAPSHOT (293)[org.apache.brooklyn.ui.modularity.ExternalUiModule] : Cannot register component\n" +
        //        "org.osgi.service.component.ComponentException: The component name 'org.apache.brooklyn.ui.external.module' has already been registered by Bundle 293 (org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules) as Component of Class org.apache.brooklyn.ui.modularity.ExternalUiModule\n" +
        //        "\tat org.apache.felix.scr.impl.ComponentRegistry.checkComponentName(ComponentRegistry.java:240) ~[?:?]\n" +
        //        "\tat org.apache.felix.scr.impl.BundleComponentActivator.validateAndRegister(BundleComponentActivator.java:443) ~[?:?]", secondBrooklynLogEntry.getMessage());
    }

    @Test
    public void testQueryLogSampleWithDateTimeFormatMismatch() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_DATEFORMAT.getName(), UNEXPECTED_DATE_TIME_FORMAT);
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(1000); // Request all.
        logBookQueryParams.setTail(false);
        logBookQueryParams.setLevels(ImmutableList.of());
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);

        // Expect no entries found, date-time format did not match, sorting is not possible.
        assertEquals(0, brooklynLogEntries.size());
    }

    @Test
    public void testQueryTailOfLogSample() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(4); // Request 4 records.
        logBookQueryParams.setTail(true); // Request tail!
        logBookQueryParams.setLevels(ImmutableList.of());
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);
        assertEquals(4, brooklynLogEntries.size());

        // Test with log levels only. There are 5 records in total in the normal order: DEBUG, ERROR, INFO, INFO, WARN.
        // Expect 4 last items starting with ERROR.
        assertEquals("ERROR", brooklynLogEntries.get(0).getLevel());
        assertEquals("INFO", brooklynLogEntries.get(1).getLevel());
        assertEquals("INFO", brooklynLogEntries.get(2).getLevel());
        assertEquals("WARN", brooklynLogEntries.get(3).getLevel());
    }

    @Test
    public void testQueryLogSampleWithSearchSinglePhrase() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(2); // Request first two only.
        logBookQueryParams.setTail(false);
        logBookQueryParams.setLevels(ImmutableList.of());
        logBookQueryParams.setSearchPhrases(ImmutableList.of("Cannot register component")); // Request search phrase.
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);
        assertEquals(1, brooklynLogEntries.size());

        // Search phrase appears in ERROR log line only.
        BrooklynLogEntry brooklynLogEntry = brooklynLogEntries.get(0);
        assertEquals("ERROR", brooklynLogEntry.getLevel());
        // TODO: this log message is expected to be a multi-line one. Fix log-store RegEx to support this.
        //       The second assertion below is the expected one, not the following one:
        assertEquals("bundle org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules:1.1.0.SNAPSHOT (293)[org.apache.brooklyn.ui.modularity.ExternalUiModule] : Cannot register component", brooklynLogEntry.getMessage());
        //assertEquals("bundle org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules:1.1.0.SNAPSHOT (293)[org.apache.brooklyn.ui.modularity.ExternalUiModule] : Cannot register component\n" +
        //        "org.osgi.service.component.ComponentException: The component name 'org.apache.brooklyn.ui.external.module' has already been registered by Bundle 293 (org.apache.brooklyn.ui.modularity.brooklyn-ui-external-modules) as Component of Class org.apache.brooklyn.ui.modularity.ExternalUiModule\n" +
        //        "\tat org.apache.felix.scr.impl.ComponentRegistry.checkComponentName(ComponentRegistry.java:240) ~[?:?]\n" +
        //        "\tat org.apache.felix.scr.impl.BundleComponentActivator.validateAndRegister(BundleComponentActivator.java:443) ~[?:?]", secondBrooklynLogEntry.getMessage());

        // TODO: cover case with search phrase in the stack-trace, once multi-line issue mentioned above is resolved.
    }

    @Test
    public void testQueryLogSampleWithSearchMultiplePhrases() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(2); // Request first two only.
        logBookQueryParams.setTail(false);
        logBookQueryParams.setLevels(ImmutableList.of());
        logBookQueryParams.setSearchPhrases(ImmutableList.of("bundle")); // Request search phrase.
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);
        assertEquals(2, brooklynLogEntries.size());

        // Search phrase appears in ERROR and WARN log lines.
        assertEquals("ERROR", brooklynLogEntries.get(0).getLevel());
        assertEquals("WARN", brooklynLogEntries.get(1).getLevel());

        // Now request multiple search phrases
        logBookQueryParams.setSearchPhrases(ImmutableList.of("bundle", "is waiting for dependencies"));
        brooklynLogEntries = fileLogStore.query(logBookQueryParams);
        assertEquals(1, brooklynLogEntries.size());

        // 2 phrases appear in WARN log line only
        assertEquals("WARN", brooklynLogEntries.get(0).getLevel());
    }

    @Test
    public void testQueryLogSampleWithZeroNumberOfLInes() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(0); // Request zero lines.
        logBookQueryParams.setTail(false);
        logBookQueryParams.setLevels(ImmutableList.of());
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);
        assertEquals(0, brooklynLogEntries.size());
    }

    @Test
    public void testQueryLogSampleWithDateTimeRange() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(1000); // Request all.
        logBookQueryParams.setTail(false);
        logBookQueryParams.setLevels(ImmutableList.of());
        logBookQueryParams.setDateTimeFrom("Mon Jul 05 12:38:10 UTC 2021"); // Date of the first INFO log line.
        logBookQueryParams.setDateTimeTo("Mon Jul 05 12:38:12 UTC 2021"); // Date of the second INFO log line.
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);
        assertEquals(2, brooklynLogEntries.size());

        // Check first log line,
        BrooklynLogEntry firstBrooklynLogEntry = brooklynLogEntries.get(0);
        assertEquals("INFO", firstBrooklynLogEntry.getLevel());
        assertEquals("  org.apache.brooklyn.ui.modularity.brooklyn-ui-module-registry/1.1.0.SNAPSHOT", firstBrooklynLogEntry.getMessage());

        // Check second log line.
        BrooklynLogEntry secondBrooklynLogEntry = brooklynLogEntries.get(1);
        assertEquals("INFO", secondBrooklynLogEntry.getLevel());
        assertEquals("registering JasperInitializer", secondBrooklynLogEntry.getMessage());
    }

    @Test
    public void testQueryLogSampleWithLogLevels() {
        File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(JAVA_LOG_SAMPLE_PATH)).getFile());
        ManagementContextInternal mgmt = LocalManagementContextForTests.newInstance();
        mgmt.getBrooklynProperties().put(LOGBOOK_LOG_STORE_PATH.getName(), file.getAbsolutePath());
        LogBookQueryParams logBookQueryParams = new LogBookQueryParams();
        logBookQueryParams.setNumberOfItems(1000); // Request all.
        logBookQueryParams.setTail(false);
        logBookQueryParams.setLevels(ImmutableList.of("INFO", "DEBUG")); // Request INFO and DEBUG levels.
        FileLogStore fileLogStore = new FileLogStore(mgmt);
        List<BrooklynLogEntry> brooklynLogEntries = fileLogStore.query(logBookQueryParams);

        // There is one DEBUG log line and two INFO lines.
        assertEquals(3, brooklynLogEntries.size());

        // Check appearance of log levels
        assertEquals("DEBUG", brooklynLogEntries.get(0).getLevel());
        assertEquals("INFO", brooklynLogEntries.get(1).getLevel());
        assertEquals("INFO", brooklynLogEntries.get(2).getLevel());
    }
}