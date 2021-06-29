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

import junit.framework.TestCase;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;
import org.junit.Test;

public class FileLogStoreTest extends TestCase {
    private final String JAVA_LOG_LINE = "2021-05-27T11:36:59,251 - DEBUG 146 o.a.b.c.m.i.LocalManagementContext [qtp158784971-235] Top-level effector invocation: restart[] on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}";
    private final String JAVA_LOG_LINE_WITH_EXTRA_SPACE = "2021-06-07T14:58:58,487 - INFO    6 o.o.p.l.s.s.EventAdminConfigurationNotifier [s4j.pax.logging)] Sending Event Admin notification (configuration successful) to org/ops4j/pax/logging/Configuration";
    private final String TASK_LOG_LINE = "2021-05-27T11:36:59,258 OGObOWJs-[gwpndj09r8] DEBUG 146 o.a.b.c.m.i.EffectorUtils [ager-WgxriwjB-43] Invoking effector restart on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}";

    @Test
    public void testParseLogJavaLine() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(JAVA_LOG_LINE);
        assertNull(brooklynLogEntry.getTaskId());
        assertNull(brooklynLogEntry.getEntityIds());
        assertEquals("2021-05-27T11:36:59,251", brooklynLogEntry.getTimestampString());
        assertEquals("DEBUG", brooklynLogEntry.getLevel());
        assertEquals("146", brooklynLogEntry.getBundleId());
        assertEquals("o.a.b.c.m.i.LocalManagementContext", brooklynLogEntry.getClazz());
        assertEquals("qtp158784971-235", brooklynLogEntry.getThreadName());
        assertEquals("Top-level effector invocation: restart[] on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}", brooklynLogEntry.getMessage());
    }

    @Test
    public void testParseLogJavaLineWithExtraSpace() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(JAVA_LOG_LINE_WITH_EXTRA_SPACE);
        assertNull(brooklynLogEntry.getTaskId());
        assertNull(brooklynLogEntry.getEntityIds());
        assertEquals("2021-06-07T14:58:58,487", brooklynLogEntry.getTimestampString());
        assertEquals("INFO", brooklynLogEntry.getLevel());
        assertEquals("6", brooklynLogEntry.getBundleId());
        assertEquals("o.o.p.l.s.s.EventAdminConfigurationNotifier", brooklynLogEntry.getClazz());
        assertEquals("s4j.pax.logging)", brooklynLogEntry.getThreadName());
        assertEquals("Sending Event Admin notification (configuration successful) to org/ops4j/pax/logging/Configuration", brooklynLogEntry.getMessage());
    }

    @Test
    public void testParseLogTaskLine() {
        FileLogStore cut = new FileLogStore();
        BrooklynLogEntry brooklynLogEntry = cut.parseLogLine(TASK_LOG_LINE);
        assertEquals("2021-05-27T11:36:59,258", brooklynLogEntry.getTimestampString());
        assertEquals("OGObOWJs", brooklynLogEntry.getTaskId());
        assertEquals("[gwpndj09r8]", brooklynLogEntry.getEntityIds());
        assertEquals("DEBUG", brooklynLogEntry.getLevel());
        assertEquals("146", brooklynLogEntry.getBundleId());
        assertEquals("o.a.b.c.m.i.EffectorUtils", brooklynLogEntry.getClazz());
        assertEquals("ager-WgxriwjB-43", brooklynLogEntry.getThreadName());
        assertEquals("Invoking effector restart on BasicApplicationImpl{id=gwpndj09r8, name=Application (gwpndj09r8)}", brooklynLogEntry.getMessage());
    }
}