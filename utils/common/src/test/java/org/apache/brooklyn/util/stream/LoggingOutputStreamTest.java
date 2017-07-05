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
package org.apache.brooklyn.util.stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class LoggingOutputStreamTest {

    private List<String> logs;
    private Logger mockLogger;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        logs = new ArrayList<>();
        mockLogger = Mockito.mock(Logger.class);
        Mockito.when(mockLogger.isDebugEnabled()).thenReturn(true);
        Mockito.doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
              Object[] args = invocation.getArguments();
              logs.add((String)args[0]);
              return null;
            }
        }).when(mockLogger).debug(Mockito.anyString());
    }
    
    @Test
    public void testCallsDelegateStream() throws Exception {
        ByteArrayOutputStream delegate = new ByteArrayOutputStream();
        LoggingOutputStream out = LoggingOutputStream.builder().outputStream(delegate).build();
        out.write(new byte[] {1,2,3});
        out.flush();
        assertTrue(Arrays.equals(delegate.toByteArray(), new byte[] {1, 2, 3}));
    }
    
    @Test
    public void testNoopIfNoDelegateStream() throws Exception {
        // Just checking that throws no exceptions
        LoggingOutputStream out = LoggingOutputStream.builder().build();
        out.write(new byte[] {1,2,3});
        out.flush();
    }
    
    @Test
    public void testLogsLines() throws Exception {
        LoggingOutputStream out = LoggingOutputStream.builder().logger(mockLogger).build();
        out.write("line1\n".getBytes(StandardCharsets.UTF_8));
        out.write("line2".getBytes(StandardCharsets.UTF_8));
        out.flush();
        
        assertEquals(logs, ImmutableList.of("line1", "line2"));
    }
    
    @Test
    public void testLogsLinesWithPrefix() throws Exception {
        LoggingOutputStream out = LoggingOutputStream.builder().logger(mockLogger).logPrefix("myprefix:").build();
        out.write("line1\n".getBytes(StandardCharsets.UTF_8));
        out.write("line2".getBytes(StandardCharsets.UTF_8));
        out.flush();
        
        assertEquals(logs, ImmutableList.of("myprefix:line1", "myprefix:line2"));
    }
    
    @Test
    public void testLogsUnicode() throws Exception {
        LoggingOutputStream out = LoggingOutputStream.builder().logger(mockLogger).build();
        String test = "Лорем.";
        out.write((test+"\n").getBytes(StandardCharsets.UTF_8));
        out.flush();

        assertEquals(logs, ImmutableList.of(test));
    }
}
