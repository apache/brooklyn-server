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

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.Charset;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class StreamGobblerTest {

    private static final Logger LOG = LoggerFactory.getLogger(StreamGobblerTest.class);
    private static final String TEST_CHARSET_NAME =
            "UTF-8";
//            "US-ASCII";  // fails

    private static final Charset TEST_CHARSET = Charset.forName(TEST_CHARSET_NAME);

    private final String NL = Os.LINE_SEPARATOR;

    private void testStreamGobbler(String text) throws Exception {
        text = text.replace(""+StreamGobbler.REPLACEMENT_CHARACTER, "_");

        LOG.info("Processing text: '{}'", text);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(text.getBytes(TEST_CHARSET));
        StreamGobbler streamGobbler = new StreamGobbler(in, out, (Logger) null);
        streamGobbler.start();
        streamGobbler.join(5000);
        streamGobbler.close();
        out.close();

        // approximate regex-- might not work for all whitespace combos
        String expected = Strings.isBlank(text) ? "" : text.replace("\t\r","\r").replaceAll("\r","\n") + NL;
        Assert.assertEquals(out.toString(TEST_CHARSET_NAME), expected);
    }

    @Test
    public void testUnicodeString() throws Exception {

        // empty
        testStreamGobbler("");

        // single chars
        testStreamGobbler(" "); // 1 byte char
        testStreamGobbler("ß"); // 2 bytes char
        testStreamGobbler("√"); // 3 bytes char
        testStreamGobbler("𑱣"); // 4 bytes char

        // duplicate chars
        testStreamGobbler("  "); // 2 x (1 byte char)
        testStreamGobbler("ßß"); // 2 x (2 bytes char)
        testStreamGobbler("√√"); // 2 x (3 bytes char)
        testStreamGobbler("𑱣𑱣"); // 2 x (4 bytes char)

        // mixed text
        testStreamGobbler("옖ʧ񆑮");
        testStreamGobbler("aßc√qq1!");
        testStreamGobbler("옖ʧ񆑮\t롬㟦密䕎孓");
        testStreamGobbler("їїх\rхфт шф9в 0-ф");
        testStreamGobbler("їїх\t\rхфт шф9в 0-ф");
        testStreamGobbler("a ßßa√√aˆa©aƒa∫a˚\na˙a¬a∆a¥a®a†a.  √");
        testStreamGobbler("å¨¨∫√çˆˆø¨¨\0iubxo𑱣qpihbpπ∫ˆ¨¨øß†a");
        testStreamGobbler(" oubibosu√bfhf иіашвщ, гирф𑱣ііззфххіхіїїх. цйїхз/йї звохй отв 90320к4590е- †a");

        // random text
        testStreamGobbler(RandomStringUtils.random(999));
    }

    @Test
    public void testGobbleStream() throws Exception {
        byte[] bytes = new byte[] {'a','b','c'};
        InputStream stream = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamGobbler gobbler = new StreamGobbler(stream, out, null);
        gobbler.start();
        try {
            gobbler.join(10*1000);
            assertFalse(gobbler.isAlive());
            assertEquals(new String(out.toByteArray()), "abc" + NL);
        } finally {
            gobbler.close();
            gobbler.interrupt();
        }
    }
    
    @Test
    public void testGobbleMultiLineBlockingStream() throws Exception {
        PipedOutputStream pipedOutputStream = new PipedOutputStream();
        PipedInputStream stream = new PipedInputStream(pipedOutputStream);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamGobbler gobbler = new StreamGobbler(stream, out, null);
        gobbler.start();
        try {
            pipedOutputStream.write("line1\n".getBytes());
            pipedOutputStream.flush();
            assertEqualsEventually(out, "line1" + NL);

            pipedOutputStream.write("line2\n".getBytes());
            pipedOutputStream.flush();
            assertEqualsEventually(out, "line1" + NL + "line2" + NL);

            pipedOutputStream.write("line".getBytes());
            pipedOutputStream.write("3\n".getBytes());
            pipedOutputStream.flush();
            assertEqualsEventually(out, "line1" + NL + "line2" + NL + "line3" + NL);

            pipedOutputStream.close();
            
            gobbler.join(10*1000);
            assertFalse(gobbler.isAlive());
            assertEquals(new String(out.toByteArray()), "line1" + NL + "line2" + NL + "line3" + NL);
        } finally {
            gobbler.close();
            gobbler.interrupt();
        }
    }
    
    private void assertEqualsEventually(final ByteArrayOutputStream out, final String expected) {
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(new String(out.toByteArray()), expected);
            }});
    }
}
