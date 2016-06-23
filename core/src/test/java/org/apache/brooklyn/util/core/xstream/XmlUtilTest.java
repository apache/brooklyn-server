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
package org.apache.brooklyn.util.core.xstream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.brooklyn.util.core.xstream.XmlUtil.Escaper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class XmlUtilTest {

    /*
     * For invalid unicode characters, see https://issues.apache.org/jira/browse/BROOKLYN-305 and 
     * https://en.wikipedia.org/wiki/Valid_characters_in_XML.
     *
     * If we include the xml version in the xml string, then more unicode characters are accepted.
     * But if it still contains something illegal, then we can resort to calling
     * xpathHandlingIllegalChars (to temporarily escape those characters
     * 
     */

    private static final Logger LOG = LoggerFactory.getLogger(XmlUtilTest.class);
    
    @Test
    public void testXpath() throws Exception {
        String xml = "<a><b>myb</b></a>";
        for (int i = 0; i < 2; i++) {
            assertEquals(XmlUtil.xpath(xml, "/a/b[text()]"), "myb");
        }
    }

    @Test
    public void testXpathWithEscapedCharsAndXmlVersion1_1() throws Exception {
        StringBuilder xml = new StringBuilder("<?xml version=\"1.1\" encoding=\"UTF-8\"?>"+"\n"+
                "<a><b>myb</b><c>");
        for (int i = 0; i < Integer.valueOf("FFFF", 16); i++) {
            if (isValidUnicodeInXml1_1(i)) {
                String unicode = Integer.toHexString(i);
                xml.append("&#x"+unicode+";");
            }
        }
        xml.append("</c></a>");
        assertEquals(XmlUtil.xpath(xml.toString(), "/a/b[text()]"), "myb");
    }

    @Test
    public void testXpathWithEscapedCharsAndXmlUnversioned() throws Exception {
        StringBuilder xml = new StringBuilder("<a><b>myb</b><c>");
        for (int i = 0; i < Integer.valueOf("FFFF", 16); i++) {
            if (isValidUnicodeInXml1_0(i)) {
                String unicode = Integer.toHexString(i);
                xml.append("&#x"+unicode+";");
            }
        }
        xml.append("</c></a>");
        assertEquals(XmlUtil.xpath(xml.toString(), "/a/b[text()]"), "myb");
    }

    @Test
    public void testXpathHandlingIllegalChars() throws Exception {
        StringBuilder xml = new StringBuilder("<a><b>myb</b><c>");
        for (int i = 0; i < Integer.valueOf("FFFF", 16); i++) {
            String unicode = Integer.toHexString(i);
            xml.append("&#x"+unicode+";");
        }
        xml.append("</c></a>");
        assertEquals(XmlUtil.xpathHandlingIllegalChars(xml.toString(), "/a/b[text()]"), "myb");
    }

    @Test
    public void testEscaper() throws Exception {
        // Escapes unicode char, ignoring things around it
        assertEscapeAndUnescape("&#x001b;", "BR_UNICODE_x001b;");
        assertEscapeAndUnescape("&#00027;", "BR_UNICODE_00027;");
        assertEscapeAndUnescape("&#x1b;", "BR_UNICODE_x1b;");
        assertEscapeAndUnescape("&#x1b;SUFFIX", "BR_UNICODE_x1b;SUFFIX");
        assertEscapeAndUnescape("PREFIX&#x1b;", "PREFIXBR_UNICODE_x1b;");
        assertEscapeAndUnescape("PREFIX&#x1b;SUFFIX", "PREFIXBR_UNICODE_x1b;SUFFIX");

        // Ignores invalid unicodes
        assertEscapeAndUnescape("&#x001b", "&#x001b"); // no ";"
        assertEscapeAndUnescape("&#x001g;", "&#x001g;"); // chars out of range
        assertEscapeAndUnescape("&#x001bb;", "&#x001bb;"); // too many chars

        // Handles strings that already contain the expected escape sequence
        assertEscapeAndUnescape("BR_UNICODE_x1b;", "NOT_BR_UNICODE_x1b;");
        assertEscapeAndUnescape("NOT_BR_UNICODE_x1b;", "NOT_NOT_BR_UNICODE_x1b;");
        assertEscapeAndUnescape("BR_UNICODE_x1b;THEN&#x1b;", "NOT_BR_UNICODE_x1b;THENBR_UNICODE_x1b;");
    }

    protected void assertEscapeAndUnescape(String val) {
        assertEscapeAndUnescape(val, Optional.<String>absent());
    }

    protected void assertEscapeAndUnescape(String val, String expectedEscaped) {
        assertEscapeAndUnescape(val, Optional.of(expectedEscaped));
    }
    
    protected void assertEscapeAndUnescape(String val, Optional<String> expectedEscaped) {
        Escaper escaper = new XmlUtil.Escaper();
        String escaped = escaper.escape(val);
        if (expectedEscaped.isPresent()) {
            assertEquals(escaped, expectedEscaped.get());
        }
        assertEquals(escaper.unescape(escaped), val);
    }
    
    @Beta
    protected boolean isValidUnicodeInXml1_1(int c) {
        int min = 0;
        int max = 65535; // xFFFF
        return min <= c && c <= max &&
                c != 0 && (c <= 55295 || c >= 57344) && c != 65534;
    }

    @Beta
    protected boolean isValidUnicodeInXml1_0(int c) {
        // see https://www.w3.org/TR/xml/#charsets
        return c == 0x9 || c == 0xA || c == 0xD || (0x20 <= c && c <= 0xD7FF) 
                || (0xE000 <= c && c <= 0xFFFD) || (0x10000 <= c && c <= 0x10FFFF);
    }

    // For checking our assumptions about what is a valid and invalid (escaped) unicode char
    // in XML version 1.0 and 1.1.
    @Test(groups={"WIP"}, enabled=false)
    public void testXpathWithEachEscapeCharacterAndXmlVersion() throws Exception {
        List<Integer> errsInXml1_1 = Lists.newArrayList();
        List<Integer> errsInXmlUnversioned = Lists.newArrayList();
        for (int i = 0; i < Integer.valueOf("FFFF", 16); i++) {
            String unicode = Integer.toHexString(i);
            try {
                String xml = Joiner.on("\n").join(
                        "<?xml version=\"1.1\" encoding=\"UTF-8\"?>",
                        "<a><b>myb</b><c>&#x"+unicode+";</c></a>");
                assertEquals(XmlUtil.xpath(xml, "/a/b[text()]"), "myb");
                assertTrue(isValidUnicodeInXml1_1(i), "i="+i+"; unicode="+unicode);
            } catch (Throwable t) {
                LOG.debug("Failed for code "+unicode+": "+t.getMessage());
                errsInXml1_1.add(Integer.valueOf(unicode, 16));
                assertFalse(isValidUnicodeInXml1_1(i), "i="+i+"; unicode="+unicode);
            }
            try {
                String xml = "<a><b>myb</b><c>&#x"+unicode+";</c></a>";
                assertEquals(XmlUtil.xpath(xml, "/a/b[text()]"), "myb");
                assertTrue(isValidUnicodeInXml1_0(i), "i="+i+"; unicode="+unicode);
            } catch (Throwable t) {
                LOG.debug("Failed for code "+unicode+": "+t.getMessage());
                errsInXmlUnversioned.add(Integer.valueOf(unicode, 16));
                assertFalse(isValidUnicodeInXml1_0(i), "i="+i+"; unicode="+unicode);
            }
        }
        LOG.info("XML version 1.1 invalidCodes="+errsInXml1_1);
        LOG.info("XML unversioned invalidCodes="+errsInXmlUnversioned);
    }
}
