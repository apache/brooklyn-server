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
package org.apache.brooklyn.util.core.internal.winrm.winrm4j;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import static org.testng.Assert.*;

public class PrettyXmlWriterTest {

    public static final String XML = "<Objs xmlns=\"http://schemas.microsoft.com/powershell/2004/04\" Version=\"1.1.0.1\"><Obj S=\"progress\" RefId=\"0\"><TN RefId=\"0\"><T>System.Management.Automation.PSCustomObject</T><T>System.Object</T></TN><MS><I64 N=\"SourceId\">1</I64><PR N=\"Record\"><AV>Preparing modules for first use.</AV><AI>0</AI><Nil/><PI>-1</PI><PC>-1</PC><T>Completed</T><SR>-1</SR><SD/></PR></MS></Obj></Objs>";
    public static final String REAL_XML = "#< CLIXML\n" +
            "<Objs Version=\"1.1.0.1\" xmlns=\"http://schemas.microsoft.com/powershell/2004/04\"><Obj S=\"progress\" RefId=\"0\"><TN RefId=\"0\"><T>System.Management.Automation.PSCustomObject</T><T>System.Object</T></TN><MS><I64 N=\"SourceId\">1</I64><PR N=\"Record\"><AV>Preparing modules for first use.</AV><AI>0</AI><Nil /><PI>-1</PI><PC>-1</PC><T>Completed</T><SR>-1</SR><SD> </SD></PR></MS></Obj><Obj S=\"progress\" RefId=\"1\"><TNRef RefId=\"0\" /><MS><I64 N=\"SourceId\">1</I64><PR N=\"Record\"><AV>Preparing modules for first use.</AV><AI>0</AI><Nil /><PI>-1</PI><PC>-1</PC><T>Completed</T><SR>-1</SR><SD> </SD></PR></MS></Obj><Obj S=\"information\" RefId=\"2\"><TN RefId=\"1\"><T>System.Management.Automation.InformationRecord</T><T>System.Object</T></TN><ToString>hello from start</ToString><Props><Obj N=\"MessageData\" RefId=\"3\"><TN RefId=\"2\"><T>System.Management.Automation.HostInformationMessage</T><T>System.Object</T></TN><ToString>hello from start</ToString><Props><S N=\"Message\">hello from start</S><B N=\"NoNewLine\">false</B><S N=\"ForegroundColor\">Gray</S><S N=\"BackgroundColor\">Black</S></Props></Obj><S N=\"Source\">C:\\Users\\Administrator\\.brooklyn-tosca-execution\\hzq1jaa0m4\\ldkdm2w4mq\\20210118-120841565-tosca.interfaces.node.lifecycle.Standard.start\\effector.ps1</S><DT N=\"TimeGenerated\">2021-01-18T19:08:50.3727132+00:00</DT><Obj N=\"Tags\" RefId=\"4\"><TN RefId=\"3\"><T>System.Collections.Generic.List`1[[System.String, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]]</T><T>System.Object</T></TN><LST><S>PSHOST</S></LST></Obj><S N=\"User\">MYWIN-01\\Administrator</S><S N=\"Computer\">mywin-01</S><U32 N=\"ProcessId\">3864</U32><U32 N=\"NativeThreadId\">1912</U32><U32 N=\"ManagedThreadId\">10</U32></Props></Obj></Objs>";
    private RecordingWriter recordingWriter;
    private PrettyXmlWriter prettyXmlWriter;

    @BeforeMethod
    public void setUp() {
        recordingWriter = new RecordingWriter();
        prettyXmlWriter = new PrettyXmlWriter(recordingWriter);
    }

    @Test
    public void testREAL() throws IOException {
        prettyXmlWriter.write(REAL_XML, 0, REAL_XML.length());
        prettyXmlWriter.flush();

        String s = recordingWriter.out.toString();
        System.out.println(s);
    }

    @Test
    public void testInsertNewLines() throws IOException {
        prettyXmlWriter.write(XML, 0, XML.length());

        int newLines = countNewLines();
        assertEquals(newLines,20);
    }

    @Test
    public void testNoNewLineIfTagSurroundsText() throws IOException {
        String xml = "<tag>fdskljdfsljkfsd</tag>";
        prettyXmlWriter.write(xml, 0, xml.length());

        int newLines = countNewLines();
        assertEquals(newLines,0);
    }

    @Test
    public void testNewLineIfPartialString() throws  IOException {
        String xmlPart1 = "<t1><t2>fdskljfds</t2>";
        String xmlPart2 = "<t3>djskdsjk</t3></t1>";

        prettyXmlWriter.write(xmlPart1, 0, xmlPart1.length());
        prettyXmlWriter.write(xmlPart2, 0, xmlPart2.length());

        assertEquals(recordingWriter.out.toString(), "<t1>\n\t<t2>fdskljfds</t2>\n\t<t3>djskdsjk</t3>\n</t1>");
    }

    @Test
    public void testIndentOnNewLine() throws IOException {
        String xml = "<t1><t2>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(recordingWriter.out.toString(), "<t1>\n\t<t2>");
    }

    @Test
    public void testIncreaseIndentOnEachOpenTag() throws IOException {
        String xml = "<t1><t2><t3>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(recordingWriter.out.toString(), "<t1>\n\t<t2>\n\t\t<t3>");
    }

    @Test
    public void testOutdentBeforeClosingTag() throws IOException {
        String xml = "<t1><t2></t2>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(recordingWriter.out.toString(), "<t1>\n\t<t2>\n\t</t2>");
    }

    @Test
    public void testDontIndentAfterClosingTag() throws IOException {
        String xml = "<t1><t2></t2></t1>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(recordingWriter.out.toString(), "<t1>\n\t<t2>\n\t</t2>\n</t1>");
    }

    @Test
    public void testDontIndentWithoutNewLine() throws IOException {
        String xml = "<t1><t2><t3>Some Text</t3></t2></t1>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(recordingWriter.out.toString(), "<t1>\n\t<t2>\n\t\t<t3>Some Text</t3>\n\t</t2>\n</t1>");
    }

    @Test
    public void testDontIndentSelfClosingTag() throws IOException {
        String xml = "<t1><t2><t3/></t2></t1>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(recordingWriter.out.toString(), "<t1>\n\t<t2>\n\t\t<t3/>\n\t</t2>\n</t1>");
    }

    @Test
    public void testIgnoreComment() throws IOException {
       String xml = "#< CLIXML\n<t1><t2><t3/></t2></t1>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(recordingWriter.out.toString(), "#< CLIXML\n<t1>\n\t<t2>\n\t\t<t3/>\n\t</t2>\n</t1>");
    }

    private int countNewLines() {
        int newLines = 0;
        String s = recordingWriter.out.toString();
        for (char c : s.toCharArray()) {
            if(c=='\n') newLines++;
        }
        return newLines;
    }

    static class RecordingWriter extends Writer {

        StringBuilder out = new StringBuilder();

        @Override
        public void write(int c) throws IOException {
            out.append((char) c);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            out.append(Arrays.copyOfRange(cbuf, off, off + len));
        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}