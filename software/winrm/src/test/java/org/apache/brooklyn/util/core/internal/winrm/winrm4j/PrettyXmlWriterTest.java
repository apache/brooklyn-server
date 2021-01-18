package org.apache.brooklyn.util.core.internal.winrm.winrm4j;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import static org.testng.Assert.*;

public class PrettyXmlWriterTest {

    public static final String XML = "<?xml version=\"1.0\" ?><Objs xmlns=\"http://schemas.microsoft.com/powershell/2004/04\" Version=\"1.1.0.1\"><Obj S=\"progress\" RefId=\"0\"><TN RefId=\"0\"><T>System.Management.Automation.PSCustomObject</T><T>System.Object</T></TN><MS><I64 N=\"SourceId\">1</I64><PR N=\"Record\"><AV>Preparing modules for first use.</AV><AI>0</AI><Nil/><PI>-1</PI><PC>-1</PC><T>Completed</T><SR>-1</SR><SD/></PR></MS></Obj></Objs>";
    private RecordingWriter writer;
    private PrettyXmlWriter prettyXmlWriter;

    @BeforeMethod
    public void setUp() {
        writer = new RecordingWriter();
        prettyXmlWriter = new PrettyXmlWriter(writer);
    }

    @Test
    public void testInsertNewLines() throws IOException {
        prettyXmlWriter.write(XML, 0, XML.length());

        int newLines = countNewLines();
        assertEquals(newLines,21);
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

        int newLines = countNewLines();
        assertEquals(newLines,3);
    }

    @Test
    public void testIndentOnNewLine() throws IOException {
        String xml = "<t1><t2>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(writer.out.toString(), "<t1>\n\t<t2>");
    }

    @Test
    public void testIncreaseIndentOnEachOpenTag() throws IOException {
        String xml = "<t1><t2><t3>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(writer.out.toString(), "<t1>\n\t<t2>\n\t\t<t3>");
    }

    @Test
    public void testOutdentBeforeClosingTag() throws IOException {
        String xml = "<t1><t2></t2>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(writer.out.toString(), "<t1>\n\t<t2>\n\t</t2>");
    }

    @Test
    public void testDontIndentAfterClosingTag() throws IOException {
        String xml = "<t1><t2></t2></t1>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(writer.out.toString(), "<t1>\n\t<t2>\n\t</t2>\n</t1>");
    }

    @Test
    public void testDontIndentWithoutNewLine() throws IOException {
        String xml = "<t1><t2><t3>Some Text</t3></t2></t1>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(writer.out.toString(), "<t1>\n\t<t2>\n\t\t<t3>Some Text</t3>\n\t</t2>\n</t1>");
    }

    @Test
    public void testDontIndentSelfClosingTag() throws IOException {
        String xml = "<t1><t2><t3/></t2></t1>";
        prettyXmlWriter.write(xml, 0, xml.length());

        assertEquals(writer.out.toString(), "<t1>\n\t<t2>\n\t\t<t3/>\n\t</t2>\n</t1>");
    }

    private int countNewLines() {
        int newLines = 0;
        String s = writer.out.toString();
        for (char c : s.toCharArray()) {
            if(c=='\n') newLines++;
        }
        return newLines;
    }

    static class RecordingWriter extends Writer {

        StringBuffer out = new StringBuffer();

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