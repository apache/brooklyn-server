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

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import org.apache.brooklyn.util.core.internal.winrm.winrm4j.PrettyXmlWriterTest.RecordingWriter;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ErrorXmlWriterTest {

    public static final String XML_SMALL = "\n<S S=\"error\">Some error</S>\n#foo \n<S S=\"other\">not-error</S>\n\n";
    public static final String NOT_XML = "# foo\n\nbar";

    public static final String XML_BIG = "#< CLIXML\n" +
            "<Objs Version=\"1.1.0.1\" xmlns=\"http://schemas.microsoft.com/powershell/2004/04\">\n" +
            "\t<Obj S=\"progress\" RefId=\"0\">\n" +
            "\t\t<TN RefId=\"0\">\n" +
            "\t\t\t<T>System.Management.Automation.PSCustomObject</T>\n" +
            "\t\t\t<T>System.Object</T>\n" +
            "\t\t</TN>\n" +
            "\t\t<MS>\n" +
            "\t\t\t<I64 N=\"SourceId\">1</I64>\n" +
            "\t\t\t<PR N=\"Record\">\n" +
            "\t\t\t\t<AV>Preparing modules for first use.</AV>\n" +
            "\t\t\t\t<AI>0</AI>\n" +
            "\t\t\t\t<Nil />\n" +
            "\t\t\t\t<PI>-1</PI>\n" +
            "\t\t\t\t<PC>-1</PC>\n" +
            "\t\t\t\t<T>Completed</T>\n" +
            "\t\t\t\t<SR>-1</SR>\n" +
            "\t\t\t\t<SD> </SD>\n" +
            "\t\t\t</PR>\n" +
            "\t\t</MS>\n" +
            "\t</Obj>\n" +
            "\t<Obj S=\"progress\" RefId=\"1\">\n" +
            "\t\t<TNRef RefId=\"0\" />\n" +
            "\t\t<MS>\n" +
            "\t\t\t<I64 N=\"SourceId\">1</I64>\n" +
            "\t\t\t<PR N=\"Record\">\n" +
            "\t\t\t\t<AV>Preparing modules for first use.</AV>\n" +
            "\t\t\t\t<AI>0</AI>\n" +
            "\t\t\t\t<Nil />\n" +
            "\t\t\t\t<PI>-1</PI>\n" +
            "\t\t\t\t<PC>-1</PC>\n" +
            "\t\t\t\t<T>Completed</T>\n" +
            "\t\t\t\t<SR>-1</SR>\n" +
            "\t\t\t\t<SD> </SD>\n" +
            "\t\t\t</PR>\n" +
            "\t\t</MS>\n" +
            "\t</Obj>\n" +
            "\t<S S=\"error\">NEEDLE</S>"+
            "\t<S S=\"error\">Found\n\n</S>";

    private RecordingWriter recordingWriter;
    private ErrorXmlWriter xmlWriter;

    @BeforeMethod
    public void setUp() {
        recordingWriter = new RecordingWriter();
        xmlWriter = new ErrorXmlWriter(recordingWriter);
    }

    @Test
    public void testSmall() throws IOException {
        xmlWriter.write(XML_SMALL, 0, XML_SMALL.length());
        xmlWriter.flush();

        String s = recordingWriter.out.toString();
        Assert.assertEquals(s, "Some error\n");
    }

    @Test
    public void testNotXml() throws IOException {
        xmlWriter.write(NOT_XML, 0, NOT_XML.length());
        xmlWriter.flush();

        String s = recordingWriter.out.toString();
        Assert.assertEquals(s, "bar");
    }

    @Test
    public void testBig() throws IOException {
        xmlWriter.write(XML_BIG, 0, XML_BIG.length());
        xmlWriter.flush();

        String s = recordingWriter.out.toString();
        Assert.assertEquals(s, "NEEDLE\nFound\n\n");
    }

}