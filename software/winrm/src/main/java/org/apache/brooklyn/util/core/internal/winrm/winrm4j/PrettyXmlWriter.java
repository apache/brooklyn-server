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

public class PrettyXmlWriter extends Writer {
    private final Writer wrappedWriter;
    private boolean tagClosed = false;
    private int indentLevel = 0;
    private boolean newLine = true;
    private char lastChar = '\n';
    private boolean isComment = false;

    public PrettyXmlWriter(Writer writer) {
        super(writer);
        wrappedWriter = writer;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        for (int i = off; i < off + len; i++) {
            char c = cbuf[i];
            if (isComment) {
                // Currently in a comment (weird MS comment not xml comment)
                if (c == '\n') {
                    isComment = false;
                    writeNewLine();
                } else {
                    writeChar(c);
                }
            } else if (newLine && c == '#') {
                // MS CLI XML uses this to start a comment
                isComment = true;
                writeChar(c);
            } else {
                //Not a comment - treat as XML
                handleMeaningfulChar(cbuf, i, c, off + len - 1);
            }
        }
    }

    private void handleMeaningfulChar(char[] cbuf, int i, char c, int end) throws IOException {
        if (tagClosed) {
            // Tag was closed on last call to write so need a new line
            writeNewLine();
            tagClosed = false;
        }
        if (c == '<') {
            // Start if a tag
            if (!newLine) {
                // Assume closing tag following text - e.g. <t>some text</t>
                indentLevel--;
            } else if (i < end && cbuf[i + 1] == '/') {
                // Closing tag
                indentLevel--;
                printIndent();
                indentLevel--;
            } else {
                //
                printIndent();
            }
        }
        writeChar(c);
        if ('>' == c ) {
            if (i < end) {
                if (cbuf[i + 1] == '<') {
                    writeNewLine();
                    increaseIndentIfNotSelfClosingTag();
                }
            } else {
                // We've closed a tag but don't know what the next character is
                tagClosed = true;
                increaseIndentIfNotSelfClosingTag();
            }
        }
        lastChar = c;
    }

    private void increaseIndentIfNotSelfClosingTag() {
        if (lastChar != '/') {
            indentLevel++;
        }
    }

    private void writeNewLine() throws IOException {
        wrappedWriter.write('\n');
        newLine = true;
    }

    private void writeChar(char c) throws IOException {
        wrappedWriter.write(c);
        newLine = false;
    }

    private void printIndent() throws IOException {
        for (int j = 0; j < indentLevel; j++) {
            wrappedWriter.write('\t');
        }
    }

    @Override
    public void flush() throws IOException {
        wrappedWriter.flush();
    }

    @Override
    public void close() throws IOException {
        wrappedWriter.close();
    }
}
