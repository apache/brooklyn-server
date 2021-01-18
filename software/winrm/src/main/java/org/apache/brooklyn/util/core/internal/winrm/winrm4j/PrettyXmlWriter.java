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
    private Writer wrappedWriter;
    private boolean tagClosed = Boolean.FALSE;
    private int indentLevel = 0;
    private boolean newLine = Boolean.TRUE;
    private char lastChar = '\n';
    private boolean isComment = Boolean.FALSE;

    public PrettyXmlWriter(Writer writer) {
        super(writer);
        wrappedWriter = writer;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        for (int i = off; i < off + len; i++) {
            char c = cbuf[i];
            if (isComment) {
                if (c == '\n') {
                    isComment = false;
                    writeNewLine();
                } else {
                    writeChar(c);
                }
            } else if (newLine && c == '#') {
                isComment = true;
                writeChar(c);
            } else {
                if (c == '<') {
                    lastChar = '<';
                    if (!newLine) {
                        indentLevel--;
                    } else if (i + 1 < off + len && cbuf[i + 1] == '/') {
                        indentLevel--;
                        printIndent();
                        indentLevel--;
                    } else {
                        printIndent();
                    }
                }
                writeChar(c);
                if ('>' == c || tagClosed) {
                    if (i + 1 < off + len) {
                        if (cbuf[i + 1] == '<') {
                            writeNewLine();
                            if (lastChar != '/') {
                                indentLevel++;
                            }
                        }
                    } else {
                        tagClosed = true;
                    }
                } else {
                    tagClosed = false;
                }
                lastChar = c;
            }
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
