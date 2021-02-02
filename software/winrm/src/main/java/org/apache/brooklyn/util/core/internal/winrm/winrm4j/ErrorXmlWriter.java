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
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Filters XML output for any S tag with an S attribute of "error" and extracts the text, one per line.
 * Or if any error or not XML, switches to pass through.
 *
 * Supports streaming, so viewers get output as quickly as possible. Accepts # comment lines.
 * But otherwise very rough-and-ready! */
public class ErrorXmlWriter extends Writer {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorXmlWriter.class);

    private final Writer wrappedWriter;

    // means we give up, probably not xml, just echo
    private boolean passThrough;
    private boolean waitingForContent = true;

    private boolean inTag;

    private boolean inTagName;
    private String tag;
    private boolean thisTagIsAnEndTag;
    private boolean thisTagIsSelfClosing;

    private boolean inAttribute;
    private String attribute;

    private boolean inValue;
    private String value;

    private boolean inComment;

    private String textWrittenHere;
    private boolean textAllowedHere;

    private boolean isLastCharBackslashEscaped;
    private boolean isLastCharLineStart;
    private boolean cacheLastCharLineStart;

    public ErrorXmlWriter(Writer writer) {
        super(writer);
        wrappedWriter = writer;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        char c;
        cacheLastCharLineStart = true;
        for (int i = off; i < off + len; i++) {
            isLastCharLineStart = cacheLastCharLineStart;
            c = cbuf[i];
            cacheLastCharLineStart = (c=='\n');

            if (passThrough) {
                writeChar(c);
                continue;
            }

            try {
                if (inComment) {
                    // Currently in a comment (weird MS comment not xml comment)
                    if (c == '\n') {
                        inComment = false;
                    }
                    continue;
                }

                if (isLastCharLineStart && c == '#') {
                    inComment = true;
                    continue;
                }

                if (inTag) {
                    if (inTagName) {
                        if (c == ' ') {
                            if (!tag.isEmpty()) {
                                onTagBegin(tag);
                                inAttribute = true;
                                attribute = "";
                            } else {
                                // ignore
                            }
                        } else if (c == '>') {
                            onTagBegin(tag);
                            processTagFinished(tag);
                        } else if (c == '/') {
                            // nothing
                            if (tag.isEmpty()) {
                                thisTagIsAnEndTag = true;
                            } else {
                                onTagBegin(tag);
                            }

                        } else {
                            tag = tag + c;
                        }
                        continue;
                    }


                    if (inAttribute) {
                        if (c == '=') {
                            inAttribute = false;
                            inValue = true;
                            value = "";
                            continue;
                        } else if (c == ' ') {
                            if (Strings.isBlank(attribute)) {
                                // skip
                            } else {
                                inAttribute = false;
                            }
                        } else if (c == '>') {
                            inAttribute = false;
                            inTag = false;
                        } else if (c == '/') {
                            // nothing
                        } else {
                            attribute = attribute + c;
                        }
                        continue;
                    }

                    if (inValue) {
                        if (c == '\\') {
                            isLastCharBackslashEscaped = true;
                            continue;
                        }
                        if (isLastCharBackslashEscaped) {
                            isLastCharBackslashEscaped = false;
                            value += "\\" + c;
                            continue;
                        }
                        if (c == '"') {
                            if (value.trim().isEmpty()) {
                                // string start
                                value += c;
                                continue;
                            } else {
                                // string end
                                value += c;
                                processAttributeValue(attribute, value, tag);
                                attribute = null;
                                value = null;
                                inValue = false;
                                continue;
                            }
                        }
                        value = value + c;
                        continue;
                    }

                    if (c == '=') {
                        inValue = true;
                        value = "";
                        continue;
                    }

                    if (c == '\n') {
                        continue;
                    }

                    if (c == ' ') {
                        continue;
                    }

                    if (c == '>') {
                        processTagFinished(tag);
                        continue;
                    }

                    // otherwise it starts a new attribute
                    inAttribute = true;
                    attribute = "";

                    continue;
                }

                if (c == '<') {
                    waitingForContent = false;
                    thisTagIsSelfClosing = false;
                    thisTagIsAnEndTag = false;
                    inTag = true;
                    inTagName = true;
                    thisTagIsAnEndTag = false;
                    tag = "";
                    endTextAllowed();
                    continue;
                }

                if (textAllowedHere) {
                    // could add to textWrittenHere here
                    writeChar(c);
                }

                if (waitingForContent && !Character.isWhitespace(c)) {
                    // not xml
                    writeChar(c);
                    passThrough = true;
                    continue;
                }

            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                LOG.debug("Error trying to process WinRM output as XML; switching to pass-through: "+e, e);
                passThrough = true;
            }
        }
    }

    private void endTextAllowed() throws IOException {
        if (textAllowedHere && !isLastCharLineStart) {
            writeChar('\n');
        }
        textWrittenHere = null;
        textAllowedHere = false;
    }

    private void onTagBegin(String tag) {
        // might be start or end tag, we don't track that.  we don't need this at all actually currently but maybe one day
//        LOG.info(tag+":");
        inTagName = false;
    }

    private void processAttributeValue(String attribute, String value, String tag) {
//        LOG.info(tag+" @"+attribute+ " = "+value);
        if ("S".equalsIgnoreCase(attribute) && "\"error\"".equalsIgnoreCase(value.trim())) {
            textAllowedHere = true;
            textWrittenHere = "";
        }
    }

    private void processTagFinished(String tag) {
//        if (thisTagIsAnEndTag) LOG.info("/"+tag);
//        else if (thisTagIsSelfClosing) LOG.info("  "+tag+"/");
//        else LOG.info("  .");
        inTag = false;
    }

    String buffered = null;
    private void writeChar(char c) throws IOException {
        if (buffered !=null) {
            buffered += c;
            if (c=='_') {
                if ("_x000A_".equalsIgnoreCase(buffered)) {
                    wrappedWriter.write('\n');
                    cacheLastCharLineStart = true;
                } else if ("_x000D_".equalsIgnoreCase(buffered)) {
                    // suppress
                } else {
                    wrappedWriter.write(buffered);
                }
                buffered = null;
            }
            return;
        }
        if (c=='_') {
            buffered = ""+c;
            return;
        }

        wrappedWriter.write(c);
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
