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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

/**
 * Wraps another output stream, intercepting the writes to log it.
 */
public class LoggingOutputStream extends FilterOutputStream {

    private static final OutputStream NOOP_OUTPUT_STREAM = new FilterOutputStream(null) {
        @Override public void write(int b) throws IOException {
        }
        @Override public void flush() throws IOException {
        }
        @Override public void close() throws IOException {
        }        
    };
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        OutputStream out;
        Logger log;
        String logPrefix;
        
        public Builder outputStream(OutputStream val) {
            this.out = val;
            return this;
        }
        public Builder logger(Logger val) {
            this.log = val;
            return this;
        }
        public Builder logPrefix(String val) {
            this.logPrefix = val;
            return this;
        }
        public LoggingOutputStream build() {
            return new LoggingOutputStream(this);
        }
    }
    
    protected final Logger log;
    protected final String logPrefix;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final StringBuilder lineSoFar = new StringBuilder(16);

    private LoggingOutputStream(Builder builder) {
        super(builder.out != null ? builder.out : NOOP_OUTPUT_STREAM);
        log = builder.log;
        logPrefix = (builder.logPrefix != null) ? builder.logPrefix : "";
      }

    @Override
    public void write(int b) throws IOException {
        if (running.get() && b >= 0) onChar(b);
        out.write(b);
    }

    @Override
    public void flush() throws IOException {
        try {
            if (lineSoFar.length() > 0) {
                onLine(lineSoFar.toString());
                lineSoFar.setLength(0);
            }
        } finally {
            super.flush();
        }
    }
    
    // Overriding close() because FilterOutputStream's close() method pre-JDK8 has bad behavior:
    // it silently ignores any exception thrown by flush(). Instead, just close the delegate stream.
    // It should flush itself if necessary.
    @Override
    public void close() throws IOException {
        try {
            onLine(lineSoFar.toString());
            lineSoFar.setLength(0);
        } finally {
            out.close();
            running.set(false);
        }
    }
    
    public void onChar(int c) {
        if (c=='\n' || c=='\r') {
            if (lineSoFar.length()>0)
                //suppress blank lines, so that we can treat either newline char as a line separator
                //(eg to show curl updates frequently)
                onLine(lineSoFar.toString());
            lineSoFar.setLength(0);
        } else {
            lineSoFar.append((char)c);
        }
    }
    
    public void onLine(String line) {
        //right trim, in case there is \r or other funnies
        while (line.length()>0 && Character.isWhitespace(line.charAt(line.length()-1)))
            line = line.substring(0, line.length()-1);
        //right trim, in case there is \r or other funnies
        while (line.length()>0 && (line.charAt(0)=='\n' || line.charAt(0)=='\r'))
            line = line.substring(1);
        if (!line.isEmpty()) {
            if (log!=null && log.isDebugEnabled()) log.debug(logPrefix+line);
        }
    }
}
