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

import java.io.*;
import java.nio.charset.Charset;
import java.util.function.Supplier;
import org.apache.brooklyn.util.exceptions.Exceptions;

/** Creates a new {@link java.io.InputStream} on every access.  Caller's repsonsibility to close. */
public abstract class InputStreamSource implements Supplier<InputStream> {

    private final String name;

    public static InputStreamSource of(String name, byte[] bytes) {
        return new InputStreamSourceFromBytes(name, bytes);
    }

    public static InputStreamSource of(String name, String data, Charset charset) {
        return new InputStreamSourceFromBytes(name, data.getBytes(charset));
    }

    public static InputStreamSource of(String name, String data) {
        return new InputStreamSourceFromBytes(name, data.getBytes());
    }

    public static InputStreamSource of(String name, File data) {
        return new InputStreamSourceFromFile(name, data);
    }

    public static InputStreamSource ofRenewableSupplier(String name, Supplier<InputStream> supplier) {
        return new InputStreamSourceFromSupplier(name, supplier);
    }

    public InputStreamSource(String name) {
        this.name = name;
    }


    public static class InputStreamSourceFromSupplier extends InputStreamSource {
        private final Supplier<InputStream> supplier;

        protected InputStreamSourceFromSupplier(String name, Supplier<InputStream> supplier) {
            super(name);
            this.supplier = supplier;
        }

        @Override
        public InputStream get() {
            return supplier.get();
        }
    }

    public static class InputStreamSourceFromBytes extends InputStreamSource {
        private final byte[] bytes;

        protected InputStreamSourceFromBytes(String name, byte[] bytes) {
            super(name);
            this.bytes = bytes;
        }

        @Override
        public InputStream get() {
            return new ByteArrayInputStream(bytes);
        }
    }

    public static class InputStreamSourceFromFile extends InputStreamSource {
        private final File data;

        protected InputStreamSourceFromFile(String name, File data) {
            super(name);
            this.data = data;
        }

        @Override
        public InputStream get() {
            try {
                return new FileInputStream(data);
            } catch (FileNotFoundException e) {
                throw Exceptions.propagate(e);
            }
        }
    }
}
