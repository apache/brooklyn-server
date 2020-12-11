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

import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.ByteSource;

/** @deprecated since 0.10.0; underlying guava deprecated; 
 * guava says to use a {@link ByteSource}; and in many cases there's also a better way */
@Deprecated
public class InputStreamSupplier extends ByteSource {

    private final InputStream target;

    private InputStreamSupplier(InputStream target) {
        this.target = target;
    }

    @Deprecated
    public InputStream getInput() throws IOException {
        return openStream();
    }

    public static InputStreamSupplier of(InputStream target) {
        return new InputStreamSupplier(target);
    }

    public static InputStreamSupplier fromString(String input) {
        return new InputStreamSupplier(Streams.newInputStreamWithContents(input));
    }

    @Override
    public InputStream openStream() throws IOException {
        return target;
    }
}
