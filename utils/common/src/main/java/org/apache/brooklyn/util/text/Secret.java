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
package org.apache.brooklyn.util.text;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.brooklyn.util.stream.Streams;

import java.io.ByteArrayInputStream;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/** Wraps something which should be recognized as a secret.
 *
 * Jackson serialization returns a suppressed message with a checksum, and deserialization is blocked,
 * unless the serialization/deserialization is performed in a `runWithJacksonSerializationEnabled` block.
 */
public class Secret<T> implements Supplier<T> {

    public Secret(T secret) {
        this.secret = secret;
    }

    private final T secret;

    public T get() {
        return secret;
    }

    @JsonValue
    public Object getSanitized() {
        if (SecretHelper.permitJacksonSerializationInThisThread.get()!=null) {
            return secret;
        }

        return SecretHelper.suppress(secret);
    }

    @JsonCreator
    static <T> Secret<T> jacksonNotNormallyAllowedToCreate(T possiblySuppressedValue) {
        if (SecretHelper.permitJacksonSerializationInThisThread.get()!=null) {
            return new Secret(possiblySuppressedValue);
        }
        if (SecretHelper.isProbablySuppressed(possiblySuppressedValue)) {
            throw new IllegalStateException("Secrets deserialization detected on value which appears suppressed");
        }

        // we could allow, if that makes some coercion easier. but for now require callers to opt-in,
        // using runWithJacksonSerializationEnabledInThread
//        return new Secret(possiblySuppressedValue);
        throw new IllegalStateException("Secrets cannot be deserialized from JSON");
    }

    public static class SecretHelper {
        public static boolean isProbablySuppressed(Object value) {
            if ((""+value).startsWith("<suppressed>")) return true;
            return false;
        }

        public static String suppress(Object value) {
            if (value == null) return null;
            // only include the first few chars so that malicious observers can't uniquely brute-force discover the source
            String md5Checksum = Strings.maxlen(Streams.getMd5Checksum(new ByteArrayInputStream(("" + value).getBytes())), 8);
            return "<suppressed> (MD5 hash: " + md5Checksum + ")";
        }

        static ThreadLocal<Integer> permitJacksonSerializationInThisThread = new ThreadLocal<>();

        static <T> T runWithJacksonSerializationEnabledInThread(Callable<T> callable) throws Exception {
            try {
                Integer old = permitJacksonSerializationInThisThread.get();
                if (old==null) old = 0;
                old++;
                permitJacksonSerializationInThisThread.set(old);

                return callable.call();

            } finally {
                Integer old = permitJacksonSerializationInThisThread.get();
                old--;
                if (old==0) permitJacksonSerializationInThisThread.remove();
                else permitJacksonSerializationInThisThread.set(old);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Secret) return Objects.equals(get(), ((Secret)obj).get());
        return Objects.equals(get(), obj);
    }

    @Override
    public int hashCode() {
        Object x = get();
        return x==null ? 0 : x.hashCode();
    }

    @Override
    public String toString() {
        return "Secret["+SecretHelper.suppress(secret)+"]";
    }

}
