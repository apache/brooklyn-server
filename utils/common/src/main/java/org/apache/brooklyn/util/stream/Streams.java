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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

/**
 * Methods to manage byte and character streams.
 *
 * @see com.google.common.io.ByteStreams
 * @see com.google.common.io.CharStreams
 */
public class Streams {

    private static final Logger log = LoggerFactory.getLogger(Streams.class);

    /** drop-in non-deprecated replacement for {@link Closeable}'s deprecated closeQuiety;
     * we may wish to review usages, particularly as we drop support for java 1.6,
     * but until then use this instead of the deprecated method */
    @Beta
    public static void closeQuietly(Closeable x) {
        try {
            if (x!=null)
                x.close();
        } catch (Exception e) {
            if (log.isDebugEnabled())
                log.debug("Error closing (ignored) "+x+": "+e);
        }
    }

    /** @deprecated since 0.7.0 use {@link #newInputStreamWithContents(String)} */ @Deprecated
    public static InputStream fromString(String contents) {
        return newInputStreamWithContents(contents);
    }
    
    public static InputStream newInputStreamWithContents(String contents) {
        byte[] bytes = checkNotNull(contents, "contents").getBytes(Charsets.UTF_8);
        return KnownSizeInputStream.of(bytes);
    }

    public static Reader newReaderWithContents(String contents) {
        return new StringReader(contents);
    }
    
    public static Reader reader(InputStream stream) {
        return new InputStreamReader(stream);
    }
    
    public static Reader reader(InputStream stream, Charset charset) {
        return new InputStreamReader(stream, charset);
    }

    /** reads the input stream fully, returning a byte array; throws unchecked exception on failure;
     *  to get a string, use <code>readFully(reader(is))</code> or <code>readFullyString(is)</code>;
     *  consider using {@ #readFullyAndClose(InputStream)} instead;*/
    public static byte[] readFully(InputStream is) {
        try {
            return ByteStreams.toByteArray(is);
        } catch (IOException ioe) {
            throw Exceptions.propagate(ioe);
        }
    }
    
    /** reads the input stream fully into the given byte buffer or until the supplied buffer is full,
     * returning the number of bytes read */
    public static int readFully(InputStream s, byte[] buf) throws IOException {
        int count = 0;
        while (count < buf.length) {
            int countHere = s.read(buf, count, buf.length-count);
            if (countHere<=0) return count;
            count += countHere;
        }
        return count;
    }
    
    public static byte[] readFullyAndClose(InputStream is) {
        try {
            return readFully(is);
        } finally {
            Streams.closeQuietly(is);
        }
    }

    /**
     * Consider using {@link #readFullyStringAndClose(InputStream)} instead.
     */
    public static String readFullyString(InputStream is) {
        return readFully(reader(is));
    }

    public static String readFullyStringAndClose(InputStream is) {
        try {
            return readFullyString(is);
        } finally {
            Streams.closeQuietly(is);
        }
    }

    /**
     * Consider using {@link #readFullyAndClose(Reader)} instead.
     */
    public static String readFully(Reader is) {
        try {
            return CharStreams.toString(is);
        } catch (IOException ioe) {
            throw Exceptions.propagate(ioe);
        }
    }
    
    public static String readFullyAndClose(Reader is) {
        try {
            return readFully(is);
        } finally {
            Streams.closeQuietly(is);
        }
    }

    public static void copy(InputStream input, OutputStream output) {
        try {
            ByteStreams.copy(input, output);
            output.flush();
        } catch (IOException ioe) {
            throw Exceptions.propagate(ioe);
        }
    }
    
    /** copies and closes both */
    public static void copyClose(InputStream input, OutputStream output) {
        try {
            copy(input, output);
        } finally {
            closeQuietly(input);
            closeQuietly(output);
        }
    }

    public static void copy(Reader input, Writer output) {
        try {
            CharStreams.copy(input, output);
            output.flush();
        } catch (IOException ioe) {
            throw Exceptions.propagate(ioe);
        }
    }

    public static Supplier<Integer> sizeSupplier(final ByteArrayOutputStream src) {
        Preconditions.checkNotNull(src);
        return new Supplier<Integer>() {
            @Override
            public Integer get() {
                return src.size();
            }
        };
    }

    public static Function<ByteArrayOutputStream,Integer> sizeFunction() {
        return new Function<ByteArrayOutputStream,Integer>() {
            @Override
            public Integer apply(ByteArrayOutputStream input) {
                return input.size();
            }
        };
    }

    public static ByteArrayOutputStream byteArrayOfString(String in) {
        return byteArray(in.getBytes(Charsets.UTF_8));
    }

    public static ByteArrayOutputStream byteArray(byte[] in) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            stream.write(in);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
        return stream;
    }

    public static boolean logStreamTail(Logger log, String message, ByteArrayOutputStream stream, int max) {
        if (stream!=null && stream.size()>0) {
            String streamS = stream.toString();
            if (max>=0 && streamS.length()>max)
                streamS = "... "+streamS.substring(streamS.length()-max);
            log.info(message+":\n"+streamS);
            return true;
        }
        return false;
    }

    public static String getMd5Checksum(InputStream in) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw Exceptions.propagate(e);
        }
        DigestInputStream dis = new DigestInputStream(in, md);
        readFullyAndClose(dis);
        byte[] digest = md.digest();
        StringBuilder result = new StringBuilder();
        for (byte b: digest) {
            result.append(Strings.padStart(Integer.toHexString((256+b)%256), 2, '0'));
        }
        return result.toString().toUpperCase();
    }

    /** True iff the input streams read to completion give identical contents. Streams are closed afterwards. */
    public static boolean compare(InputStream s1, InputStream s2) throws IOException {
        try {
            if (s1==null || s2==null) {
                return s1==null && s2==null;
            }
            byte[] buf1 = new byte[4096], buf2 = new byte[4096];
            while (true) {
                int r1 = readFully(s1, buf1);
                int r2 = readFully(s2, buf2);
                // do this in case HeapByteBuffer.equals has an efficient intrinsic comparison;
                // curiously there is no efficient array-compare a la System.arraycopy;
                // the only thing I've found is that Arrays.equals(char[], char[]) is optimized/intrinsic,
                // and allegedly 8 times faster than all other array comparison routines!
                // https://stackoverflow.com/questions/41153992/why-is-arrays-equalschar-char-8-times-faster-than-all-the-other-versions
                if (!ByteBuffer.wrap(buf1,0,r1).equals(ByteBuffer.wrap(buf2,0,r2))) return false;
                if (r1<4096) return true;
            }
        } finally {
            closeQuietly(s1);
            closeQuietly(s2);
        }
    }

}
