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
package org.apache.brooklyn.core.typereg;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.WildcardGlobs;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience supertype for {@link BrooklynCatalogBundleResolver} instances.
 * <p>
 * This supplies a default {@link #scoreForBundle(String, Supplier<InputStream>)} method
 * method which returns 1 if the format code matches,
 * and otherwise branches to various abstract score methods which subclasses can implement,
 * cf {@link AbstractTypePlanTransformer}.
 */
public abstract class AbstractCatalogBundleResolver implements BrooklynCatalogBundleResolver {

    private static final Logger log = LoggerFactory.getLogger(AbstractCatalogBundleResolver.class);

    protected ManagementContext mgmt;

    @Override
    public void setManagementContext(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    public AbstractCatalogBundleResolver withManagementContext(ManagementContext mgmt) {
        this.mgmt = mgmt;
        return this;
    }

    private final String format;
    private final String formatName;
    private final String formatDescription;

    protected AbstractCatalogBundleResolver(String format, String formatName, String formatDescription) {
        this.format = format;
        this.formatName = formatName;
        this.formatDescription = formatDescription;
    }
    
    @Override
    public String getFormatCode() {
        return format;
    }

    @Override
    public String getFormatName() {
        return formatName;
    }

    @Override
    public String getFormatDescription() {
        return formatDescription;
    }

    @Override
    public String toString() {
        return getFormatCode()+":"+JavaClassNames.simpleClassName(this);
    }
    
    @Override
    public double scoreForBundle(String format, Supplier<InputStream> f) {
        if (getFormatCode().equals(format)) return 1;
        if (Strings.isBlank(format))
            return scoreForNullFormat(f);
        else
            return scoreForNonmatchingNonnullFormat(format, f);
    }

    protected abstract double scoreForNullFormat(Supplier<InputStream> f);

    protected double scoreForNonmatchingNonnullFormat(String format, Supplier<InputStream> f) {
        return 0;
    }

    public static class FileTypeDetector implements AutoCloseable {
        final Supplier<InputStream> streamSupplier;

        private byte[] bytesRead = new byte[0];
        private boolean readAll = false;

        public FileTypeDetector(Supplier<InputStream> streamSupplier) {
            this.streamSupplier = streamSupplier;
        }

        protected byte[] readBytes(int length) {
            if (bytesRead.length >= length) {
                return bytesRead;
            }
            bytesRead = new byte[length];
            InputStream stream = streamSupplier.get();
            int size;
            try {
                size = stream.read(bytesRead);
                stream.close();
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
            if (size<length) {
                bytesRead = Arrays.copyOf(bytesRead, size<0 ? 0 : size);
                readAll = true;
            }
            return bytesRead;
        }

        private byte[] readAll() {
            if (readAll) {
                return bytesRead;
            }
            int size = 1024;
            while (size < bytesRead.length) {
                size = size * 16;
            }
            do {
                byte[] br = readBytes(size);
                if (br.length<size) {
                    readAll = true;
                    return br;
                }
                size = size * 16;
            } while (true);
        }

        // https://en.wikipedia.org/wiki/List_of_file_signatures
        private final static byte[] HEADER_ZIP              = new byte[] { 0x50, 0x4B, 0x03, 0x04 };
        private final static byte[] HEADER_ZIP_EMPTY        = new byte[] { 0x50, 0x4B, 0x05, 0x06 };
        private final static byte[] HEADER_ZIP_SPANNED      = new byte[] { 0x50, 0x4B, 0x07, 0x08 };
        private final static List<byte[]> HEADERS_ZIP = Arrays.asList(HEADER_ZIP, HEADER_ZIP_EMPTY, HEADER_ZIP_SPANNED);

        public boolean isZip() {
            byte[] header = readBytes(4);
            return HEADERS_ZIP.stream().anyMatch(zip -> Arrays.equals(header, zip));
        }


//[1] c-printable ::= #x9 | #xA | #xD | [#x20-#x7E]          /* 8 bit */
//                  | #x85 | [#xA0-#xD7FF]
//                  | [#xE000-#xFFFD]                        /* 16 bit */
//                  | [#x10000-#x10FFFF]                     /* 32 bit */
        private boolean isValidYamlChar(int c) {
            if (c < 0x20) {
                return c==0x09 || c==0x0a || c==0x0d;
            }
            if (c <= 0x7E) return true;
            if (c < 0xA0) {
                return (c == 0x85);
            }
            if (c <= 0xD7FF) return true;
            if (c < 0xE000) return false;
            if (c <= 0xFFFD) return true;
            if (c < 0x10000) return false;
            if (c <= 0x10FFFF) return true;
            return false;
        }

        public boolean isPrintableText() {
            int size = 16;  // enough to rule out most binary files

            while (true) {
                byte[] headerB = readBytes(size);
                String header = new String(headerB);

                // scan
                if (!header.chars().allMatch(this::isValidYamlChar)) {
                    return false;
                }

                if (headerB.length < size) {
                    // we have read everything
                    return true;
                }

                // keep scaling out until we read the whole thing
                size *= 16;
            }
        }

        private Object yaml;

        public boolean isYaml() {
            if (!isPrintableText()) return false;
            String header = new String(bytesRead);

            try {
                yaml = Yamls.parseAll(header);
                return true;
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                return false;
            }
        }

        public Maybe<Object> getYaml() {
            if (yaml!=null) return Maybe.of(yaml);
            if (!isPrintableText()) return Maybe.absent("Input does not consist of valid printable YAML characters.");

            try {
                yaml = Yamls.parseAll(new String(bytesRead));
                return Maybe.ofAllowingNull(yaml);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                return Maybe.absent(e);
            }
        }

        public List<String> zipFileMatchesGlob(String glob) {
            try {
                return getZipFile().stream().map(ZipEntry::getName).filter(name -> Os.isPathGlobMatched(glob, name, true)).collect(Collectors.toList());

            } catch (Exception e) {
                throw new IllegalArgumentException("Error reading zip: "+e, e);
            }
        }

        File zipFile = null;
        public ZipFile getZipFile() throws IOException {
            if (zipFile==null) {
                zipFile = Os.newTempFile("zip-bundle-detector", "zip");
                FileOutputStream fout = new FileOutputStream(zipFile);
                Streams.copy(streamSupplier.get(), fout);
                fout.close();
            }

            ZipFile zf = new ZipFile(zipFile);
            return zf;
        }

        public String zipFileContents(String s) {
            try {
                ZipFile zf = getZipFile();
                ZipEntry ze = zf.getEntry(s);
                return Streams.readFullyStringAndClose(zf.getInputStream(ze));

            } catch (Exception e) {
                throw new IllegalArgumentException("Error reading zip: "+e, e);
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            close();
        }

        @Override
        public void close() {
            if (zipFile!=null) {
                zipFile.delete();
                zipFile = null;
            }
        }
    }
}
