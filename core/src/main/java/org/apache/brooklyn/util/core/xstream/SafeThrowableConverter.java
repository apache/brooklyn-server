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
package org.apache.brooklyn.util.core.xstream;

import com.google.common.annotations.VisibleForTesting;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.core.DefaultConverterLookup;
import com.thoughtworks.xstream.core.util.QuickWriter;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.path.PathTrackingWriter;
import org.apache.brooklyn.util.core.xstream.XmlSerializer.PrettyPrintWriterExposingStack;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.function.Predicate;

/** This is a hacky way to try to recover in some non-serializable situations. But it can and often does
 * generate XML which cannot be read back, because it will omit fields which might be required to create an object.
 */
public class SafeThrowableConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(SafeThrowableConverter.class);
    private final DefaultConverterLookup converterLookup;
    private final Predicate<Class> supportedTypes;

    ThreadLocal<Object> converting = new ThreadLocal<>();

    @VisibleForTesting
    public static int TODO = 0;

    public SafeThrowableConverter(Predicate<Class> supportedTypes, DefaultConverterLookup converterLookup) {
        this.supportedTypes = supportedTypes;
        this.converterLookup = converterLookup;
    }

    @Override
    public boolean canConvert(@SuppressWarnings("rawtypes") Class type) {
        return converting.get()==null && supportedTypes.test(type);
    }

    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        boolean wrappedHere = false;

        int depth = -1;
        HierarchicalStreamWriter w2 = writer;
        try {
            if (converting.get()!=null) {
                // shouldn't come here; above should prevent it
                wrappedHere = false;
            } else {
                wrappedHere = true;
                converting.set(source);
                // flush cache so it recomputes whether we can convert (we cannot anymore)
                converterLookup.flushCache();
                while (w2 instanceof PathTrackingWriter) { w2 = w2.underlyingWriter(); }
                if (w2 instanceof PrettyPrintWriterExposingStack) {
                    depth = ((PrettyPrintWriterExposingStack)w2).path.depth();
                }
            }
            try {
                context.convertAnother(source);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                if (depth<0) throw Exceptions.propagate(e);

                log.debug("Unable to convert "+source+"; will abandon keys that aren't valid: "+e);
                while (((PrettyPrintWriterExposingStack)w2).path.depth()>depth) {
                    writer.endNode();
                    writer.flush();
                    try {
                        ((PrettyPrintWriterExposingStack) w2).getOrigWriter().write("<!-- fields omitted in previous node due to error -->");
                    } catch (IOException ex) {
                        throw Exceptions.propagate(ex);
                    }
                }
            }
        } finally {
            if (wrappedHere) {
                converting.set(null);
                converterLookup.flushCache();
            }
        }
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        converting.set("not for use with unmarshalling");
        // flush cache so it recomputes whether we can convert (we cannot anymore)
        converterLookup.flushCache();
        return context.convertAnother(null, context.getRequiredType());
    }

}
