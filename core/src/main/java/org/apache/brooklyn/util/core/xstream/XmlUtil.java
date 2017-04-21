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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.google.common.annotations.Beta;

public class XmlUtil {

    /**
     * Thread-local storage for sharing the {@link DocumentBuilder}, to avoid repeated construction.
     * See {@linkplain http://stackoverflow.com/questions/9828254/is-documentbuilderfactory-thread-safe-in-java-5}.
     */
    private static class SharedDocumentBuilder {
        private static ThreadLocal<DocumentBuilder> instance = new ThreadLocal<DocumentBuilder>();
        
        public static DocumentBuilder get() throws ParserConfigurationException {
            DocumentBuilder result = instance.get();
            if (result == null) {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                result = factory.newDocumentBuilder();
                instance.set(result);
            } else {
                result.reset();
            }
            return result;
        }
    }

    public static String xpath(String xml, String xpath) {
        return (String) xpath(xml, xpath, XPathConstants.STRING);
    }

    public static Object xpath(String xml, String xpath, QName returnType) {
        try {
            DocumentBuilder builder = SharedDocumentBuilder.get();
            Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes()));
            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPathExpression expr = xPathfactory.newXPath().compile(xpath);
            
            return expr.evaluate(doc, returnType);
            
        } catch (ParserConfigurationException e) {
            throw Exceptions.propagate(e);
        } catch (SAXException e) {
            throw Exceptions.propagate(e);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        } catch (XPathExpressionException e) {
            throw Exceptions.propagate(e);
        }
    }

    public static Object xpathHandlingIllegalChars(String xml, String xpath) {
        return xpathHandlingIllegalChars(xml, xpath, XPathConstants.STRING);
    }

        /**
         * Executes the given xpath on the given xml. If this fails becaues the xml is invalid
         * (e.g. contains "&#x1b;"), then it will attempt to escape such illegal characters
         * and try again. Note that the *escaped* values may be contained in the returned result!
         * The escaping used is the prefix "BR_UNICODE_"; if that string is already in the xml,
         * then it will replace that with "NOT_BR_UNICODE_".
         */
    @Beta
    public static Object xpathHandlingIllegalChars(String xml, String xpath, QName returnType) {
        try {
            return xpath(xml, xpath, returnType);
        } catch (Exception e) {
            SAXException saxe = Exceptions.getFirstThrowableOfType(e, SAXException.class);
            if (saxe != null && saxe.toString().contains("&#")) {
                // Looks like illegal chars (e.g. xstream converts unicode char 27 to "&#x1b;", 
                // which is not valid in XML! Try again with an escaped xml.
                Escaper escaper = new Escaper();
                String xmlCleaned = escaper.escape(xml);
                try {
                    Object result = xpath(xmlCleaned, xpath, returnType);
                    if (result instanceof String) {
                        return escaper.unescape((String)result);
                    } else {
                        return result;
                    }
                } catch (Exception e2) {
                    Exceptions.propagateIfFatal(e2);
                }
            }
            throw e;
        }
    }

    /**
     * Replaces things like "&#x1b;" with "BR_UNICODE_x1b". This is because xstream happily writes 
     * out such characters (which are not valid in xml), but xpath fails when parsing them.
     */
    @Beta
    protected static class Escaper {

        public String escape(String string) {
            String unicodeRegex = "&#([x0-9a-fA-f]{1,5});";
            return string.replaceAll("BR_UNICODE_", "NOT_BR_UNICODE_")
                    .replaceAll(unicodeRegex, "BR_UNICODE_$1;");
        }
        
        public String unescape(String string) {
            String unicodeRegex = "(?<!NOT_)BR_UNICODE_([x0-9a-fA-F]{1,5})";
            return string
                    .replaceAll(unicodeRegex, "&#$1")
                    .replaceAll("NOT_BR_UNICODE_", "BR_UNICODE_");
        }
    }
}
