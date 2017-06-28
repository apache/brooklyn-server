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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

/** Utilities for parsing and working with versions following the recommended Brooklyn scheme,
 * following <code>major.minor.patch-qualifier</code> syntax,
 * with support for mapping to OSGi. 
 * <p>
 * See tests for examples, {@link VersionComparator} for more notes, and its tests for more examples.
 */
public class BrooklynVersionSyntax {

    public static final String USABLE_REGEX = "[^:\\s/\\\\]+";
    public static final String DOT = "\\.";

    public final static String OSGI_TOKEN_CHARS = "A-Za-z0-9_-";
    public final static String OSGI_TOKEN_REGEX = "[" + OSGI_TOKEN_CHARS + "]+";
    public final static String NUMBER = "[0-9]+";
    public final static String QUALIFIER = OSGI_TOKEN_REGEX;
    
    public final static String VALID_OSGI_VERSION_REGEX = 
        NUMBER + 
            "(" + DOT + NUMBER +  
                "(" + DOT + NUMBER +  
                    "(" + DOT + QUALIFIER +  
                    ")?" +
                ")?" +
            ")?";
    
    public final static String GOOD_BROOKLYN_VERSION_REGEX = 
        NUMBER + 
            "(" + DOT + NUMBER +  
                "(" + DOT + NUMBER +  
                    "(" + "-" + QUALIFIER +  
                    ")?" +
                ")?" +
            ")?";
    
    private static boolean isUsable(String candidate) {
        return candidate!=null && candidate.matches(USABLE_REGEX);
    }
    
    /** 
     * For versions we currently work with any non-empty string that does not contain a ':' or whitespace.
     * However we discourage things that are not OSGi versions; see {@link #isValidOsgiVersion(String)}. 
     * In some places (eg bundles) the use of OSGi version syntax may be enforced.  
     */
    public static boolean isUsableVersion(String candidate) {
        return isUsable(candidate);
    }
    
    /** True if the argument matches the Brooklyn version syntax, 
     * <code>MAJOR.MINOR.POINT-QUALIFIER</code> or part thereof (not ending in a period though),
     * where the first three are whole numbers and the final piece is any valid OSGi token
     * (containing letters, numbers, _ and -; no full stops).
     * See also {@link #isValidOsgiVersion(String)} and note this _requires_ a different separator to OSGi.
     */
    public static boolean isGoodBrooklynVersion(String candidate) {
        return candidate!=null && candidate.matches(GOOD_BROOKLYN_VERSION_REGEX);
    }
    
    /** True if the argument matches the OSGi version spec, of the form 
     * <code>MAJOR.MINOR.POINT.QUALIFIER</code> or part thereof (not ending in a period though),
     * where the first three are whole numbers and the final piece is any valid OSGi token
     * (containing letters, numbers, _ and -; no full stops).
     * See also {@link #isGoodBrooklynVersion(String)}.
     */
    public static boolean isValidOsgiVersion(String candidate) {
        return candidate!=null && candidate.matches(VALID_OSGI_VERSION_REGEX);
    }

    /** Creates a string satisfying {@link #isValidOsgiVersion(String)} based on the input.
     * For input satisfying {@link #isGoodBrooklynVersion(String)} the only change will be in the qualifer separator
     * (from "-" to ".") and making any "0" minor/patch token explicit (so "1-x" becomes "1.0.0.x"),
     * and the change can be reversed using {@link #toGoodBrooklynVersion(String)} (modulo insertion of "0"'s for minor/patch numbers if missing).
     * For input satisfying {@link #isValidOsgiVersion(String)}, the only change will be insertions of 0 for minor/patch.
     * Precise behaviour for other input is not guaranteed but callers can expect output which resembles the input,
     * with any major/minor/patch string at the front preserved and internal contiguous alphanumeric sequences preserved. */
    public static String toValidOsgiVersion(String input) {
        Preconditions.checkNotNull(input);
        return toGoodVersion(input, ".", true);
        /* Note Maven has and used:  DefaultMaven2OsgiConverter
         * from https://github.com/apache/felix/blob/trunk/tools/maven-bundle-plugin/src/main/java/org/apache/maven/shared/osgi/DefaultMaven2OsgiConverter.java
         * but it (a) is more complicated, and (b) doesn't aggressively find numbers e.g. "1beta" goes to "0.0.0.1beta" instead of "1.0.0.beta" 
         */
    }

    /** Creates a string satisfying {@link #isGoodBrooklynVersion(String)} based on the input.
     * For input satisfying {@link #isGoodBrooklynVersion(String)} the input will be returned unchanged.
     * For input satisfying {@link #isValidOsgiVersion(String)} the qualifier separator will be changed to "-",
     * and {@link #toValidOsgiVersion(String)} can be used to reverse the input (modulo insertion of "0"'s for minor/patch numbers if missing).
     * Precise behaviour for other input is not guaranteed but callers can expect output which resembles the input,
     * with any major/minor/patch string at the front preserved and internal contiguous alphanumeric sequences preserved. */
    public static String toGoodBrooklynVersion(String input) {
        return toGoodVersion(input, "-", false);
    }
    
    private static String toGoodVersion(String input, String qualifierSeparator, boolean requireMinorAndPatch) {
        Preconditions.checkNotNull(input);
        final String FUZZY_REGEX = 
            "(" + NUMBER + "(" + DOT + NUMBER + "(" + DOT + NUMBER + ")?)?)?" +  
            "(" + ".*)";
        Matcher m = Pattern.compile(FUZZY_REGEX).matcher(input);
        if (!m.matches()) {
            throw new IllegalStateException("fuzzy matcher should match anything: '"+input+"'");  // sanity check - shouldn't happen
        }
        StringBuilder result = new StringBuilder();
        if (Strings.isEmpty(m.group(1))) {
            result.append("0.0.0");
        } else {
            result.append(m.group(1));
            if (requireMinorAndPatch) {
                if (Strings.isEmpty(m.group(2))) {
                    result.append(".0");
                }            
                if (Strings.isEmpty(m.group(3))) {
                    result.append(".0");
                }            
            }
        }
        String q = m.group(4);
        if (Strings.isNonEmpty(q)) {
            boolean collapsedUnsupported = false;
            boolean starting = true;
            result.append(qualifierSeparator);
            for (int i=0; i<q.length(); i++) {
                char c = q.charAt(i);
                boolean include;
                boolean unsupported = false;
                if (starting) {
                    // treat first char as separator char unless it is a letter/number 
                    include = ('A' <= c && 'Z' >= c) || ('a' <= c && 'z' >= c) || ('0' <= c && '9' >= c);
                    starting = false;
                    if (!include) {
                        if (c=='-' || c=='_' || c=='.') {
                            // treat these as separator chars, and drop them
                            if (q.length()==1) {
                                // unless there are no other chars (e.g. version "1." becomes "1-_"
                                unsupported = true;
                            }
                        } else {
                            // treat other chars as unsupported
                            unsupported = true;
                        }
                    }
                } else {
                    include = ('A' <= c && 'Z' >= c) || ('a' <= c && 'z' >= c) || ('0' <= c && '9' >= c) || c=='-' || c=='_';
                    if (!include) {
                        // treat as unsupported, unless we've collapsed
                        if (!collapsedUnsupported) {
                            unsupported = true;
                        }
                    }
                }
                if (include) {
                    result.append(c);
                    collapsedUnsupported = false;
                } else if (unsupported) {
                    // stick a "_" in for unsupported chars
                    result.append('_');
                    collapsedUnsupported = true;
                }
            }
        }
        return result.toString();
    }

    /** Returns true if the given strings are equal when mapped according to {@link #toValidOsgiVersion(String)} */
    public static boolean equalAsOsgiVersions(String v1, String v2) {
        if (v1==null || v2==null) return (v1==null && v2==null);
        return toValidOsgiVersion(v1).equals(toValidOsgiVersion(v2));
    }

}
