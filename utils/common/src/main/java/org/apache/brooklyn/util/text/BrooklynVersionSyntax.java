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

/** Utilities for parsing and working with versions following the recommended Brooklyn scheme,
 * following <code>major.minor.patch-qualifier</code> syntax,
 * with support for mapping to OSGi. 
 * <p>
 * See {@link VersionComparator} and its tests for examples.
 */
public class BrooklynVersionSyntax {

    private static final String USABLE_REGEX = "[^:\\s/\\\\]+";

    private final static String OSGI_TOKEN_CHARS = "A-Za-z0-9_-";
    private final static String OSGI_TOKEN_REGEX = "[" + OSGI_TOKEN_CHARS + "]+";
    private final static String NUMBER = "[0-9]+";
    private final static String QUALIFIER = OSGI_TOKEN_REGEX;
    
    public final static String VALID_OSGI_VERSION_REGEX = 
        NUMBER + 
            "(" + "\\." + NUMBER +  
                "(" + "\\." + NUMBER +  
                    "(" + "\\." + QUALIFIER +  
                    ")?" +
                ")?" +
            ")?";
    
    public final static String GOOD_BROOKLYN_VERSION_REGEX = 
        NUMBER + 
            "(" + "\\." + NUMBER +  
                "(" + "\\." + NUMBER +  
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
     * (something satisfying {@link #isGoodTypeName(String)} with no periods).
     * See also {@link #isValidOsgiVersion(String)} and note this _requires_ a different separator to OSGi.
     */
    public static boolean isGoodBrooklynVersion(String candidate) {
        return candidate!=null && candidate.matches(GOOD_BROOKLYN_VERSION_REGEX);
    }
    
    /** True if the argument matches the OSGi version spec, of the form 
     * <code>MAJOR.MINOR.POINT.QUALIFIER</code> or part thereof (not ending in a period though),
     * where the first three are whole numbers and the final piece is any valid OSGi token
     * (something satisfying {@link #isGoodTypeName(String)} with no periods).
     * See also {@link #isGoodVersion(String)}.
     */
    public static boolean isValidOsgiVersion(String candidate) {
        return candidate!=null && candidate.matches(VALID_OSGI_VERSION_REGEX);
    }

}
