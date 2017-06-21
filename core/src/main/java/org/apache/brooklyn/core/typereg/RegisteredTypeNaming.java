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

import org.apache.brooklyn.util.text.BrooklynVersionSyntax;

/**
 * Methods for testing validity of names and decomposing them. 
 * Mostly based on OSGi, specifically sections 1.3.2 and 3.2.5 of 
 * osgi.core-4.3.0 spec (https://www.osgi.org/release-4-version-4-3-download/). */
public class RegisteredTypeNaming {

    private static final String USABLE_REGEX = "[^:\\s/\\\\]+";

    private final static String DOT = "\\.";
    
    public final static String OSGI_TOKEN_CHARS = "A-Za-z0-9_-";
    public final static String OSGI_TOKEN_REGEX = "[" + OSGI_TOKEN_CHARS + "]+";
    public final static String OSGI_SYMBOLIC_NAME_REGEX = OSGI_TOKEN_REGEX + "(" + DOT + OSGI_TOKEN_REGEX + ")*";

    private static boolean isUsable(String candidate) {
        return candidate!=null && candidate.matches(USABLE_REGEX);
    }
    
    /** 
     * For type names we currently work with any non-empty string that does not contain 
     * a ':' or whitespace or forward slash or backslash.
     * However we discourage things that are not OSGi symbolic names; see {@link #isValidTypeName(String)}. 
     * In some places (eg bundles) the use of OSGi symbolic names may be enforced.  
     */
    public static boolean isUsableTypeName(String candidate) {
        return isUsable(candidate);
    }

    /** 
     * We recommend type names be OSGi symbolic names, such as:
    *
    * <code>com.acme-group.1-Virtual-Machine</code>
    *
    * Note that this is more permissive than Java, allowing hyphens and 
    * allowing segments to start with numbers.
    * However it is also more restrictive:  OSGi does <i>not</i> allow
    * accented characters or most punctuation.  Only hyphens and underscores are allowed
    * in segment names, and periods are allowed only as segment separators.
    */
    public static boolean isGoodTypeName(String candidate) {
        return isUsable(candidate) && candidate.matches(OSGI_SYMBOLIC_NAME_REGEX);
    }

    /** @see BrooklynVersionSyntax#isUsableVersion(String) */
    public static boolean isUsableVersion(String candidate) {
        return BrooklynVersionSyntax.isUsableVersion(candidate);
    }
    
    /** @see BrooklynVersionSyntax#isGoodBrooklynVersion(String) */
    public static boolean isGoodBrooklynVersion(String candidate) {
        return BrooklynVersionSyntax.isGoodBrooklynVersion(candidate);
    }
    
    /** @see BrooklynVersionSyntax#isValidOsgiVersion(String) */
    public static boolean isValidOsgiVersion(String candidate) {
        return BrooklynVersionSyntax.isValidOsgiVersion(candidate);
    }

    /** True if the argument has exactly one colon, and the part before
     * satisfies {@link #isUsableTypeName(String)} and the part after {@link #isUsableVersion(String)}. */
    public static boolean isUsableTypeColonVersion(String candidate) {
        // simplify regex, rather than decomposing and calling the methods referenced in the javadoc
        return candidate!=null && candidate.matches(USABLE_REGEX + ":" + USABLE_REGEX);        
    }

    /** True if the argument has exactly one colon, and the part before
     * satisfies {@link #isGoodTypeName(String)} and the part after 
     * {@link #isGoodBrooklynVersion(String)}. */
    public static boolean isGoodBrooklynTypeColonVersion(String candidate) {
        if (candidate==null) return false;
        int idx = candidate.indexOf(':');
        if (idx<=0) return false;
        if (!isGoodTypeName(candidate.substring(0, idx))) return false;
        if (!isGoodBrooklynVersion(candidate.substring(idx+1))) return false;
        return true;
    }

    /** True if the argument has exactly one colon, and the part before
     * satisfies {@link #isGoodTypeName(String)} and the part after 
     * {@link #isValidOsgiVersion(String)}. */
    public static boolean isValidOsgiTypeColonVersion(String candidate) {
        if (candidate==null) return false;
        int idx = candidate.indexOf(':');
        if (idx<=0) return false;
        if (!isGoodTypeName(candidate.substring(0, idx))) return false;
        if (!isValidOsgiVersion(candidate.substring(idx+1))) return false;
        return true;
    }
}
