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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

/**
 * {@link Comparator} for version strings.
 * <p>
 * SNAPSHOT items always lowest rated, then looking for no qualifier, then doing natural order comparison.
 * This gives the desired semantics for our recommended version syntax.
 * <p>
 * Impossible to follow semantics for all versioning schemes but 
 * does the obvious right thing for normal schemes and pretty well in fringe cases.
 * <p>
 * See test case for lots of examples.
 */
public class VersionComparator implements Comparator<String> {
    
    private static final String SNAPSHOT = "SNAPSHOT";

    public static final VersionComparator INSTANCE = new VersionComparator();

    public static VersionComparator getInstance() {
        return INSTANCE;
    }

    public static boolean isSnapshot(String version) {
        if (version==null) return false;
        return version.toUpperCase().contains(SNAPSHOT);
    }

    
    @SuppressWarnings("unused")
    private static class TwoBooleans {
        private final boolean b1, b2;
        public TwoBooleans(boolean b1, boolean b2) { this.b1 = b1; this.b2 = b2; }
        boolean bothTrue() { return b1 && b2; }
        boolean eitherTrue() { return b1 || b2; }
        boolean bothFalse() { return !eitherTrue(); }
        boolean same() { return b1==b2; }
        boolean different() { return b1!=b2; }
        int compare(boolean trueIsLess) { return same() ? 0 : b1==trueIsLess ? -1 : 1; }
        public static TwoBooleans of(boolean v1, boolean v2) {
            return new TwoBooleans(v1, v2);
        }
    }
    @Override
    public int compare(String v1, String v2) {
        if (Objects.equal(v1, v2)) return 0;
        
        TwoBooleans nulls = TwoBooleans.of(v1==null, v2==null);
        if (nulls.eitherTrue()) return nulls.compare(true);

        TwoBooleans snapshots = TwoBooleans.of(isSnapshot(v1), isSnapshot(v2));
        if (snapshots.different()) return snapshots.compare(true);

        String u1 = versionWithQualifier(v1);
        String u2 = versionWithQualifier(v2);
        int uq = NaturalOrderComparator.INSTANCE.compare(u1, u2);
        if (uq!=0) return uq;
        
        // qualified items are lower than unqualified items
        TwoBooleans no_qualifier = TwoBooleans.of(u1.equals(v1), u2.equals(v2));
        if (no_qualifier.different()) return no_qualifier.compare(false);

        // now compare qualifiers
        
        // allow -, ., and _ as qualifier offsets; if used, compare qualifer without that
        String q1 = v1.substring(u1.length());
        String q2 = v2.substring(u2.length());
        
        String qq1 = q1, qq2 = q2;
        if (q1.startsWith("-") || q1.startsWith(".") || q1.startsWith("_")) q1 = q1.substring(1);
        if (q2.startsWith("-") || q2.startsWith(".") || q2.startsWith("_")) q2 = q2.substring(1);
        uq = NaturalOrderComparator.INSTANCE.compare(q1, q2);
        if (uq!=0) return uq;
        
        // if qualifiers the same, look at qualifier separator char
        int sep1 = qq1.startsWith("-") ? 3 : qq1.startsWith(".") ? 2 : qq1.startsWith("_") ? 1 : 0;
        int sep2 = qq2.startsWith("-") ? 3 : qq2.startsWith(".") ? 2 : qq2.startsWith("_") ? 1 : 0;
        uq = Integer.compare(sep1, sep2);
        if (uq!=0) return uq;
        
        // finally, do normal comparison if both have qualifier or both unqualified (in case leading 0's are used)
        return NaturalOrderComparator.INSTANCE.compare(v1, v2);
        
        // (previously we did this but don't think we need any of that complexity)
        //return compareDotSplitParts(splitOnDot(v1), splitOnDot(v2));
    }

    private String versionWithQualifier(String v1) {
        Matcher q = Pattern.compile("([0-9]+(\\.[0-9]+(\\.[0-9])?)?)(.*)").matcher(v1);
        return q.matches() ? q.group(1) : "";
    }

    private boolean isQualifiedAndUnqualifiedVariantOfSameVersion(String v1, String v2) {
        Matcher q = Pattern.compile("([0-9]+(\\.[0-9]+(\\.[0-9])?)?)(.*)").matcher(v1);
        return (q.matches() && q.group(1).equals(v2));
    }

    private static boolean hasQualifier(String v) {
        return !v.matches("[0-9]+(\\.[0-9]+(\\.[0-9])?)?");
    }
    
    @VisibleForTesting
    static String[] splitOnDot(String v) {
        return v.split("(?<=\\.)|(?=\\.)");
    }
    
    private int compareDotSplitParts(String[] v1Parts, String[] v2Parts) {
        for (int i = 0; ; i++) {
            if (i >= v1Parts.length && i >= v2Parts.length) {
                // end of both
                return 0;
            }
            if (i == v1Parts.length) {
                // sequence depends whether the extra part *starts with* a number
                // ie
                //                   2.0 < 2.0.0
                // and
                //   2.0.qualifier < 2.0 < 2.0.0qualifier < 2.0.0-qualifier < 2.0.0.qualifier < 2.0.0 < 2.0.9-qualifier
                return isNumberInFirstCharPossiblyAfterADot(v2Parts, i) ? -1 : 1;
            }
            if (i == v2Parts.length) {
                // as above but inverted
                return isNumberInFirstCharPossiblyAfterADot(v1Parts, i) ? 1 : -1;
            }
            // not at end; compare this dot split part
            
            int result = compareDotSplitPart(v1Parts[i], v2Parts[i]);
            if (result!=0) return result;
        }
    }
    
    private int compareDotSplitPart(String v1, String v2) {
        String[] v1Parts = splitOnNonWordChar(v1);
        String[] v2Parts = splitOnNonWordChar(v2);
        
        for (int i = 0; ; i++) {
            if (i >= v1Parts.length && i >= v2Parts.length) {
                // end of both
                return 0;
            }
            if (i == v1Parts.length) {
                // shorter set always wins here; i.e.
                // 1-qualifier < 1
                return 1;
            }
            if (i == v2Parts.length) {
                // as above but inverted
                return -1;
            }
            // not at end; compare this dot split part
            
            String v1p = v1Parts[i];
            String v2p = v2Parts[i];
            
            if (v1p.equals(v2p)) continue;
            
            if (isNumberInFirstChar(v1p) || isNumberInFirstChar(v2p)) {
                // something starting with a number is higher than something not
                if (!isNumberInFirstChar(v1p)) return -1;
                if (!isNumberInFirstChar(v2p)) return 1;
                
                // both start with numbers; can use natural order comparison *unless*
                // one is purely a number AND the other *begins* with that number,
                // followed by non-digit chars, in which case prefer the pure number
                // ie:
                //           1beta < 1
                // but note
                //            1 < 2beta < 11beta
                if (isNumber(v1p) || isNumber(v2p)) {
                    if (!isNumber(v1p)) {
                        if (v1p.startsWith(v2p)) {
                            if (!isNumberInFirstChar(Strings.removeFromStart(v1p, v2p))) {
                                // v2 is a number, and v1 is the same followed by non-numbers
                                return -1;
                            }
                        }
                    }
                    if (!isNumber(v2p)) {
                        // as above but inverted
                        if (v2p.startsWith(v1p)) {
                            if (!isNumberInFirstChar(Strings.removeFromStart(v2p, v1p))) {
                                return 1;
                            }
                        }
                    }
                    // both numbers, skip to natural order comparison
                }
            }
            
            // otherwise it is in-order
            int result = NaturalOrderComparator.INSTANCE.compare(v1p, v2p);
            if (result!=0) return result;
        }
    }

    @VisibleForTesting
    static String[] splitOnNonWordChar(String v) {
        Collection<String> parts = new ArrayList<String>();
        String remaining = v;
        
        // use lookahead to split on all non-letter non-numbers, putting them into their own buckets 
        parts.addAll(Arrays.asList(remaining.split("(?<=[^0-9\\p{L}])|(?=[^0-9\\p{L}])")));
        return parts.toArray(new String[parts.size()]);
    }

    @VisibleForTesting
    static boolean isNumberInFirstCharPossiblyAfterADot(String[] parts, int i) {
        if (parts==null || parts.length<=i) return false;
        if (isNumberInFirstChar(parts[i])) return true;
        if (".".equals(parts[i])) {
            if (parts.length>i+1)
                if (isNumberInFirstChar(parts[i+1])) 
                    return true;
        }
        return false;
    }

    @VisibleForTesting
    static boolean isNumberInFirstChar(String v) {
        if (v==null || v.length()==0) return false;
        return Character.isDigit(v.charAt(0));
    }
    
    @VisibleForTesting
    static boolean isNumber(String v) {
        if (v==null || v.length()==0) return false;
        return v.matches("[\\d]+");
    }
}
