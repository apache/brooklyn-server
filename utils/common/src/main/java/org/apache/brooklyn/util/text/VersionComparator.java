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

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;

/**
 * {@link Comparator} for version strings.
 * <p>
 * This gives the desired semantics for our recommended version syntax,
 * following <code>major.minor.patch-qualifier</code> syntax, 
 * doing numeric order of major/minor/patch (1.10 > 1.9),
 * treating anything with a qualifier lower than the corresponding without but higher than items before
 * (1.2 > 1.2-rc > 1.1),
 * SNAPSHOT items always lowest rated (1.0 > 2-SNAPSHOT), and 
 * qualifier sorting follows {@link NaturalOrderComparator} (1-M10 > 1-M9).
 * <p>
 * Impossible to follow semantics for all versioning schemes but 
 * does the obvious right thing for normal schemes and pretty well in fringe cases.
 * Probably the most surprising fringe behaviours will be
 * 1.2.3.4 < 1.2.3 (the ".4" is considered a qualifier).
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

}
