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
 * 

BROOKLYN NOTE: This is based on code from Pierre-Luc Paour,
adapted for the Brooklyn project in accordance with the original terms below.
Main changes are the package and edits for more recent Java compatibility.

--

NaturalOrderComparator.java -- Perform 'natural order' comparisons of strings in Java.
Copyright (C) 2003 by Pierre-Luc Paour <natorder@paour.com>

Based on the C version by Martin Pool, of which this is more or less a straight conversion.
Copyright (C) 2000 by Martin Pool <mbp@humbug.org.au>

This software is provided 'as-is', without any express or implied
warranty.  In no event will the authors be held liable for any damages
arising from the use of this software.

Permission is granted to anyone to use this software for any purpose,
including commercial applications, and to alter it and redistribute it
freely, subject to the following restrictions:

1. The origin of this software must not be misrepresented; you must not
claim that you wrote the original software. If you use this software
in a product, an acknowledgment in the product documentation would be
appreciated but is not required.
2. Altered source versions must be plainly marked as such, and must not be
misrepresented as being the original software.
3. This notice may not be removed or altered from any source distribution.

 */
package org.apache.brooklyn.util.text;

import java.util.Comparator;

/** comparator which takes two strings and puts them in an order 
 * with special rules for numbers (whole number) to be placed in numeric order;
 * e.g. "10">"9", including when those numbers occur in the midst of equal text; e.g. "a10" > "a9";
 * but not if the text differs; e.g. "a10" < "b9".
 * leading zeroes are slightly lower than the same number without the leading zero, so "8" < "09" < "9",
 * but after considering the remainder of the string, so "09.b" > "9.a".
 * decimals are treated like a word-split, not a decimal point, i.e. "1.9" < "1.10".
 * <p>
 * this does not return anything as equal unless they are the same string.
 * <p>
 * class is thread-safe.
 * <p>
 * nulls not supported. to support nulls, wrap in guava:
 * <code>Ordering.from(NaturalOrderComparator.INSTANCE).nullsFirst()</code>)
 * <p>
 * <p>
 * Based on https://github.com/paour/natorder 
 */
public class NaturalOrderComparator implements Comparator<String> {
    
    public static final NaturalOrderComparator INSTANCE = new NaturalOrderComparator();
    
    /** compares a string of digits once 0's have been skipped, stopping when the digits finish */
    int compareRight(String a, String b)
    {
        int bias = 0;
        int ia = 0;
        int ib = 0;

        // The longest run of digits wins.  That aside, the greatest
        // value wins, but we can't know that it will until we've scanned
        // both numbers to know that they have the same magnitude, so we
        // remember it in BIAS.
        for (;; ia++, ib++) {
            char ca = charAt(a, ia);
            char cb = charAt(b, ib);

            if (!Character.isDigit(ca)
                    && !Character.isDigit(cb)) {
                return bias;
            } else if (!Character.isDigit(ca)) {
                return -1;
            } else if (!Character.isDigit(cb)) {
                return +1;
            } else if (ca < cb) {
                if (bias == 0) {
                    bias = -1;
                }
            } else if (ca > cb) {
                if (bias == 0)
                    bias = +1;
            } else if (ca == 0 && cb == 0) {
                return bias;
            }
        }
    }

    @Override
    public int compare(String a, String b) {

        int ia = 0, ib = 0;
        int nza = 0, nzb = 0;
        char ca, cb;
        int result;

        while (true) {
            // only count the number of zeroes leading the last number compared
            nza = nzb = 0;

            ca = charAt(a, ia); cb = charAt(b, ib);

            // skip over leading spaces or zeros
            while (Character.isSpaceChar(ca) || ca == '0') {
                if (ca == '0') {
                    nza++;
                } else {
                    // only count consecutive zeroes
                    nza = 0;
                }

                ca = charAt(a, ++ia);
            }

            while (Character.isSpaceChar(cb) || cb == '0') {
                if (cb == '0') {
                    nzb++;
                } else {
                    // only count consecutive zeroes
                    nzb = 0;
                }

                cb = charAt(b, ++ib);
            }

            // process run of digits
            if (Character.isDigit(ca) && Character.isDigit(cb)) {
                if ((result = compareRight(a.substring(ia), b.substring(ib))) != 0) {
                    return result;
                }
                // numeric portion is the same; previously we incremented and checked again
                // which is inefficient (we keep checking remaining numbers here);
                // also if we want to compare on 0's after comparing remainder of string
                // we need to recurse here after skipping the digits we just checked above
                do {
                    ia++; ib++;
                    boolean aDigitsDone = ia >= a.length() || !Character.isDigit(charAt(a, ia));
                    boolean bDigitsDone = ib >= b.length() || !Character.isDigit(charAt(b, ib));
                    if (aDigitsDone != bDigitsDone) {
                        // sanity check
                        throw new IllegalStateException("Digit sequence lengths should have matched, '"+a+"' v '"+b+"'");
                    }
                    if (aDigitsDone) break;
                } while (true);
                if (ia < a.length() || ib < b.length()) {
                    if (ia >= a.length()) return -1;  // only b has more chars
                    if (ib >= b.length()) return 1;  // only a has more chars
                    // both have remaining chars; neither is numeric due to compareRight; recurse into remaining
                    if ((result = compare(a.substring(ia), b.substring(ib))) != 0) {
                        return result;
                    }
                }
                // numbers are equal value, remaining string compares identical; 
                // comes down to whether there are any leading zeroes here
                return nzb-nza;
                
            } else if ((Character.isDigit(ca) || nza>0) && (Character.isDigit(cb) || nzb>0)) {
                // both sides are numbers, but at least one is a sequence of zeros
                if (nza==0) {
                    // b=0, a>0
                    return 1;
                }
                if (nzb==0) {
                    // inverse
                    return -1;
                }
                // both sides were zero, continue to next check below
            }

            if (ca == 0 && cb == 0) {
                // The strings compare the same.  Perhaps the caller
                // will want to call strcmp to break the tie.
                return nza - nzb;
            }

            if (ca < cb) {
                return -1;
            } else if (ca > cb) {
                return +1;
            }

            ++ia; ++ib;
        }
    }

    static char charAt(String s, int i) {
        if (i >= s.length()) {
            return 0;
        } else {
            return s.charAt(i);
        }
    }

}