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
package org.apache.brooklyn.util.math;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

public class BitUtils {

    /** reverses the bits in a byte, i.e.  128 = 0b1000000 = bit list {0,0,0,0,0,0,0,1},
     * reversed yields 1 = 0b00000001 = bit list {1,0,0,0,0,0,0,0} */
    public static byte reverseBitSignificance(byte b) {
        return (byte) (Integer.reverse(b) >> 24);
    }

    /** as {@link #reverseBitSignificance(byte)} but accepting int for convenience */
    public static byte reverseBitSignificanceInByte(int b) {
        return reverseBitSignificance((byte)b);
    }

    /** returns an array of bytes where the bits in each byte have been reversed;
     * note however the order of the arguments is not reversed;
     * useful e.g. in working with IP address CIDR's */
    public static byte[] reverseBitSignificance(byte ...bytes) {
        byte[] result = new byte[bytes.length];
        for (int i=0; i<bytes.length; i++)
            result[i] = reverseBitSignificance(bytes[i]);
        return result;
    }

    /** as {@link #reverseBitSignificance(byte...)}, but taking ints for convenience (ignoring high bits) */
    public static byte[] reverseBitSignificanceInBytes(int ...bytes) {
        return reverseBitSignificance(Bytes.toArray(Ints.asList(bytes)));
    }

    /** why oh why are bytes signed! */
    public static int unsigned(byte b) {
        return b & 0xff;
    }

    /** returns the value in 0..255 which is equivalent mod 256 */
    public static int unsignedByte(int b) {
        return b & 0xff;
    }

}
