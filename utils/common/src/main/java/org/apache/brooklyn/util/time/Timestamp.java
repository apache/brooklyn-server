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
package org.apache.brooklyn.util.time;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

/** This wrapper around Date making it better suited to serialization etc */
public class Timestamp extends Date {

    public Timestamp(Date d) { super(d.getTime()); }
    public Timestamp(Calendar d) { this(d.getTime()); }
    public Timestamp(Instant d) { this(d.toEpochMilli()); }
    /** takes milliseconds since epoch */
    public Timestamp(long l) {
        this(new Calendar.Builder().setInstant(l).build());
    }
    /** parses many common formats, as per {@link Time#parseCalendar(String)} */
    public Timestamp(String s) {
        this(Time.parseCalendar(s));
    }


    /** json constructor */
    private Timestamp() {
        this(Calendar.getInstance());
    }

    public static Timestamp fromString(String s) { return new Timestamp(s); }

    public Calendar toCalendar() {
        return new Calendar.Builder().setInstant(this).build();
    }

    @Override
    public String toString() {
        return Time.makeDateString(this);
    }

}
