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
package org.apache.brooklyn.util.yorml;

public class YormlException extends RuntimeException {

    private static final long serialVersionUID = 7825908737102292499L;
    
    YormlContext context;
    
    public YormlException(String message) { super(message); }
    public YormlException(String message, Throwable cause) { super(message, cause); }
    public YormlException(String message, YormlContext context) { this(message); this.context = context; }
    public YormlException(String message, YormlContext context, Throwable cause) { this(message, cause); this.context = context; }

    public YormlContext getContext() {
        return context;
    }
    
    @Override
    public String toString() {
        if (context==null) return super.toString();
        return super.toString() + " ("+context+")";
    }
    
}
