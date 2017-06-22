/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.osgi;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;

import com.google.common.base.Objects;

/** Records a name (string) and version (string),
 * with conveniences for pretty-printing and converting to OSGi format. */
public class VersionedName {
    private final String name;
    private final String v;
    
    @Deprecated // since 0.12.0 - remove along with version and readResolve in 0.13.0
    private final String symbolicName = null;
    @Deprecated // since 0.12.0
    private final Version version = null;
    

    private Object readResolve() {
        if (symbolicName!=null || version!=null) {
            // remove legacy fields, and convert to brooklyn recommended version in the process
            // (might be slightly weird if bundles are persisted, but code will forgive that,
            // and if types were persisted this will do the right thing)
            return new VersionedName(symbolicName, BrooklynVersionSyntax.toGoodBrooklynVersion( version.toString() ));
        }
        return this;
    }
    
    public VersionedName(Bundle b) {
        this(b.getSymbolicName(), b.getVersion().toString());
    }

    public VersionedName(String name, String v) {
        this.name = checkNotNull(name, "name");
        this.v = v;
    }
    public VersionedName(String name, @Nullable Version v) {
        this.name = checkNotNull(name, "name").toString();
        this.v = v==null ? null : v.toString();
    }

    
    @Override
    public String toString() {
        return name + ":" + v;
    }
    
    public String toOsgiString() {
        return name + ":" + getOsgiVersion();
    }

    public boolean equals(String sn, String v) {
        return name.equals(sn) && Objects.equal(this.v, v);
        // could also consider equal if the osgi mapping is equal (but for now we don't; if caller wants that, they should create Version)
        // || (v!=null && Version.parseVersion(BrooklynVersionSyntax.toValidOsgiVersion(v)).equals(getOsgiVersion())));
    }

    public boolean equals(String sn, Version v) {
        return name.equals(sn) && Objects.equal(getOsgiVersion(), v);
    }

    public String getSymbolicName() {
        return name;
    }

    private transient Version cachedOsgiVersion;
    @Nullable
    public Version getOsgiVersion() {
        if (cachedOsgiVersion==null && v!=null) {
            cachedOsgiVersion = v==null ? null : Version.parseVersion(BrooklynVersionSyntax.toValidOsgiVersion(v));
        }
        return cachedOsgiVersion;
    }

    @Nullable
    public String getVersionString() {
        return v;
    }
    
    @Deprecated /** @deprecated since 0.12.0 use {@link #getVersionString()} or {@link #getOsgiVersion()} */
    public Version getVersion() {
        return getOsgiVersion();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, v);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof VersionedName)) {
            return false;
        }
        VersionedName o = (VersionedName) other;
        return Objects.equal(name, o.name) && Objects.equal(v, o.v);
    }
    
    /** As {@link #equals(Object)} but accepting the argument as equal if versions are identical when injected to OSGi-valid versions */
    public boolean equalsOsgi(Object other) {
        if (!(other instanceof VersionedName)) {
            return false;
        }
        VersionedName o = (VersionedName) other;
        return Objects.equal(name, o.name) && Objects.equal(getOsgiVersion(), o.getOsgiVersion());        
    }

    public static VersionedName fromString(String nameOptionalColonVersion) {
        if (nameOptionalColonVersion==null) return null;
        int colon = nameOptionalColonVersion.indexOf(':');
        if (colon<0) throw new IllegalArgumentException("Versioned name '"+nameOptionalColonVersion+"' must be of form 'name:version'");
        return new VersionedName(nameOptionalColonVersion.substring(0, colon), nameOptionalColonVersion.substring(colon+1));
    }

}
