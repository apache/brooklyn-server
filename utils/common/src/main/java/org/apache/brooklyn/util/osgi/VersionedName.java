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

import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.apache.brooklyn.util.text.Strings;
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
    public String getOsgiVersionString() {
        Version ov = getOsgiVersion();
        if (ov==null) return null;
        return ov.toString();
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
    
    /** As {@link #equals(Object)} but accepting the argument as equal 
     * if versions are identical under the {@link #getOsgiVersion()} conversion;
     * also accepts strings as the other, converting as per {@link #fromString(String)} */
    public boolean equalsOsgi(Object other) {
        if (other instanceof String) {
            other = VersionedName.fromString((String)other);
        }
        if (!(other instanceof VersionedName)) {
            return false;
        }
        VersionedName o = (VersionedName) other;
        return Objects.equal(name, o.name) && Objects.equal(getOsgiVersion(), o.getOsgiVersion());        
    }

    /** Returns a {@link VersionedName} instance where {@link #getVersionString()} complies with OSGi syntax (or is null), 
     * even if the input does not. 
     * Typically used to convert from Brooklyn-recommended <code>1.0-SNAPSHOT</code> syntax to 
     * OSGi-mandated <code>1.0.0.SNAPSHOT</code> syntax. */
    public static VersionedName toOsgiVersionedName(VersionedName vn) {
        if (vn==null) return null;
        return new VersionedName(vn.getSymbolicName(), vn.getOsgiVersion());
    }

    /** As {@link #parseMaybe(String)} but throwing if invalid; allows null version */
    public static VersionedName fromString(String vn) {
        return parseMaybe(vn, false).get();
    }
    
    /** Takes a string which might be of the form "symbolic-name" or "symbolic-name:version" (or something else entirely)
     * and returns a VersionedName. The versionedName.getVersion() will be null if if there was no version in the input
     * and the second argument is false, versions not required.
     * Returns a {@link Maybe#absent()} with a suitable message if not valid. */
    public static Maybe<VersionedName> parseMaybe(String symbolicNameWithVersion, boolean versionRequired) {
        if (Strings.isBlank(symbolicNameWithVersion)) {
            return Maybe.absent("Identifier is blank");
        }
        String[] parts = symbolicNameWithVersion.split(":");
        if (versionRequired && parts.length!=2) {
            return Maybe.absent("Identifier '"+symbolicNameWithVersion+"' must be of 'name:version' syntax");
        }
        if (parts.length > 2) {
            return Maybe.absent("Identifier '"+symbolicNameWithVersion+"' has too many parts; max one ':' symbol");
        }
        return Maybe.of(new VersionedName(parts[0], parts.length == 2 ? parts[1] : null));
    }

}
