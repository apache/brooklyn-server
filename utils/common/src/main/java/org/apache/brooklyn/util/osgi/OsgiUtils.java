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

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.Version;

/**
 * Simple OSGi utilities.
 * 
 * @author Ciprian Ciubotariu <cheepeero@gmx.net>
 */
public class OsgiUtils {

    public static URL getContainerUrl(URL url, String resourceInThatDir) {
        //Switching from manual parsing of jar: and file: URLs to java provided functionality.
        //The old code was breaking on any Windows path and instead of fixing it, using
        //the provided Java APIs seemed like the better option since they are already tested
        //on multiple platforms.
        boolean isJar = "jar".equals(url.getProtocol());
        if(isJar) {
            try {
                //let java handle the parsing of jar URL, no network connection is established.
                //Strips the jar protocol:
                //  jar:file:/<path to jar>!<resourceInThatDir>
                //  becomes
                //  file:/<path to jar>
                JarURLConnection connection = (JarURLConnection) url.openConnection();
                url = connection.getJarFileURL();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        } else {
            //Remove the trailing resouceInThatDir path from the URL, thus getting the parent folder.
            String path = url.toString();
            int i = path.indexOf(resourceInThatDir);
            if (i==-1) throw new IllegalStateException("Resource path ("+resourceInThatDir+") not in url substring ("+url+")");
            String parent = path.substring(0, i);
            try {
                url = new URL(parent);
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Resource ("+resourceInThatDir+") found at invalid URL parent (" + parent + ")", e);
            }
        }
        return url;
    }

    public static String getVersionedId(Manifest manifest) {
        Attributes atts = manifest.getMainAttributes();
        return atts.getValue(Constants.BUNDLE_SYMBOLICNAME) + ":" + atts.getValue(Constants.BUNDLE_VERSION);
    }

    public static String getVersionedId(Bundle b) {
        return b.getSymbolicName() + ":" + b.getVersion();
    }

    /** Takes a string which might be of the form "symbolic-name" or "symbolic-name:version" (or something else entirely)
     * and returns a VersionedName. The versionedName.getVersion() will be null if if there was no version in the input
     * (or returning {@link Maybe#absent()} if not valid, with a suitable error message). */
    public static Maybe<VersionedName> parseOsgiIdentifier(String symbolicNameOptionalWithVersion) {
        if (Strings.isBlank(symbolicNameOptionalWithVersion)) {
            return Maybe.absent("OSGi identifier is blank");
        }
        String[] parts = symbolicNameOptionalWithVersion.split(":");
        if (parts.length > 2) {
            return Maybe.absent("OSGi identifier has too many parts; max one ':' symbol");
        }
        Version v = null;
        if (parts.length == 2) {
            try {
                v = Version.parseVersion(parts[1]);
            } catch (IllegalArgumentException e) {
                return Maybe.absent("OSGi identifier has invalid version string (" + e.getMessage() + ")");
            }
        }
        return Maybe.of(new VersionedName(parts[0], v));
    }

    public static String toOsgiVersion(String version) {
        if (version != null) {
            return DefaultMaven2OsgiConverter.cleanupVersion(version);
        } else {
            return null;
        }
    }

    // Maven to OSGi version converter
    // Source: https://github.com/apache/felix/blob/trunk/tools/maven-bundle-plugin/src/main/java/org/apache/maven/shared/osgi/DefaultMaven2OsgiConverter.java
    private static class DefaultMaven2OsgiConverter {
        /**
         * Clean up version parameters. Other builders use more fuzzy definitions of
         * the version syntax. This method cleans up such a version to match an OSGi
         * version.
         *
         * @param VERSION_STRING
         * @return
         */
        static final Pattern FUZZY_VERSION = Pattern.compile( "(\\d+)(\\.(\\d+)(\\.(\\d+))?)?([^a-zA-Z0-9](.*))?",
            Pattern.DOTALL );
    
    
        static public String cleanupVersion( String version )
        {
            StringBuffer result = new StringBuffer();
            Matcher m = FUZZY_VERSION.matcher( version );
            if ( m.matches() )
            {
                String major = m.group( 1 );
                String minor = m.group( 3 );
                String micro = m.group( 5 );
                String qualifier = m.group( 7 );
    
                if ( major != null )
                {
                    result.append( major );
                    if ( minor != null )
                    {
                        result.append( "." );
                        result.append( minor );
                        if ( micro != null )
                        {
                            result.append( "." );
                            result.append( micro );
                            if ( qualifier != null )
                            {
                                result.append( "." );
                                cleanupModifier( result, qualifier );
                            }
                        }
                        else if ( qualifier != null )
                        {
                            result.append( ".0." );
                            cleanupModifier( result, qualifier );
                        }
                        else
                        {
                            result.append( ".0" );
                        }
                    }
                    else if ( qualifier != null )
                    {
                        result.append( ".0.0." );
                        cleanupModifier( result, qualifier );
                    }
                    else
                    {
                        result.append( ".0.0" );
                    }
                }
            }
            else
            {
                result.append( "0.0.0." );
                cleanupModifier( result, version );
            }
            return result.toString();
        }
    
        static void cleanupModifier( StringBuffer result, String modifier )
        {
            for ( int i = 0; i < modifier.length(); i++ )
            {
                char c = modifier.charAt( i );
                if ( ( c >= '0' && c <= '9' ) || ( c >= 'a' && c <= 'z' ) || ( c >= 'A' && c <= 'Z' ) || c == '_'
                    || c == '-' )
                    result.append( c );
                else
                    result.append( '_' );
            }
        }
    }
}
