### OSGi shenaningans

Not everything is built for OSGi, thus to be able to upgrade dependencies and upgrade Apache Brooklyn here and there, some tricky things must be done.

Section (1) from `feature.xml` : 
```
<!-- jclouds and other things declare a dependency on this but don't actually need it at runtime.
     but this _does_ need it pulled in. it wants 3.0.1, jclouds wants 2.0.2.
     2.0.2 fails with the wrong Bundle-ManifestVersion: 1
     3.0.1 and 3.0.2 cause startup to  trying to resolve packagesckages
    <bundle dependency='true'>mvn:com.google.code.findbugs/jsr305/3.0.2</bundle>
-->

<!-- unique to this, mostly, it seems -->
<bundle dependency='true'>wrap:mvn:dk.brics.automaton/automaton/1.11-8</bundle>
<!-- careful with this one. we might be able to use the original mvn:com.google.code.findbugs/jsr305/3.0.2 but
     often it blocked startup of features while it tried to resolve packages, possibly just because dk.brics was missing,
     but possibly also because javax.annotation is exported by multiple suspects;
     also note the v2 of that findbugs gives failues re 'Bundle-ManifestVersion: 1'
     (and though that v2 is referenced by jclouds it isn't actually needed);
     the servicemix package seems to work so we're using it, at latest version
-->
```

Section (2) from `feature.xml` : 
```
<!-- unwrapped complains about OSGi R3 bundle not supported -->
<!-- test with
    bundle:install -s wrap:mvn:com.squareup.okio/okio/1.15.0\$Bundle-SymbolicName=squareup-okio\&Bundle-Version=1.15.0
    bundle:install -s wrap:mvn:com.squareup.okio/okio/1.15.0\$Bundle-SymbolicName=squareup-okio\&Bundle-Version=1.15.0\&Export-Package=okio\;version=1.15.0
-->
```

Section (3) from `feature.xml` : 
```
<!-- test with
    // require-bundle doesn't work
    bundle:install -s wrap:mvn:com.squareup.okhttp3/okhttp/3.12.6\$Bundle-SymbolicName=squareup-okhttp3\&Bundle-Version=3.12.6\&Require-Bundle=squareup-okio
    bundle:install -s wrap:mvn:com.squareup.okhttp3/okhttp/3.12.6\$Bundle-SymbolicName=squareup-okhttp3\&Bundle-Version=3.12.6\&Import-Package=okio\;version=1.15.0,javax.annotation.meta,javax.annotation,javax.net.ssl,javax.net,javax.security.auth.x500,org.conscrypt
    bundle:install -s wrap:mvn:com.squareup.okhttp3/okhttp/3.12.6\$Bundle-SymbolicName=squareup-okhttp3\&Bundle-Version=3.12.6\&Import-Package=okio\;version=1.15.0,javax.annotation,javax.net.ssl,javax.net,javax.security.auth.x500,*
    bundle:install -s wrap:mvn:com.squareup.okhttp3/okhttp/3.12.6\$Bundle-SymbolicName=squareup-okhttp3\&Bundle-Version=3.12.6\&Import-Package=okio\;version=1.15.0,*\;resolution:=optional
-->
```

Section (4) from `feature.xml` : 
```
<!-- later versions, but we aren't interested
    <bundle dependency='true'>wrap:mvn:com.squareup.okio/okio/1.17.5</bundle>
    <bundle dependency='true'>wrap:mvn:com.squareup.okhttp3/okhttp/3.14.7</bundle>
 -->
<!-- servicemix variant but it complains about getting javax.annotation from two places
    <bundle dependency='true'>wrap:mvn:org.apache.servicemix.bundles/org.apache.servicemix.bundles.okhttp/3.14.1_1</bundle>
-->
```