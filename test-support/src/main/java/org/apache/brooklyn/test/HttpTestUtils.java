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
package org.apache.brooklyn.test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Utility methods to aid testing HTTP.
 * 
 * @author aled
 *
 * @deprecated since 0.9.0. Prefer {@link org.apache.brooklyn.util.http.HttpAsserts} which has no TestNG dependencies
 * (or {@link HttpTool} for some utility methods).
 */
@Deprecated
public class HttpTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTestUtils.class);

    static final ExecutorService executor = Executors.newCachedThreadPool();

    @Deprecated /** @deprecated since 1.1, refer to (and replace per) method in HttpTool */
    public static URLConnection connectToUrl(String u) throws Exception {
        return HttpTool.connectToUrlUnsafe(u);
    }

    public static void assertHealthyStatusCode(int code) {
        if (code>=200 && code<=299) return;
        Assert.fail("Wrong status code: "+code);
    }

    @Deprecated /** @deprecated since 1.1, refer to (and replace per) method in HttpTool */
    public static int getHttpStatusCode(String url) throws Exception {
        return HttpTool.getHttpStatusCodeUnsafe(url);
    }

    /**
     * Asserts that gets back any "valid" response - i.e. not an exception. This could be an unauthorized,
     * a redirect, a 404, or anything else that implies there is web-server listening on that port.
     */
    public static void assertUrlReachable(String url) {
        try {
            HttpTool.getHttpStatusCodeUnsafe(url);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted for "+url+" (in assertion that is reachable)", e);
        } catch (Exception e) {
            throw new IllegalStateException("Server at "+url+" failed to respond (in assertion that is reachable): "+e, e);
        }
    }

    public static void assertUrlUnreachable(String url) {
        try {
            int statusCode = HttpTool.getHttpStatusCodeUnsafe(url);
            fail("Expected url "+url+" unreachable, but got status code "+statusCode);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted for "+url+" (in assertion that unreachable)", e);
        } catch (Exception e) {
            IOException cause = Exceptions.getFirstThrowableOfType(e, IOException.class);
            if (cause != null) {
                // success; clean shutdown transitioning from 400 to error
            } else {
                Throwables.propagate(e);
            }                        
        }
    }

    public static void assertUrlUnreachableEventually(final String url) {
        assertUrlUnreachableEventually(MutableMap.<String, Object>of(), url);
    }
    
    public static void assertUrlUnreachableEventually(Map<String,?> flags, final String url) {
        Asserts.succeedsEventually(flags, new Runnable() {
            @Override
            public void run() {
                assertUrlUnreachable(url);
            }
         });
    }

    public static void assertHttpStatusCodeEquals(String url, int... acceptableReturnCodes) {
        List<Integer> acceptableCodes = Lists.newArrayList();
        for (int code : acceptableReturnCodes) {
            acceptableCodes.add(code);
        }
        try {
            int actualCode = HttpTool.getHttpStatusCodeUnsafe(url);
            assertTrue(acceptableCodes.contains(actualCode), "code="+actualCode+"; expected="+acceptableCodes+"; url="+url);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted for "+url+" (in assertion that result code is "+acceptableCodes+")", e);
        } catch (Exception e) {
            throw new IllegalStateException("Server at "+url+" failed to respond (in assertion that result code is "+acceptableCodes+"): "+e, e);
        }
    }

    public static void assertHttpStatusCodeEventuallyEquals(final String url, final int expectedCode) {
        assertHttpStatusCodeEventuallyEquals(MutableMap.<String, Object>of(),  url, expectedCode);
    }

    public static void assertHttpStatusCodeEventuallyEquals(Map<String,?> flags, final String url, final int expectedCode) {
        Asserts.succeedsEventually(flags, new Runnable() {
            @Override
            public void run() {
                assertHttpStatusCodeEquals(url, expectedCode);
            }
         });
    }

    public static void assertContentContainsText(final String url, final String phrase, final String ...additionalPhrases) {
        try {
            String contents = HttpTool.getContentUnsafe(url);
            Assert.assertTrue(contents != null && contents.length() > 0);
            for (String text: Lists.asList(phrase, additionalPhrases)) {
                if (!contents.contains(text)) {
                    LOG.warn("CONTENTS OF URL "+url+" MISSING TEXT: "+text+"\n"+contents);
                    Assert.fail("URL "+url+" does not contain text: "+text);
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static void assertContentNotContainsText(final String url, final String phrase, final String ...additionalPhrases) {
        try {
            String contents = HttpTool.getContentUnsafe(url);
            Assert.assertTrue(contents != null);
            for (String text: Lists.asList(phrase, additionalPhrases)) {
                if (contents.contains(text)) {
                    LOG.warn("CONTENTS OF URL "+url+" HAS TEXT: "+text+"\n"+contents);
                    Assert.fail("URL "+url+" contain text: "+text);
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static void assertErrorContentContainsText(final String url, final String phrase, final String ...additionalPhrases) {
        try {
            String contents = HttpTool.getErrorContent(url);
            Assert.assertTrue(contents != null && contents.length() > 0);
            for (String text: Lists.asList(phrase, additionalPhrases)) {
                if (!contents.contains(text)) {
                    LOG.warn("CONTENTS OF URL "+url+" MISSING TEXT: "+text+"\n"+contents);
                    Assert.fail("URL "+url+" does not contain text: "+text);
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }


    public static void assertErrorContentNotContainsText(final String url, final String phrase, final String ...additionalPhrases) {
        try {
            String err = HttpTool.getErrorContent(url);
            Assert.assertTrue(err != null);
            for (String text: Lists.asList(phrase, additionalPhrases)) {
                if (err.contains(text)) {
                    LOG.warn("CONTENTS OF URL "+url+" HAS TEXT: "+text+"\n"+err);
                    Assert.fail("URL "+url+" contain text: "+text);
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
    
    public static void assertContentEventuallyContainsText(final String url, final String phrase, final String ...additionalPhrases) {
        assertContentEventuallyContainsText(MutableMap.<String, Object>of(), url, phrase, additionalPhrases);
    }
    
    public static void assertContentEventuallyContainsText(Map<String,?> flags, final String url, final String phrase, final String ...additionalPhrases) {
        Asserts.succeedsEventually(flags, new Runnable() {
            @Override
            public void run() {
                assertContentContainsText(url, phrase, additionalPhrases);
            }
         });
    }
    
    public static void assertContentMatches(String url, String regex) {
        String contents = HttpTool.getContentUnsafe(url);
        Assert.assertNotNull(contents);
        Assert.assertTrue(contents.matches(regex), "Contents does not match expected regex ("+regex+"): "+contents);
    }

    public static void assertContentEventuallyMatches(final String url, final String regex) {
        assertContentEventuallyMatches(MutableMap.<String, Object>of(), url, regex);
    }

    public static void assertContentEventuallyMatches(Map<String,?> flags, final String url, final String regex) {
        Asserts.succeedsEventually(flags, new Runnable() {
            @Override
            public void run() {
                assertContentMatches(url, regex);
            }
        });
    }

    @Deprecated /** @deprecated since 1.1, refer to (and replace per) method in HttpTool */
    public static String getErrorContent(String url) {
        try {
            HttpURLConnection connection = (HttpURLConnection) connectToUrl(url);
            long startTime = System.currentTimeMillis();
            
            String err;
            int status;
            try {
                InputStream errStream = connection.getErrorStream();
                err = Streams.readFullyStringAndClose(errStream);
                status = connection.getResponseCode();
            } finally {
                closeQuietly(connection);
            }
            
            if (LOG.isDebugEnabled())
                LOG.debug("read of err {} ({}ms) complete; http code {}", new Object[] { url, Time.makeTimeStringRounded(System.currentTimeMillis()-startTime), status});
            return err;

        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    @Deprecated /** @deprecated since 1.1, refer to (and replace per) method in HttpTool */
    public static String getContent(String url) {
        return HttpTool.getContentUnsafe(url);
    }

    /**
     * Schedules (with the given executor) a poller that repeatedly accesses the given url, to confirm it always gives
     * back the expected status code.
     * 
     * Expected usage is to query the future, such as:
     * 
     * <pre>
     * {@code
     * Future<?> future = assertAsyncHttpStatusCodeContinuallyEquals(executor, url, 200);
     * // do other stuff...
     * if (future.isDone()) future.get(); // get exception if it's failed
     * }
     * </pre>
     * 
     * For stopping it, you can either do future.cancel(true), or you can just do executor.shutdownNow().
     * 
     * TODO Look at difference between this and WebAppMonitor, to decide if this should be kept.
     */
    public static ListenableFuture<?> assertAsyncHttpStatusCodeContinuallyEquals(ListeningExecutorService executor, final String url, final int expectedStatusCode) {
        return executor.submit(new Runnable() {
            @Override public void run() {
                // TODO Need to drop logging; remove sleep when that's done.
                while (!Thread.currentThread().isInterrupted()) {
                    assertHttpStatusCodeEquals(url, expectedStatusCode);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return; // graceful return
                    }
                }
            }
        });
    }
    
    /**
     * Consumes the input stream entirely and then cleanly closes the connection.
     * Ignores all exceptions completely, not even logging them!
     * 
     * Consuming the stream fully is useful for preventing idle TCP connections. 
     * See http://docs.oracle.com/javase/8/docs/technotes/guides/net/http-keepalive.html
     */
    public static void consumeAndCloseQuietly(HttpURLConnection connection) {
        try { Streams.readFully(connection.getInputStream()); } catch (Exception e) {}
        closeQuietly(connection);
    }
    
    /**
     * Closes all streams of the connection, and disconnects it. Ignores all exceptions completely,
     * not even logging them!
     */
    public static void closeQuietly(HttpURLConnection connection) {
        try { connection.disconnect(); } catch (Exception e) {}
        try { connection.getInputStream().close(); } catch (Exception e) {}
        try { connection.getOutputStream().close(); } catch (Exception e) {}
        try { connection.getErrorStream().close(); } catch (Exception e) {}
    }
}
