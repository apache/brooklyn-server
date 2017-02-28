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
package org.apache.brooklyn.util.net;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.net.HostAndPort;

public class NetworkingUtilsTest {

    private static final Logger log = LoggerFactory.getLogger(NetworkingUtilsTest.class);
    
    @Test
    public void testValidIp() throws Exception {
        assertTrue(Networking.isValidIp4("127.0.0.1"));
        assertTrue(Networking.isValidIp4("0.0.0.0"));
        assertFalse(Networking.isValidIp4("foo"));
        assertTrue(Networking.isValidIp4(Networking.LOOPBACK.getHostName()));
        assertTrue(Networking.isValidIp4("0.0.0.00"));
        assertTrue(Networking.isValidIp4("127.0.0.000001"));
        assertFalse(Networking.isValidIp4("127.0.0.256"));
        assertFalse(Networking.isValidIp4("127.0.0."));
        assertFalse(Networking.isValidIp4("127.0.0.9f"));
        assertFalse(Networking.isValidIp4("127.0.0.1."));
        assertFalse(Networking.isValidIp4("255.255.255.255."));
    }

    @Test
    public void testGetInetAddressWithFixedNameByIpBytes() throws Exception {
        InetAddress addr = Networking.getInetAddressWithFixedName(new byte[] { 1, 2, 3, 4 });
        assertEquals(addr.getAddress(), new byte[] { 1, 2, 3, 4 });
        assertEquals(addr.getHostName(), "1.2.3.4");

        InetAddress addr2 = Networking.getInetAddressWithFixedName(new byte[] { (byte) 255, (byte) 255, (byte) 255, (byte) 255 });
        assertEquals(addr2.getAddress(), new byte[] { (byte) 255, (byte) 255, (byte) 255, (byte) 255 });
        assertEquals(addr2.getHostName(), "255.255.255.255");
    }
    
    @Test
    public void testGetInetAddressWithFixedNameByIp() throws Exception {
        InetAddress addr = Networking.getInetAddressWithFixedName("1.2.3.4");
        assertEquals(addr.getAddress(), new byte[] { 1, 2, 3, 4 });
        assertEquals(addr.getHostName(), "1.2.3.4");

        InetAddress addr2 = Networking.getInetAddressWithFixedName("255.255.255.255");
        assertEquals(addr2.getAddress(), new byte[] { (byte) 255, (byte) 255, (byte) 255, (byte) 255 });
        assertEquals(addr2.getHostName(), "255.255.255.255");

        InetAddress addr3 = Networking.getInetAddressWithFixedName("localhost");
        assertEquals(addr3.getHostName(), "localhost");
    }

    @Test
    public void testPortRulesToRanges() throws Exception {
        RangeSet<Integer> actualRangeSet = Networking.portRulesToRanges(ImmutableList.of(
                "22", "23", "5000-6000", "8081", "80-90", "90-100", "23", "8081"));

        Asserts.assertEquals(actualRangeSet, ImmutableRangeSet.<Integer>builder()
                .add(Range.closed(22, 22))
                .add(Range.closed(23, 23))
                .add(Range.closed(80, 100))
                .add(Range.closed(5000, 6000))
                .add(Range.closed(8081, 8081))
                .build());
    }

    @Test
    public void testPortRulesToRangesWithInvalidRanges() throws Exception {
        assertPortRulesException(ImmutableList.of("-1"), "Invalid port range '-1'");
        assertPortRulesException(ImmutableList.of("1-"), "Invalid port range '1-'");
        assertPortRulesException(ImmutableList.of("-"), "Invalid port range '-'");
        assertPortRulesException(ImmutableList.of("1-2-"), "Invalid port range '1-2-'");
        assertPortRulesException(ImmutableList.of("-1-2"), "Invalid port range '-1-2'");
        assertPortRulesException(ImmutableList.of("1-2-3"), "Invalid port range '1-2-3'");
        assertPortRulesException(ImmutableList.of("a"), NumberFormatException.class, "For input string: \"a\"");
        assertPortRulesException(ImmutableList.of("1-a"), NumberFormatException.class, "For input string: \"a\"");
        assertPortRulesException(ImmutableList.of("a-2"), NumberFormatException.class, "For input string: \"a\"");
    }

    @Test
    public void testPortRulesToRangesWithOutOfRange() throws Exception {
        assertPortRulesException(ImmutableList.of("68000"), String.format("fromPort 68000 should be a number between %s and %s",
                Networking.MIN_PORT_NUMBER, Networking.MAX_PORT_NUMBER));
        assertPortRulesException(ImmutableList.of("67000-67000"), "fromPort 67000 should be a number between 1 and 65535");
        assertPortRulesException(ImmutableList.of("0-67000"), "fromPort 0 should be a number between 1 and 65535");
        assertPortRulesException(ImmutableList.of("300-67000"), "toPort 67000 should be a number between 1 and 65535");
        assertPortRulesException(ImmutableList.of("2-1"), "fromPort 2 should be less than or equal to toPort 1");
    }

    private void assertPortRulesException(Collection<String> portRules, String message) {
        assertPortRulesException(portRules, IllegalArgumentException.class, message);
    }
    
    private void assertPortRulesException(Collection<String> portRules, Class<? extends Exception> type, String message) {
        try {
            Networking.portRulesToRanges(portRules);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            if (!type.isInstance(e)) throw e;
            Asserts.expectedFailureContains(e, message);
        }
    }
    
    @Test(groups="Integration")
    public void testGetInetAddressWithFixedNameButInvalidIpThrowsException() throws Exception {
        // as with ByonLocationResolverTest.testNiceError
        // some DNS servers give an IP for this "hostname"
        // so test is marked as integration now
        try {
            Networking.getInetAddressWithFixedName("1.2.3.400");
            fail();
        } catch (Exception e) {
            if (Exceptions.getFirstThrowableOfType(e, UnknownHostException.class) == null) throw e;
        }
    }
    
    @Test
    public void testIsPortAvailableReportsTrueWhenPortIsFree() throws Exception {
        final int MIN_FREE = 5;
        int port = 58769;
        int numFree = 0;
        for (int i = 0; i < 50 && numFree < MIN_FREE; i++) {
            if (Networking.isPortAvailable(port+i))
                numFree++;
        }
        if (numFree < MIN_FREE)
            fail("This test requires that at least some ports near "+port+"+ not be in use.");
    }

    @Test
    public void testIsPortAvailableReportsFalseWhenPortIsInUse() throws Exception {
        int port = 58767;
        ServerSocket ss = null;
        do {
            port ++;
            if (Networking.isPortAvailable(port)) {
                try {
                    ss = new ServerSocket(port);
                    log.info("acquired port on "+port+" for test "+JavaClassNames.niceClassAndMethod());
                    assertFalse(Networking.isPortAvailable(port), "port mistakenly reported as available");
                } finally {
                    if (ss != null) {
                        ss.close();
                    }
                }
            }
            // repeat until we can get a port
        } while (ss == null && port < 60000);
        Assert.assertNotNull(ss, "could not get a port");
        
        final int portF = port;
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                assertTrue(Networking.isPortAvailable(portF), "port "+portF+" not made available afterwards");
            }});
    }

    @Test
    public void testIsPortAvailableReportsPromptly() throws Exception {
        // repeat until we can get an available port
        int port = 58767;
        boolean available = false;
        do {
            port++;
            Stopwatch watch = Stopwatch.createStarted();
            if (Networking.isPortAvailable(null, port)) {
                available = true;
            }
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            assertTrue(elapsedMillis < 5000, "elapsedMillis="+elapsedMillis+" for isPortAvailable(null, "+port+")");
        } while (!available && port < 60000);

        Assert.assertTrue(available);
    }

    @Test
    public void testIsPortAvailableValidatesAddress() throws Exception {
        ServerSocket ss = new ServerSocket();
        ss.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        int boundPort = ss.getLocalPort();
        assertTrue(ss.isBound());
        assertNotEquals(boundPort, 0);
        //will run isAddressValid before returning
        assertFalse(Networking.isPortAvailable(boundPort));
        ss.close();
    }
    
    @Test
    public void testIsPortAvailableTimeWait() throws Exception {
        final ServerSocket ss = new ServerSocket();
        boolean reuse = ss.getReuseAddress();
        // Default is "true" on macOS & Linux
        log.info("Platform default SO_REUSEADDR is " + reuse);
        ss.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        int boundPort = ss.getLocalPort();
        assertTrue(ss.isBound());
        assertNotEquals(boundPort, 0);

        assertFalse(Networking.isPortAvailable(boundPort));
        connectAndClose(ss);
        ss.close();

        Networking.nextAvailablePort(boundPort);
        ServerSocket ff = new ServerSocket();
        ff.bind(new InetSocketAddress(InetAddress.getLocalHost(), boundPort));
        ff.close();

        try {
            Socket s = new Socket();
            s.bind(ss.getLocalSocketAddress());
            s.close();
            Asserts.shouldHaveFailedPreviously("Bind should fail because of TIME_WAIT");
        } catch (BindException e) {
            // expected
        }

        // Because of TIME_WAIT - it's pretty long so the test shouldn't be time sensitive
        assertFalse(Networking.isPortAvailable(boundPort));

        int nextAvailable = Networking.nextAvailablePort(boundPort);
        assertNotEquals(nextAvailable, boundPort);
    }

    protected void connectAndClose(final ServerSocket ss) throws IOException, InterruptedException, ExecutionException {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Socket> client = exec.submit(new Callable<Socket>() {
            @Override
            public Socket call() throws Exception {
                Socket client = new Socket();
                client.connect(ss.getLocalSocketAddress());
                return client;
            }
        });
        Socket server = ss.accept();
        server.close();
        client.get().close();
        exec.shutdown();
    }
    
    //just some system health-checks... localhost may not resolve properly elsewhere
    //(e.g. in geobytes, AddressableLocation, etc) if this does not work
    
    @Test
    public void testLocalhostIpLookup() throws UnknownHostException {
        InetAddress address = InetAddress.getByName("127.0.0.1");
        Assert.assertEquals(127, address.getAddress()[0]);
        Assert.assertTrue(Networking.isPrivateSubnet(address));
    }
    
    @Test
    public void testLocalhostLookup() throws UnknownHostException {
        InetAddress address = InetAddress.getByName("localhost");
        Assert.assertEquals(127, address.getAddress()[0]);
        Assert.assertTrue(Networking.isPrivateSubnet(address));
        Assert.assertEquals("127.0.0.1", address.getHostAddress());
    }

    @Test
    public void test10_x_x_xSubnetPrivate() throws UnknownHostException {
        InetAddress address = InetAddress.getByAddress(new byte[] { 10, 0, 0, 1 });
        Assert.assertTrue(Networking.isPrivateSubnet(address));
    }

    @Test
    public void test172_16_x_xSubnetPrivate() throws UnknownHostException {
        InetAddress address = InetAddress.getByAddress(new byte[] { (byte)172, 31, (byte)255, (byte)255 });
        Assert.assertTrue(Networking.isPrivateSubnet(address));
    }

    @Test(groups="Integration")
    public void testBogusHostnameUnresolvable() {
        Assert.assertEquals(Networking.resolve("bogus-hostname-"+Identifiers.makeRandomId(8)), null);
    }

    @Test(groups="Integration")
    public void testIsReachable() throws Exception {
        ServerSocket serverSocket = null;
        for (int i = 40000; i < 40100; i++) {
            try {
                serverSocket = new ServerSocket(i);
            } catch (IOException e) {
                // try next number
            }
        }
        assertNotNull(serverSocket, "No ports available in range!");
        
        try {
            HostAndPort hostAndPort = HostAndPort.fromParts(serverSocket.getInetAddress().getHostAddress(), serverSocket.getLocalPort());
            assertTrue(Networking.isReachable(hostAndPort));
            
            serverSocket.close();
            assertFalse(Networking.isReachable(hostAndPort));
            
        } finally {
            serverSocket.close();
        }
    }
}
