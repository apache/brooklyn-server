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
package org.apache.brooklyn.rest.security.provider;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class BrooklynUserWithRandomPasswordSecurityProviderTest {
    private static BrooklynUserWithRandomPasswordSecurityProvider provider;
    private static String generatedPassVal;

    @BeforeClass
    public static void setUp() throws NoSuchFieldException, IllegalAccessException {
        provider = new BrooklynUserWithRandomPasswordSecurityProvider();

        Field generatedPassField = BrooklynUserWithRandomPasswordSecurityProvider.class.getDeclaredField("password");
        generatedPassField.setAccessible(true);
        generatedPassVal = (String)generatedPassField.get(provider);
    }

    @Test
    public void testRemoteWithWrongPassword() throws SecurityProvider.SecurityProviderDeniedAuthentication {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpSession session = Mockito.mock(HttpSession.class);
        when(request.getSession()).thenReturn(session);

        // failure from remote and wrong password
        when(request.getRemoteAddr()).thenReturn("5.161.89.199");
        Assert.assertFalse(provider.authenticate(request, request::getSession, "brooklyn", "whatever"));
    }

    @Test
    public void testRemoteWithCorrectPassword() throws SecurityProvider.SecurityProviderDeniedAuthentication {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpSession session = Mockito.mock(HttpSession.class);
        when(request.getSession()).thenReturn(session);

        when(request.getRemoteAddr()).thenReturn("5.161.89.199");
        doNothing().when(session).setAttribute(any(String.class), any(String.class));
        Assert.assertTrue(provider.authenticate(request, request::getSession, "brooklyn", generatedPassVal));
    }

    @Test
    public void testLocalWithAnyPassword() throws SecurityProvider.SecurityProviderDeniedAuthentication {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpSession session = Mockito.mock(HttpSession.class);
        when(request.getSession()).thenReturn(session);

        // passing from local and wrong password
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        Assert.assertTrue(provider.authenticate(request, request::getSession, "brooklyn", "whatever"));
        // passing from local and right password
        Assert.assertTrue(provider.authenticate(request, request::getSession, "brooklyn", generatedPassVal));
    }
}
