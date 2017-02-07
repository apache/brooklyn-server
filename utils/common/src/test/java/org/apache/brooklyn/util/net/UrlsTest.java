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

import org.apache.brooklyn.util.net.Urls;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UrlsTest {

    @Test
    public void testUrlToUriToStringAndBack() {
        String u = "http://localhost:8080/sample";
        Assert.assertEquals(Urls.toUrl(u).toString(), u);
        Assert.assertEquals(Urls.toUri(u).toString(), u);
        Assert.assertEquals(Urls.toUri(Urls.toUrl(u)).toString(), u);
        Assert.assertEquals(Urls.toUrl(Urls.toUri(u)).toString(), u);
    }
    
    @Test
    public void testMergePaths() throws Exception {
        assertEquals(Urls.mergePaths("a","b"), "a/b");
        assertEquals(Urls.mergePaths("/a//","/b/"), "/a/b/");
        assertEquals(Urls.mergePaths("foo://","/b/"), "foo:///b/");
        assertEquals(Urls.mergePaths("/","a","b","/"), "/a/b/");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMergePathsNPEsOnNulls() {
        Urls.mergePaths(null, "too");
    }

    @Test
    public void testPathEncode() throws Exception {
        encodeAndDecodeUrl("simple", "simple");
        encodeAndDecodeUrl("name_with/%!", "name_with%2F%25%21");

        StringBuilder allcharsBuilder = new StringBuilder();
        for (int i = 1; i < 128; i++) {
            if (i == 32 || i == 42) continue; // See testUserOrPasswordOrHostEncode()
            allcharsBuilder.append((char)i);
        }
        String allchars = allcharsBuilder.toString();
        encodeAndDecodeUrl(allchars, "%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%21%22%23%24%25%26%27%28%29%2B%2C-.%2F0123456789%3A%3B%3C%3D%3E%3F%40ABCDEFGHIJKLMNOPQRSTUVWXYZ%5B%5C%5D%5E_%60abcdefghijklmnopqrstuvwxyz%7B%7C%7D%7E%7F");
    }

    // TODO java.net.URLEncoder.encode() gets it wrong for:
    //  " " (i.e. space - char %20). It turns that into "+" in the url.
    //  "*" (i.e char %2A) is not escaped - this is wrong according to https://en.wikipedia.org/wiki/Percent-encoding (RFC 3986).
    @Test(groups="Broken")
    public void testUserOrPasswordOrHostEncode() throws Exception {
        StringBuilder passwordBuilder = new StringBuilder();
        for (int i = 1; i < 128; i++) {
            passwordBuilder.append((char)i);
        }
        String allchars = passwordBuilder.toString();
        encodeAndDecodeUrl(allchars, "%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%20%21%22%23%24%25%26%27%28%29%2A%2B%2C-.%2F0123456789%3A%3B%3C%3D%3E%3F%40ABCDEFGHIJKLMNOPQRSTUVWXYZ%5B%5C%5D%5E_%60abcdefghijklmnopqrstuvwxyz%7B%7C%7D%7E%7F");
    }

    @Test
    public void testIsUrlWithProtocol() {
        Assert.assertTrue(Urls.isUrlWithProtocol("a1+b.c-d:/"));
        Assert.assertTrue(Urls.isUrlWithProtocol("http://localhost/"));
        Assert.assertTrue(Urls.isUrlWithProtocol("protocol:xxx"));
        Assert.assertFalse(Urls.isUrlWithProtocol("protocol"));
        Assert.assertFalse(Urls.isUrlWithProtocol(":/"));
        Assert.assertFalse(Urls.isUrlWithProtocol("1:/"));
        Assert.assertTrue(Urls.isUrlWithProtocol("a1+b.c-:those/chars/are/allowed/in/protocol"));
        Assert.assertFalse(Urls.isUrlWithProtocol(null));
        Assert.assertTrue(Urls.isUrlWithProtocol("protocol:we allow spaces and %13 percent chars"));
        Assert.assertFalse(Urls.isUrlWithProtocol("protocol:we allow spaces\nbut not new lines"));
        Assert.assertFalse(Urls.isUrlWithProtocol("protocol: no space immediately after colon though"));
        Assert.assertFalse(Urls.isUrlWithProtocol("protocol:"));
        Assert.assertFalse(Urls.isUrlWithProtocol("protocol_underscore_disallowed:"));
    }

    @Test
    public void testGetBasename() {
        assertEquals(Urls.getBasename("http://somewhere.com/path/to/file.txt"), "file.txt");
        assertEquals(Urls.getBasename("http://somewhere.com/path/to/dir/"), "dir");
        assertEquals(Urls.getBasename("http://somewhere.com/path/to/file.txt?with/optional/suffice"), "file.txt");
        assertEquals(Urls.getBasename("filewith?.txt"), "filewith?.txt");
        assertEquals(Urls.getBasename(""), "");
        assertEquals(Urls.getBasename(null), null);
    }

    @Test
    public void testDataUrl() throws Exception {
        String input = "hello world";
        String url = Urls.asDataUrlBase64(input);
        Assert.assertEquals(url, "data:text/plain;base64,aGVsbG8gd29ybGQ=");
        // tests for parsing are in core in ResourceUtilsTest
    }


    @Test
    public void testEncodeAndDecodeUrl() throws Exception {
        encodeAndDecodeUrl("simple", "simple");
        encodeAndDecodeUrl("!@#$%^&*", "%21%40%23%24%25%5E%26*"); // TODO "*" should really be encoded!
        encodeAndDecodeUrl("a/b", "a%2Fb");
    }
    
    private void encodeAndDecodeUrl(String val, String expectedEncoding) {
        String actualEncoding = Urls.encode(val);
        String actualDecoded = Urls.decode(actualEncoding);
        assertEquals(actualDecoded, val, "Encode+decode does not match original: orig="+val+"; decoded="+actualDecoded);
        assertEquals(actualEncoding, expectedEncoding, "Does not match expected encoding: orig="+val+"; decoded="+actualDecoded);
    }
}
