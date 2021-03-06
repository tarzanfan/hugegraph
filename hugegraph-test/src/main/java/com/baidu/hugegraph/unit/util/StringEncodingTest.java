/*
 * Copyright 2019 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.unit.util;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.StringEncoding;

public class StringEncodingTest {

    @Test
    public void testEncode() {
        Assert.assertArrayEquals(new byte[]{},
                                 StringEncoding.encode(""));
        Assert.assertArrayEquals(new byte[]{97, 98, 99},
                                 StringEncoding.encode("abc"));
    }

    @Test
    public void testDecode() {
        Assert.assertEquals("abc",
                            StringEncoding.decode(new byte[]{97, 98, 99}));
        Assert.assertEquals("", StringEncoding.decode(new byte[]{}));
    }

    @Test
    public void testFormat() {
        Assert.assertEquals("abc[0x616263]",
                            StringEncoding.format(new byte[]{97, 98, 99}));
        Assert.assertEquals("� [0x01ff00]",
                            StringEncoding.format(new byte[]{1, -1, 0}));
        Assert.assertEquals("", StringEncoding.decode(new byte[]{}));
    }

    @Test
    public void testCompress() {
        Assert.assertArrayEquals(Bytes.fromHex("1f8b080000000000000003" +
                                               "000000000000000000"),
                                 StringEncoding.compress(""));
        Assert.assertArrayEquals(Bytes.fromHex("1f8b08000000000000004b" +
                                               "4c4a0600c241243503000000"),
                                 StringEncoding.compress("abc"));
    }

    @Test
    public void testDeompress() {
        Assert.assertEquals("", StringEncoding.decompress(Bytes.fromHex(
                                "1f8b080000000000000003000000000000000000")));
        Assert.assertEquals("abc", StringEncoding.decompress(Bytes.fromHex(
                            "1f8b08000000000000004b4c4a0600c241243503000000")));
    }

    @Test
    public void testWriteAsciiString() {
        String str = "abc133";
        int length = StringEncoding.getAsciiByteLength(str);
        Assert.assertEquals(6, length);
        byte[] buf = new byte[length];
        Assert.assertEquals(length,
                            StringEncoding.writeAsciiString(buf, 0, str));
        Assert.assertArrayEquals(Bytes.fromHex("6162633133b3"), buf);

        str = "";
        length = StringEncoding.getAsciiByteLength(str);
        Assert.assertEquals(1, length);
        buf = new byte[length];
        Assert.assertEquals(length,
                            StringEncoding.writeAsciiString(buf, 0, str));
        Assert.assertArrayEquals(Bytes.fromHex("80"), buf);
    }

    @Test
    public void testReadAsciiString() {
        byte[] buf = Bytes.fromHex("6162633133b3");
        Assert.assertEquals("abc133", StringEncoding.readAsciiString(buf, 0));

        buf = Bytes.fromHex("80");
        Assert.assertEquals("", StringEncoding.readAsciiString(buf, 0));
    }
}
