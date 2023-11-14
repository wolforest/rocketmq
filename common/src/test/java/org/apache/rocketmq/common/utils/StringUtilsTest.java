/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.Assert.assertEquals;

public class StringUtilsTest {
    @Test
    public void testString2File() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test" + File.separator + "StringUtilsTest" + System.currentTimeMillis();
        StringUtils.string2File("StringUtils_testString2File", fileName);
        assertThat(StringUtils.file2String(fileName)).isEqualTo("StringUtils_testString2File");
    }

    @Test
    public void testFile2String() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test" + File.separator + "StringUtilsTest" + System.currentTimeMillis();
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        PrintWriter out = new PrintWriter(fileName);
        out.write("TestForStringUtils");
        out.close();
        String string = StringUtils.file2String(fileName);
        assertThat(string).isEqualTo("TestForStringUtils");
        file.delete();
    }

    @Test
    public void testGetDiskPartitionSpaceUsedPercent() {
        String tmpDir = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test";

        assertThat(IOTinyUtils.getDiskPartitionSpaceUsedPercent(null)).isCloseTo(-1, within(0.000001));
        assertThat(IOTinyUtils.getDiskPartitionSpaceUsedPercent("")).isCloseTo(-1, within(0.000001));
        assertThat(IOTinyUtils.getDiskPartitionSpaceUsedPercent("nonExistingPath")).isCloseTo(-1, within(0.000001));
        assertThat(IOTinyUtils.getDiskPartitionSpaceUsedPercent(tmpDir)).isNotCloseTo(-1, within(0.000001));
    }

    @Test
    public void testIsBlank() {
        assertThat(StringUtils.isBlank("Hello ")).isFalse();
        assertThat(StringUtils.isBlank(" Hello")).isFalse();
        assertThat(StringUtils.isBlank("He llo")).isFalse();
        assertThat(StringUtils.isBlank("  ")).isTrue();
        assertThat(StringUtils.isBlank("Hello")).isFalse();
    }

    @Test
    public void testJoin() {
        List<String> list = Arrays.asList("groupA=DENY", "groupB=PUB|SUB", "groupC=SUB");
        String comma = ",";
        assertEquals("groupA=DENY,groupB=PUB|SUB,groupC=SUB", StringUtils.join(list, comma));
        assertEquals(null, StringUtils.join(null, comma));
        List<String> objects = Collections.emptyList();
        assertEquals("", StringUtils.join(objects, comma));
    }

    @Test
    public void testCleanBuffer() {
        IOTinyUtils.cleanBuffer(null);
        IOTinyUtils.cleanBuffer(ByteBuffer.allocate(10));
        IOTinyUtils.cleanBuffer(ByteBuffer.allocate(0));
    }

}
