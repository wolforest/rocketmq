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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IOUtilsTest {

    /**
     * https://bazel.build/reference/test-encyclopedia#filesystem
     */
    private String testRootDir = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test" + File.separator + "iotinyutilstest";

    @Before
    public void init() {
        File dir = new File(testRootDir);
        if (dir.exists()) {
            IOUtils.deleteFile(dir);
        }

        dir.mkdirs();
    }

    @After
    public void destroy() {
        File file = new File(testRootDir);
        IOUtils.deleteFile(file);
    }

    @Test
    public void testString2File() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test" + File.separator + "StringUtilsTest" + System.currentTimeMillis();
        IOUtils.string2File("StringUtils_testString2File", fileName);
        assertThat(IOUtils.file2String(fileName)).isEqualTo("StringUtils_testString2File");
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
        String string = IOUtils.file2String(fileName);
        assertThat(string).isEqualTo("TestForStringUtils");
        file.delete();
    }

    @Test
    public void testToString() throws Exception {
        byte[] b = "testToString".getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(b);

        String str = IOUtils.toString(is, null);
        assertEquals("testToString", str);

        is = new ByteArrayInputStream(b);
        str = IOUtils.toString(is, StandardCharsets.UTF_8.name());
        assertEquals("testToString", str);

        is = new ByteArrayInputStream(b);
        Reader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        str = IOUtils.toString(isr);
        assertEquals("testToString", str);
    }


    @Test
    public void testCopy() throws Exception {
        char[] arr = "testToString".toCharArray();
        Reader reader = new CharArrayReader(arr);
        Writer writer = new CharArrayWriter();

        long count = IOUtils.copy(reader, writer);
        assertEquals(arr.length, count);
    }

    @Test
    public void testReadLines() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("testReadLines").append("\n");
        }

        StringReader reader = new StringReader(sb.toString());
        List<String> lines = IOUtils.readLines(reader);

        assertEquals(10, lines.size());
    }

    @Test
    public void testToBufferedReader() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("testToBufferedReader").append("\n");
        }

        StringReader reader = new StringReader(sb.toString());
        Method method = IOUtils.class.getDeclaredMethod("toBufferedReader", new Class[]{Reader.class});
        method.setAccessible(true);
        Object bReader = method.invoke(IOUtils.class, reader);

        assertTrue(bReader instanceof BufferedReader);
    }

    @Test
    public void testWriteStringToFile() throws Exception {
        File file = new File(testRootDir, "testWriteStringToFile");
        assertTrue(!file.exists());

        IOUtils.writeStringToFile(file, "testWriteStringToFile", StandardCharsets.UTF_8.name());

        assertTrue(file.exists());
    }

    @Test
    public void testCleanDirectory() throws Exception {
        for (int i = 0; i < 10; i++) {
            IOUtils.writeStringToFile(new File(testRootDir, "testCleanDirectory" + i), "testCleanDirectory", StandardCharsets.UTF_8.name());
        }

        File dir = new File(testRootDir);
        assertTrue(dir.exists() && dir.isDirectory());
        assertTrue(dir.listFiles().length > 0);

        IOUtils.cleanDirectory(new File(testRootDir));

        assertTrue(dir.listFiles().length == 0);
    }

    @Test
    public void testDelete() throws Exception {
        for (int i = 0; i < 10; i++) {
            IOUtils.writeStringToFile(new File(testRootDir, "testDelete" + i), "testCleanDirectory", StandardCharsets.UTF_8.name());
        }

        File dir = new File(testRootDir);
        assertTrue(dir.exists() && dir.isDirectory());
        assertTrue(dir.listFiles().length > 0);

        IOUtils.delete(new File(testRootDir));

        assertTrue(!dir.exists());
    }

    @Test
    public void testCopyFile() throws Exception {
        File source = new File(testRootDir, "source");
        String target = testRootDir + File.separator + "dest";

        IOUtils.writeStringToFile(source, "testCopyFile", StandardCharsets.UTF_8.name());

        IOUtils.copyFile(source.getCanonicalPath(), target);

        File dest = new File(target);
        assertTrue(dest.exists());
    }

    @Test
    public void testCalculateFileSizeInPath() throws Exception {
        /**
         * testCalculateFileSizeInPath
         *  - file_0
         *  - dir_1
         *      - file_1_0
         *      - file_1_1
         *      - dir_1_2
         *          - file_1_2_0
         *  - dir_2
         */
        String basePath = System.getProperty("java.io.tmpdir") + File.separator + "testCalculateFileSizeInPath";
        File baseFile = new File(basePath);
        try {
            // test empty path
            assertEquals(0, IOUtils.calculateFileSizeInPath(baseFile));

            // create baseDir
            assertTrue(baseFile.mkdirs());

            File file0 = new File(baseFile, "file_0");
            assertTrue(file0.createNewFile());
            writeFixedBytesToFile(file0, 1313);

            assertEquals(1313, IOUtils.calculateFileSizeInPath(baseFile));

            // build a file tree like above
            File dir1 = new File(baseFile, "dir_1");
            dir1.mkdirs();
            File file10 = new File(dir1, "file_1_0");
            File file11 = new File(dir1, "file_1_1");
            File dir12 = new File(dir1, "dir_1_2");
            dir12.mkdirs();
            File file120 = new File(dir12, "file_1_2_0");
            File dir2 = new File(baseFile, "dir_2");
            dir2.mkdirs();

            // write all file with 1313 bytes data
            assertTrue(file10.createNewFile());
            writeFixedBytesToFile(file10, 1313);
            assertTrue(file11.createNewFile());
            writeFixedBytesToFile(file11, 1313);
            assertTrue(file120.createNewFile());
            writeFixedBytesToFile(file120, 1313);

            assertEquals(1313 * 4, IOUtils.calculateFileSizeInPath(baseFile));
        } finally {
            IOUtils.delete(baseFile);
        }
    }

    private void writeFixedBytesToFile(File file, int size) throws Exception {
        FileOutputStream outputStream = new FileOutputStream(file);
        byte[] bytes = new byte[size];
        outputStream.write(bytes, 0, size);
        outputStream.close();
    }

    @Test
    public void testCleanBuffer() {
        IOUtils.cleanBuffer(null);
        IOUtils.cleanBuffer(ByteBuffer.allocateDirect(10));
        IOUtils.cleanBuffer(ByteBuffer.allocateDirect(0));
        IOUtils.cleanBuffer(ByteBuffer.allocate(10));
    }

}
