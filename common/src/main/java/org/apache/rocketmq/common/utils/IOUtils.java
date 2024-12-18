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

import io.netty.util.internal.PlatformDependent;
import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * @renamed from IOTinyUtils to IOUtils
 */
public class IOUtils {

    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    private static final Logger STORE_LOG = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);


    static public String toString(InputStream input, String encoding) throws IOException {
        return (null == encoding) ? toString(new InputStreamReader(input, StandardCharsets.UTF_8)) : toString(new InputStreamReader(
            input, encoding));
    }

    static public String toString(Reader reader) throws IOException {
        CharArrayWriter sw = new CharArrayWriter();
        copy(reader, sw);
        return sw.toString();
    }

    static public long copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[1 << 12];
        long count = 0;
        for (int n = 0; (n = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    static public List<String> readLines(Reader input) throws IOException {
        BufferedReader reader = toBufferedReader(input);
        List<String> list = new ArrayList<>();
        String line;
        for (; ; ) {
            line = reader.readLine();
            if (null != line) {
                list.add(line);
            } else {
                break;
            }
        }
        return list;
    }

    static private BufferedReader toBufferedReader(Reader reader) {
        return reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
    }

    static public void copyFile(String source, String target) throws IOException {
        File sf = new File(source);
        if (!sf.exists()) {
            throw new IllegalArgumentException("source file does not exist.");
        }
        File tf = new File(target);
        tf.getParentFile().mkdirs();
        if (!tf.exists() && !tf.createNewFile()) {
            throw new RuntimeException("failed to create target file.");
        }

        FileChannel sc = null;
        FileChannel tc = null;
        try {
            tc = new FileOutputStream(tf).getChannel();
            sc = new FileInputStream(sf).getChannel();
            sc.transferTo(0, sc.size(), tc);
        } finally {
            if (null != sc) {
                sc.close();
            }
            if (null != tc) {
                tc.close();
            }
        }
    }

    public static void delete(File fileOrDir) throws IOException {
        if (fileOrDir == null) {
            return;
        }

        if (fileOrDir.isDirectory()) {
            cleanDirectory(fileOrDir);
        }

        fileOrDir.delete();
    }

    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) { // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                delete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

    public static void writeStringToFile(File file, String data, String encoding) throws IOException {
        OutputStream os = null;
        try {
            os = new FileOutputStream(file);
            os.write(data.getBytes(encoding));
        } finally {
            if (null != os) {
                os.close();
            }
        }
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static boolean isPathExists(final String path) {
        File file = new File(path);
        return file.exists();
    }

    public static double getDiskPartitionSpaceUsedPercent(final String path) {
        if (null == path || path.isEmpty()) {
            STORE_LOG.error("Error when measuring disk space usage, path is null or empty, path : {}", path);
            return -1;
        }

        try {
            File file = new File(path);

            if (!file.exists()) {
                STORE_LOG.error("Error when measuring disk space usage, file doesn't exist on this path: {}", path);
                return -1;
            }

            long totalSpace = file.getTotalSpace();

            if (totalSpace > 0) {
                long usedSpace = totalSpace - file.getFreeSpace();
                long usableSpace = file.getUsableSpace();
                long entireSpace = usedSpace + usableSpace;
                long roundNum = 0;
                if (usedSpace * 100 % entireSpace != 0) {
                    roundNum = 1;
                }
                long result = usedSpace * 100 / entireSpace + roundNum;
                return result / 100.0;
            }
        } catch (Exception e) {
            STORE_LOG.error("Error when measuring disk space usage, got exception: :", e);
            return -1;
        }

        return -1;
    }

    public static long getDiskPartitionTotalSpace(final String path) {
        if (null == path || path.isEmpty()) {
            return -1;
        }

        try {
            File file = new File(path);

            if (!file.exists()) {
                return -1;
            }

            return file.getTotalSpace() - file.getFreeSpace() + file.getUsableSpace();
        } catch (Exception e) {
            return -1;
        }
    }

    public static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                deleteFile(file1);
            }
            file.delete();
        }
    }

    public static void deleteEmptyDirectory(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (!file.isDirectory()) {
            return;
        }
        File[] files = file.listFiles();
        if (files == null || files.length <= 0) {
            file.delete();
            STORE_LOG.info("delete empty direct, {}", file.getPath());
        }
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
            STORE_LOG.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
        }
    }

    public static long calculateFileSizeInPath(File path) {
        long size = 0;
        try {
            if (!path.exists() || Files.isSymbolicLink(path.toPath())) {
                return 0;
            }
            if (path.isFile()) {
                return path.length();
            }
            if (path.isDirectory()) {
                File[] files = path.listFiles();
                if (files != null && files.length > 0) {
                    for (File file : files) {
                        long fileSize = calculateFileSizeInPath(file);
                        if (fileSize == -1) return -1;
                        size += fileSize;
                    }
                }
            }
        } catch (Exception e) {
            log.error("calculate all file size in: {} error", path.getAbsolutePath(), e);
            return -1;
        }
        return size;
    }

    public static String exceptionSimpleDesc(final Throwable e) {
        StringBuilder sb = new StringBuilder();
        if (e != null) {
            sb.append(e);

            StackTraceElement[] stackTrace = e.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                StackTraceElement element = stackTrace[0];
                sb.append(", ");
                sb.append(element.toString());
            }
        }
        return sb.toString();
    }

    public static String jstack() {
        return jstack(Thread.getAllStackTraces());
    }

    public static String jstack(Map<Thread, StackTraceElement[]> map) {
        StringBuilder result = new StringBuilder();
        try {
            for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements == null || elements.length <= 0) {
                    continue;
                }

                String threadName = entry.getKey().getName();
                result.append(String.format("%-40sTID: %d STATE: %s%n", threadName, thread.getId(), thread.getState()));
                for (StackTraceElement el : elements) {
                    result.append(String.format("%-40s%s%n", threadName, el.toString()));
                }
                result.append("\n");
            }
        } catch (Throwable e) {
            result.append(exceptionSimpleDesc(e));
        }

        return result.toString();
    }

    /**
     * Free direct-buffer's memory actively.
     * @param buffer Direct buffer to free.
     */
    public static void cleanBuffer1(final ByteBuffer buffer) {
        if (null == buffer) {
            return;
        }

        if (!buffer.isDirect()) {
            return;
        }

        PlatformDependent.freeDirectBuffer(buffer);
    }

    public static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("buffer is non-direct");
        }
        ByteBuffer viewedBuffer = (ByteBuffer) ((DirectBuffer) buffer).attachment();
        if (viewedBuffer == null) {
            return buffer;
        } else {
            return viewed(viewedBuffer);
        }
    }

    public static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    public static void cleanBuffer(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }
        if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
            try {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                Unsafe unsafe = (Unsafe) field.get(null);
                Method cleaner = method(unsafe, "invokeCleaner", new Class[] {ByteBuffer.class});
                cleaner.invoke(unsafe, viewed(buffer));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        } else {
            invoke(invoke(viewed(buffer), "cleaner"), "clean");
        }
    }

    public static String dealFilePath(String aclFilePath) {
        Path path = Paths.get(aclFilePath);
        return path.normalize().toString();
    }

    public static synchronized void string2File(final String str, final String fileName) throws IOException {
        String bakFile = fileName + ".bak";
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            string2FileNotSafe(prevContent, bakFile);
        }

        string2FileNotSafe(str, fileName);
    }

    public static void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        writeStringToFile(file, str, "UTF-8");
    }

    public static String file2String(final String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    public static String file2String(final File file) throws IOException {
        if (!file.exists()) {
            return null;
        }

        byte[] data = new byte[(int) file.length()];
        boolean result;

        try (FileInputStream inputStream = new FileInputStream(file)) {
            int len = inputStream.read(data);
            result = len == data.length;
        }

        if (result) {
            return new String(data, "UTF-8");
        }

        return null;
    }

    public static String file2String(final URL url) {
        InputStream in = null;
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setUseCaches(false);
            in = urlConnection.getInputStream();
            int len = in.available();
            byte[] data = new byte[len];
            in.read(data, 0, len);
            return new String(data, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException ignored) {
                }
            }
        }

        return null;
    }
}
