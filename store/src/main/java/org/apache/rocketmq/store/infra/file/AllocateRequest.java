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
package org.apache.rocketmq.store.infra.file;

import java.io.File;
import java.util.concurrent.CountDownLatch;

public class AllocateRequest implements Comparable<AllocateRequest> {
    // Full file path
    private String filePath;
    private int fileSize;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    protected volatile MappedFile mappedFile = null;

    public AllocateRequest(String filePath, int fileSize) {
        this.filePath = filePath;
        this.fileSize = fileSize;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    public void setMappedFile(MappedFile mappedFile) {
        this.mappedFile = mappedFile;
    }

    public int compareTo(AllocateRequest other) {
        if (this.fileSize < other.fileSize)
            return 1;
        else if (this.fileSize > other.fileSize) {
            return -1;
        } else {
            int mIndex = this.filePath.lastIndexOf(File.separator);
            long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
            int oIndex = other.filePath.lastIndexOf(File.separator);
            long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
            if (mName < oName) {
                return -1;
            } else if (mName > oName) {
                return 1;
            } else {
                return 0;
            }
        }
        // return this.fileSize < other.fileSize ? 1 : this.fileSize >
        // other.fileSize ? -1 : 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
        result = prime * result + fileSize;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AllocateRequest other = (AllocateRequest) obj;
        if (filePath == null) {
            if (other.filePath != null)
                return false;
        } else if (!filePath.equals(other.filePath))
            return false;
        if (fileSize != other.fileSize)
            return false;
        return true;
    }
}
