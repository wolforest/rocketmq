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

import java.io.IOException;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.store.infra.KV;

/**
 * This class is not thread safe
 */
public class FileKV implements KV {

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public String get(String fileName) {
        try {
            return IOUtils.file2String(fileName);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void set(String fileName, String data) {
        try {
            IOUtils.string2File(data, fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
