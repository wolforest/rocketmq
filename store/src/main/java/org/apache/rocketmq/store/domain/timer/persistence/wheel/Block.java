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
package org.apache.rocketmq.store.domain.timer.persistence.wheel;


import java.nio.ByteBuffer;

/**
 * Represents a block of timer log. Format:
 * ┌────────────┬───────────┬────────┬───────────────────┬──────────────────┬───────────┬───────────┬──────────────────────────┬──────────────────────┐
 * │  unit size │  prev pos │  magic │  curr write time  │   delayed time   │ offsetPy  │   sizePy  │ hash code of real topic  │    reserved value    │
 * ├────────────┼───────────┼────────┼───────────────────┼──────────────────┼───────────┼───────────┼──────────────────────────┼──────────────────────┤
 * │   4bytes   │   8bytes  │ 4bytes │      8bytes       │      4bytes      │   8bytes  │   8bytes  │           4bytes         │         8bytes       │
 * └────────────┴───────────┴────────┴───────────────────┴──────────────────┴───────────┴───────────┴──────────────────────────┴──────────────────────┘
 */
public class Block {
    public static final short SIZE = 0
            + 4 //size
            + 8 //prev pos
            + 4 //magic value
            + 8 //curr write time, for trace
            + 4 //delayed time, for check
            + 8 //offsetPy
            + 4 //sizePy
            + 4 //hash code of real topic
            + 8;//reserved value, just in case of;
    private final ByteBuffer blockBuffer = ByteBuffer.allocate(SIZE);
    public int size;
    public long prevPos;
    public int magic;
    public long currWriteTime;
    public int delayedTime;
    public long offsetPy;
    public int sizePy;
    public int hashCodeOfRealTopic;
    public long reservedValue;

    public Block(int size,
                 long prevPos,
                 int magic,
                 long currWriteTime,
                 int delayedTime,
                 long offsetPy,
                 int sizePy,
                 int hashCodeOfRealTopic,
                 long reservedValue) {
        this.size = size;
        this.prevPos = prevPos;
        this.magic = magic;
        this.currWriteTime = currWriteTime;
        this.delayedTime = delayedTime;
        this.offsetPy = offsetPy;
        this.sizePy = sizePy;
        this.hashCodeOfRealTopic = hashCodeOfRealTopic;
        this.reservedValue = reservedValue;
    }

    public byte[] bytes() {
        ByteBuffer tmpBuffer = blockBuffer;
        tmpBuffer.clear();
        tmpBuffer.putInt(SIZE); //size
        tmpBuffer.putLong(prevPos); //prev pos ,lastPos
        tmpBuffer.putInt(magic); //magic
        tmpBuffer.putLong(currWriteTime); //currWriteTime,tmpWriteTimeMs
        tmpBuffer.putInt(delayedTime); //delayTime,(int) (delayedTime - tmpWriteTimeMs)
        tmpBuffer.putLong(offsetPy); //offset
        tmpBuffer.putInt(sizePy); //size
        tmpBuffer.putInt(hashCodeOfRealTopic); //hashcode of real topic,metricManager.hashTopicForMetrics(realTopic)
        tmpBuffer.putLong(0); //reserved value, just set to 0 now
        return tmpBuffer.array();
    }
}
