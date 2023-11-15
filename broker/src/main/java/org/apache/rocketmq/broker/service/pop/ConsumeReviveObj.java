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
package org.apache.rocketmq.broker.service.pop;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class ConsumeReviveObj {
    private final HashMap<String, PopCheckPoint> map = new HashMap<>();
    private ArrayList<PopCheckPoint> sortList;

    private long oldOffset;
    private long endTime;
    private long newOffset;

    public ArrayList<PopCheckPoint> genSortList() {
        if (sortList != null) {
            return sortList;
        }
        sortList = new ArrayList<>(map.values());
        sortList.sort((o1, o2) -> (int) (o1.getReviveOffset() - o2.getReviveOffset()));
        return sortList;
    }

    public HashMap<String, PopCheckPoint> getMap() {
        return map;
    }

    public ArrayList<PopCheckPoint> getSortList() {
        return sortList;
    }

    public long getOldOffset() {
        return oldOffset;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getNewOffset() {
        return newOffset;
    }

    public void setOldOffset(long oldOffset) {
        this.oldOffset = oldOffset;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setNewOffset(long newOffset) {
        this.newOffset = newOffset;
    }
}
