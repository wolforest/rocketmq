package org.apache.rocketmq.store.timer;

public class Pointer {
    protected volatile long currReadTimeMs;
    protected volatile long currWriteTimeMs;
    protected volatile long preReadTimeMs;
    protected volatile long commitReadTimeMs;
    protected volatile long currQueueOffset; //only one queue that is 0
    protected volatile long commitQueueOffset;
    protected volatile long lastCommitReadTimeMs;
    protected volatile long lastCommitQueueOffset;

    public Pointer() {

    }
}