package org.apache.rocketmq.store.timer;

public class Pointer {
    public volatile long currReadTimeMs;
    public volatile long currWriteTimeMs;
    public volatile long preReadTimeMs;
    public volatile long commitReadTimeMs;
    public volatile long currQueueOffset; //only one queue that is 0
    public volatile long commitQueueOffset;
    public volatile long lastCommitReadTimeMs;
    public volatile long lastCommitQueueOffset;
    public long lastEnqueueButExpiredTime;
    public long lastEnqueueButExpiredStoreTime;
    // True if current store is master or current brokerId is equal to the minimum brokerId of the replica group in slaveActingMaster mode.
    public volatile boolean shouldRunningDequeue;

    public Pointer() {

    }
}