package org.apache.rocketmq.store.infra;

public interface KV {
    void start();
    void shutdown();

    String get(String key);
    void set(String key, String value);
}
