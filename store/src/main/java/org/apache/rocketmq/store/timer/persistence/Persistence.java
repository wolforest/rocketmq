package org.apache.rocketmq.store.timer.persistence;

import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.service.Scanner;

public interface Persistence {
    boolean save(TimerRequest timerRequest);
    Scanner.ScannResult scan();
}
