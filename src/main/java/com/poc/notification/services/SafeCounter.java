package com.poc.notification.services;

public class SafeCounter {
    private long count = 0;

    public synchronized void increment(long value){
        this.count += value;
    }

    public long getCount() {
        return count;
    }
}
