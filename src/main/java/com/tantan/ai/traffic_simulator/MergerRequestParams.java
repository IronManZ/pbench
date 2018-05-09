package com.tantan.ai.traffic_simulator;

public class MergerRequestParams {
    private long _userId;
    private long _timestamp;


    MergerRequestParams(long userId, long timestamp) {
        _userId = userId;
        _timestamp = timestamp;
    }

    public long getUser_id() {
        return _userId;
    }

    public long get_timestamp() {
        return _timestamp;
    }

    @Override
    public String toString() {
        return "{" + _timestamp + ", " + _userId + "}";
    }
}
