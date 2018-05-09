package com.tantan.ai.traffic_simulator;

import org.apache.http.client.methods.HttpGet;

import java.util.List;
import java.util.TimerTask;
import java.util.stream.Collectors;

public class BatchSendTask extends TimerTask {

    private final RequestLoadReader _requestLoadReader;
    private int _offset;
    private PBench pBench = new PBench();

    public BatchSendTask(RequestLoadReader requestLoadReader, int offset) {
        _requestLoadReader = requestLoadReader;
        _offset = offset;
    }

    @Override
    public void run() {
        final long start = System.currentTimeMillis();
        System.out.println("New batch of requests starting at " + start);
        List<MergerRequestParams> mergerRequestParamsList = _requestLoadReader.getRequestUptoTime(
                _requestLoadReader.getStartingTime() + _offset);

        System.out.println("requests " + String.join(", \t",
                mergerRequestParamsList.stream().map(MergerRequestParams::toString).collect(Collectors.toList())));

        for (MergerRequestParams mergerRequestParams: mergerRequestParamsList) {
            long userId = mergerRequestParams.getUser_id();
            HttpGet request = new HttpGet(pBench.getFullUrl(userId));
            System.out.println("Sending request " + pBench.getFullUrl(userId));
        }
        _offset += 1000;
    }
}
