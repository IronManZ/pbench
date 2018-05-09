package com.tantan.ai.traffic_simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RequestLoadReader {
    private static final int ESTIMATED_NUM_CHAR_PER_LINE = 100;
    private BufferedReader _br;
    private long _startingTime;

    private static Logger LOG = LoggerFactory.getLogger(RequestLoadReader.class);

    RequestLoadReader(String requestLogPath) throws IOException {
        _br = new BufferedReader(new FileReader(requestLogPath));

        _br.mark(ESTIMATED_NUM_CHAR_PER_LINE);
        _startingTime = Long.parseLong(_br.readLine().split("\\s")[0].trim());
        _br.reset();
    }

    /**
     *
     * @param upToTimeStampInMs upper bound on the timestamp in milliseconds
     * @return a list of request parameters whose timestamp is less than or equal to {@code upToTimeStampInMs}
     */
    public List<MergerRequestParams> getRequestUptoTime(long upToTimeStampInMs) {
        List<MergerRequestParams> requestParams = new ArrayList<MergerRequestParams>();
        try {
            while(true) {
                _br.mark(ESTIMATED_NUM_CHAR_PER_LINE);
                String line = _br.readLine();
                long timestamp = Long.parseLong(line.split("\\s")[0].trim());
                long userId = Long.parseLong(line.split("\\s")[1].trim());
                if (timestamp <= upToTimeStampInMs) {
                    requestParams.add(new MergerRequestParams(userId, timestamp));
                } else {
                    _br.reset();
                    break;
                }
            }
        } catch (IOException e) {
            LOG.error("IO exception ", e.getMessage());
        }
        return requestParams;
    }

    /**
     *
     * @param n the number of lines to fetch from the request log
     * @return a list of MergerRequestParams
     */
    public List<MergerRequestParams> getNextNRequests(int n) {
        List<MergerRequestParams> requestParams = new ArrayList<MergerRequestParams>();
        try {
            for (int i=0; i<n; i++) {
                String line = _br.readLine();
                long timestamp = Long.parseLong(line.split("\\s")[0].trim());
                long userId = Long.parseLong(line.split("\\s")[1].trim());
                requestParams.add(new MergerRequestParams(userId, timestamp));
            }
        } catch (IOException e) {
            LOG.error("IO exception ", e.getMessage());
        }
        return requestParams;
    }

    public long getStartingTime() {
        return _startingTime;
    }
}
