package com.tantan.ai.traffic_simulator;

import com.google.common.base.Preconditions;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/*
cmd: pbench -t 100 -d 10 -q 300 -v 3 -l 10 http://localhost/test?@{p1}=v1&p2=v2
-t: test time in seconds
-d: send request in ms of one second, value range [1, 1000]
-g: the number of seconds in a group, all the request in a group will be distributed evenly into small windows and sent
out at the beginning of each window.
-b: number of batches/windows per group,
-q: QPS
-v: view response content
-l: print info every N loops
 */


public class PBench {
    private Integer qps = null;  // store -q param value
    private int windowSize = 1000;   // store -d param value
    private int print_loops = 10;   // store -p param value
    private int numSeconds = 100;        // -n number of groups/seconds in the test
    private Integer groupSize = null;      // -g  number of seconds in a group
    private int numBatchPerGroup = 1; // -b  break down the requests in the same group into batches
    private String _requestLogFile = "requestLog.csv";
    private LogLevel logLevel = LogLevel.INFO;   // store -l param value
    private static final String urlPattern = "http://10.189.100.42:8004/" +
            "users?search=n&with=ns&byPassThroughMode=false&user_id=&limit=2000";
    private String url = urlPattern;
    private int requestId;  // sequence requestId
    // requestId -> response time
    private ConcurrentHashMap<Integer, Long> responseTimeMap = new ConcurrentHashMap<Integer, Long>(1000);
    private AtomicLong responseDelayTime = new AtomicLong();

    private CloseableHttpAsyncClient httpclient;

    private AtomicInteger viewResult = new AtomicInteger(2);

    private AtomicInteger completed_ok = new AtomicInteger();
    private AtomicInteger completed_fail = new AtomicInteger();
    private AtomicInteger future_failed = new AtomicInteger();
    private AtomicInteger future_cancelled = new AtomicInteger();

    private AtomicLong receive_bytes = new AtomicLong();

    private final long testStartTime = System.currentTimeMillis();

    private static final Logger LOG = LoggerFactory.getLogger(PBench.class);

    public static void main(String[] argv) {
        PBench pBench = new PBench();
        pBench.config(argv);
        pBench.start();
    }

    public void config(String[] argv) {
        for (int i = 0; i < argv.length; i++) {
            String cmd = argv[i];
            String value;

            if (cmd.equals("-q")) {
                value = argv[++i];
                qps = Integer.parseInt(value);
            } else if (cmd.equals("-d")) {
                value = argv[++i];
                windowSize = Integer.parseInt(value);
                if (windowSize < 1 || windowSize > 1000) {
                    println("-d range [1,1000]");
                    System.exit(-1);
                }
            } else if (cmd.equals("-v")) {
                value = argv[++i];
                viewResult.set(Integer.parseInt(value));
            } else if (cmd.equals("-t")) {
                value = argv[++i];
                numSeconds = Integer.parseInt(value);
            } else if (cmd.equals("-g")) {
                value = argv[++i];
                groupSize = Integer.parseInt(value);
            } else if (cmd.equals("-b")) {
                value = argv[++i];
                numBatchPerGroup = Integer.parseInt(value);
            } else if (cmd.equals("-p")) {
                value = argv[++i];
                print_loops = Integer.parseInt(value);
            } else if (cmd.equals("-l")) {
                value = argv[++i];
                logLevel = LogLevel.valueOf(value.toUpperCase());
            } else if (cmd.equals("-i")) {
                value = argv[++i];
                _requestLogFile = value;
                println("Request Log file is " + _requestLogFile);
            } else if (cmd.startsWith("http")) {
                url = cmd;
            } else if (cmd.startsWith("\"http") && cmd.endsWith("\"")) {
                cmd = cmd.substring(1, cmd.length() - 1);
                url = cmd;
            } else {
                println("Unknown Arg: " + cmd);
                System.exit(-1);
            }
        }
        Preconditions.checkArgument(url != null, "url can't be null");
        Preconditions.checkArgument(qps != null || groupSize != null, "You should specify either " +
                "the QPS using -q or the group size using -g" );
        Preconditions.checkArgument(qps == null || groupSize == null, "You cannot use -q and " +
                "-b at the same time");
    }


    public static void println(String str) {
        System.out.println(str);
    }

    public void start() {
        println("Test start time: " + getTime());
        httpclient = getClient();
        httpclient.start();

        RequestLoadReader requestLoadReader = null;
        try {
            requestLoadReader = new RequestLoadReader(_requestLogFile);
        } catch (IOException e) {
            LOG.error("Cannot open file {0}", _requestLogFile, e);
            System.exit(-1);
        }

        println("Log start time is " + requestLoadReader.getStartingTime());

        long loop_start = System.currentTimeMillis();
        long receive_bytes_0 = receive_bytes.get();
        int send_requests_0 = responseTimeMap.size();

        for (int g = 0; g < numSeconds; g++) {
            // get the requests for the next group
            List<MergerRequestParams> mergerRequestParamsList = null;

            if (groupSize != null) {
                mergerRequestParamsList = requestLoadReader.getRequestUptoTime(
                        requestLoadReader.getStartingTime() + groupSize * 1000 * g);
            } else if (qps != null) {
                mergerRequestParamsList = requestLoadReader.getNextNRequests(qps);
            }

            if (LogLevel.DEBUG == logLevel) {
                println("requests for this group " + String.join(", \t",
                        mergerRequestParamsList.stream().map(MergerRequestParams::toString).collect(Collectors.toList())));
            }

            int requestPerWindow = mergerRequestParamsList.size() / numBatchPerGroup;

            // break the requests down to small windows
            for (int i=0; i < numBatchPerGroup; i++) {
                final long start = System.currentTimeMillis();
                println("New batch of requests starting at " + start);
                for(int j=0; j<requestPerWindow; j++) {
                    long userId = mergerRequestParamsList.get(i * requestPerWindow + j).getUser_id();
                    HttpGet request = new HttpGet(getFullUrl(userId));

                    if (LogLevel.DEBUG == logLevel) {
                        println("Sending request " + getFullUrl(userId));
                    }

                    FutureCallback<HttpResponse> callback = new ResponseCallBack(requestId++, start);
                    httpclient.execute(request, callback);
                }
                sleep( 1000 / numBatchPerGroup - (int) (System.currentTimeMillis() - start));  // 1000 / qps_times is the number of ms per window.

                if (i % print_loops == (print_loops - 1) && responseTimeMap.size() > 0) {
                    double time = (System.currentTimeMillis() - start) / 1000.0;
                    println(String.format("TPS: %d, Delay: %dms, Rate: %.3f MB/s", (int)((responseTimeMap.size() - send_requests_0) / time), responseDelayTime.get() / responseTimeMap.size(),
                            (receive_bytes.get() - receive_bytes_0) / 1024.0 / 1024.0 / time));
                    receive_bytes_0 = receive_bytes.get();
                    send_requests_0 = responseTimeMap.size();
                }
            }
        }

        while (responseTimeMap.size() < requestId) {
            println(getTime() + ": responses=" + responseTimeMap.size());
            sleep(1000);
        }

        calcResult();

        try {
            httpclient.close();
        } catch (IOException ignore) {

        }

    }

    public String getFullUrl(long user_id) {
        return url.replace("user_id=", "user_id=" + user_id);
    }


    private String getTime() {
        return DateUtils.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
    }

    private void calcResult() {
        if (responseTimeMap.size() > 0) {
            long testEndTime = System.currentTimeMillis();
            println(String.format("Test end time: %s  Test Duration: %.1fs", getTime(), (System.currentTimeMillis() - testStartTime) / 1000.0));
            println(String.format("Total Request %d, total responses %d", requestId, responseTimeMap.size()));
            println(String.format("QPS: %d  TPS: %d", qps, (int)(requestId / ((testEndTime - testStartTime) / 1000.0))));
            println(String.format("Receive %.1f MB, Rate %.3f MB/s", receive_bytes.get() / 1024.0 / 1024.0, receive_bytes.get() / 1024.0 / 1024.0 / ((System.currentTimeMillis() - testStartTime) / 1000.0)));
            println(String.format("Completed OK=%d fail=%d, Exception=%d, Cancelled=%d", completed_ok.get(), completed_fail.get(), future_failed.get(), future_cancelled.get()));

            Long total = 0L;
            for (Long l : responseTimeMap.values()) {
                total += l;
            }
            println(String.format("Average Delay: %dms", (total / responseTimeMap.size())));
            TreeSet<Long> sortSet = new TreeSet<Long>(responseTimeMap.values());
            long[] percentiles = new long[5];
            final int size = sortSet.size();
            Long[] sorted = sortSet.toArray(new Long[size]);
            percentiles[0] = sorted[(int)(size * 0.50)];
            percentiles[1] = sorted[(int)(size * 0.75)];
            percentiles[2] = sorted[(int)(size * 0.90)];
            percentiles[3] = sorted[(int)(size * 0.95)];
            percentiles[4] = sorted[(int)(size * 0.99)];
            println(String.format("Response Percentiles(0.50,0.75,0.90,0.95,0.99): %d %d %d %d %d", percentiles[0],
                    percentiles[1],percentiles[2],percentiles[3],percentiles[4]));
        } else {
            println("I did get any result!!! Please check your network connection!");
        }
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception i) {}
    }


    private static CloseableHttpAsyncClient getClient() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(30000)
                .setSocketTimeout(30000)
                .setConnectionRequestTimeout(10000)
                .build();

        //配置io线程
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().
                setIoThreadCount(Runtime.getRuntime().availableProcessors())
                .setSoKeepAlive(true)
                .build();
        //设置连接池大小
        ConnectingIOReactor ioReactor=null;
        try {
            ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
        } catch (IOReactorException e) {
            e.printStackTrace();
        }
        PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
        connManager.setMaxTotal(3000);
        connManager.setDefaultMaxPerRoute(1000);


        CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom().
                setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig)
                .build();

        return httpclient;
    }

    enum LogLevel {
        INFO("INFO"),
        DEBUG("DEBUG");

        private String level;

        LogLevel(String level) {
            this.level = level;
        }
    }

    class ResponseCallBack implements FutureCallback<HttpResponse> {
        private final int requestId;
        private final long startTime;

        public ResponseCallBack(int requestId, long startTime) {
            this.requestId = requestId;
            this.startTime = startTime;
        }

        @Override
        public void completed(final HttpResponse response) {
            try {
                recordTime();
                if (response.getStatusLine().getStatusCode() == 200) {
                    completed_ok.incrementAndGet();
                } else {
                    completed_fail.incrementAndGet();
                }
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                receive_bytes.addAndGet(content != null ? content.length() : 0);
                if (viewResult.get() >= 0 && viewResult.decrementAndGet() >= 0) {
                    println(content);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void failed(final Exception e) {
            recordTime();
            future_failed.incrementAndGet();
            e.printStackTrace();
        }

        @Override
        public void cancelled() {
            recordTime();
            future_cancelled.incrementAndGet();
        }

        private void recordTime() {
            long delay = System.currentTimeMillis() - startTime;
            responseDelayTime.addAndGet(delay);
            responseTimeMap.put(this.requestId, delay);
        }
    }


}