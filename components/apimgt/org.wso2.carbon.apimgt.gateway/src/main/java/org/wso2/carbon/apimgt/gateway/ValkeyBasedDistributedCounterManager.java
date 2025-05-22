package org.wso2.carbon.apimgt.gateway;

import io.valkey.HostAndPort;
import io.valkey.JedisCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.throttle.core.DistributedCounterManager;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ValkeyBasedDistributedCounterManager implements DistributedCounterManager {

    private static final Log log = LogFactory.getLog(ValkeyBasedDistributedCounterManager.class);

    private static final int DEFAULT_TIMEOUT = 2000;
    private static final int DEFAULT_REDIRECTIONS = 5;

    private final JedisCluster valkeyClient;

    private List<CounterLatencyMetrics> metrics;
    private ScheduledExecutorService metricsWriterService;
    private String csvFileName;

    public ValkeyBasedDistributedCounterManager(ValkeyConfig valkeyConfig) {
        HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        for (int i = 0; i < valkeyConfig.getHostAddresses().size(); i++) {
            hostAndPorts.add(new HostAndPort(valkeyConfig.getHostAddresses().get(i), valkeyConfig.getPorts().get(i)));
        }

        valkeyClient = new JedisCluster(hostAndPorts);
        log.info("Valkey based distributed counter manager started"); // TODO: remove this line

        initRecordMetrics(); // TODO: remove this line
    }

    private void initRecordMetrics() {
        this.metrics = new ArrayList<>(8092);
        metricsWriterService = Executors.newScheduledThreadPool(1);
        csvFileName = "valkey_" + System.currentTimeMillis() + ".csv";

        metricsWriterService.scheduleAtFixedRate(() -> {
            if (metrics.isEmpty()) return;

            try {
                // create a csv file write object first
                FileWriter csvWriter = new FileWriter(csvFileName);
                csvWriter.append("timestamp,operation,latency(ms)\n");
                csvWriter.flush();

                for (CounterLatencyMetrics metric: metrics) {
                    csvWriter.append(String.format("%d,%s,%d\n", metric.timestamp, metric.operation, metric.latency()));
                }

                csvWriter.flush();
                csvWriter.close();

                // clear the metric data
                metrics.clear();
            } catch (IOException exception) {
                log.warn("Riak metric writing failed: " + exception.getMessage());
            }

        }, 5, 10, TimeUnit.SECONDS);
    }

    @Override
    public long getCounter(String key) {
        long startTime = 0;
        try {
            String count = null;
            startTime = System.currentTimeMillis();

            count = valkeyClient.get(key);
            if (count == null || count.equals("nil")) {
                log.trace("key does not exists: " + key);
            } else {
                long l = Long.parseLong(count);
                log.trace(String.format("Key %s exists in the valkey with value: %s", key, count));
                return l;
            }
            log.trace("Shared counter key didn't exists, but returning 0");
            return 0;

        } finally {
            log.info("Time taken to getDistributedCounter :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to getDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public void setCounter(String key, long value) {
        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();
            long del = valkeyClient.del(key);
            valkeyClient.set(key, String.valueOf(value));
        } finally {
            log.info("Time taken to setDistributedCounter :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to setDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public void setCounterWithExpiry(String key, long value, long expiryTime) {
        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();
            long del = valkeyClient.del(key);
            valkeyClient.set(key, String.valueOf(value));
            valkeyClient.pexpireAt(key, expiryTime);
        } finally {
            log.info("Time taken to setDistributedCounterWithExpiry :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to setDistributedCounterWithExpiry :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long addAndGetCounter(String key, long value) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();
            long incrementedValue = 0;

            String currentValue = valkeyClient.get(key);
            if (currentValue == null || currentValue.equals("nil")) {
                valkeyClient.psetex(key, 1000, String.valueOf(value));
                incrementedValue = value;
            } else {
                incrementedValue = valkeyClient.incrBy(key, value);
            }
            if (log.isTraceEnabled()) {
                log.trace(String.format("Key %s is incremented from %s to %s", key, incrementedValue - value, incrementedValue));
            }

            return incrementedValue;
        } finally {
//            log.info("Time taken to addAndGetDistributedCounter :" + (System.currentTimeMillis() - startTime));
            metrics.add(new CounterLatencyMetrics(System.currentTimeMillis(), CounterLatencyMetrics.Operation.INCREMENT_AND_GET, (System.currentTimeMillis() - startTime)));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to addAndGetDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public void removeCounter(String key) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            valkeyClient.del(key);
        } finally {
            log.info("Time taken to removeDistributedCounter :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to removeDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long asyncGetAndAddCounter(String key, long value) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            long current = 0;
            String currentValue = valkeyClient.get(key);
            long incrementedValue = valkeyClient.incrBy(key, value);

            if (currentValue != null && !currentValue.equals("nil")) {
                current = Long.parseLong(currentValue);
            }

            if (log.isTraceEnabled()) {
                log.trace(String.format("Key %s is increased from %s to %s", key, currentValue, incrementedValue));
            }

            return current;

        } finally {
            log.info("Time taken to asyncGetAndAddDistributedCounter :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to asyncGetAndAddDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long asyncAddCounter(String key, long value) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            long incrementedValue = valkeyClient.incrBy(key, value);
            if (log.isTraceEnabled()) {
                log.trace(String.format("Key %s is increased from %s to %s", key, incrementedValue - value, incrementedValue));
            }

            return incrementedValue;
        } finally {
            log.info("Time taken to asyncAddDistributedCounter :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to asyncAddDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long asyncGetAndAlterCounter(String key, long value) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();
            long current = 0;

            String currentValue = valkeyClient.get(key);
            long incrementedValue = valkeyClient.incrBy(key, value);

            if (currentValue != null && !currentValue.equals("nil")) {
                current = Long.parseLong(currentValue);
            }

            if (log.isTraceEnabled()) {
                log.trace(String.format("Key %s increased from %s to %s", key, currentValue, incrementedValue));
            }

            return current;
        } finally {
            log.info("Time taken to asyncGetAndAlterDistributedCounter :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to asyncGetAndAlterDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long asyncGetAlterAndSetExpiryOfCounter(String key, long value, long expiryTimeStamp) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();
            long current = 0;

            String currentValue = valkeyClient.get(key);
            long incrementedValue = valkeyClient.incrBy(key, value);
            long setExpiry = valkeyClient.pexpireAt(key, expiryTimeStamp);

            if (currentValue != null && !currentValue.equals("nil")) {
                current = Long.parseLong(currentValue);
            }

            if (log.isTraceEnabled()) {
                log.trace(String.format("Key %s is increased from %s to %s with expiryTimeStamp %s", key, current, incrementedValue, setExpiry));
            }

            return current;
        } finally {
            log.info("Time taken to asyncGetAlterAndSetExpiryOfDistributedCounter :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to asyncGetAlterAndSetExpiryOfDistributedCounter :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long getTimestamp(String key) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            String value = valkeyClient.get(key);
            if (value == null || value.equals("nil")) {
                log.trace("Timestamp key doesn't exists. But returning 0");
                return 0;
            }

            return Long.parseLong(value);
        } finally {
            log.info("Time taken to getTimestamp :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to getTimestamp :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public void setTimestamp(String key, long timeStamp) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            valkeyClient.set(key, String.valueOf(timeStamp));

        } finally {
            log.info("Time taken to setTimestamp :" + (System.currentTimeMillis() - startTime));
            if (log.isTraceEnabled()) {
                log.trace("Time taken to setTimestamp :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public void setTimestampWithExpiry(String key, long timeStamp, long expiryTimeStamp) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            valkeyClient.set(key, String.valueOf(timeStamp));
            long setExpiry = valkeyClient.pexpireAt(key, expiryTimeStamp);
        } finally {
            if (log.isTraceEnabled()) {
                log.trace("Time taken to setTimestampWithExpiry :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public void removeTimestamp(String key) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            valkeyClient.del(key);
        } finally {
            if (log.isTraceEnabled()) {
                log.trace("Time taken to removeTimestamp :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public boolean isEnable() {

        return true;
    }

    @Override
    public String getType() {

        return "valkey";
    }

    @Override
    public void setExpiry(String key, long expiryTimeStamp) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();

            long setExpiry = valkeyClient.pexpireAt(key, expiryTimeStamp);
        } finally {
            if (log.isTraceEnabled()) {
                log.trace("Time taken to setExpiry :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long getTtl(String key) {

        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();
            long ttl = 0;

            ttl = valkeyClient.pttl(key);
            if (ttl == -2) {
                log.trace("TTL of key :" + key + " : " + ttl + " (Key does not exists)");
            } else if (ttl == -1) {
                log.trace("TTL of key :" + key + " : " + ttl + " (Key does not have an associated expire)");
            }

            return ttl;
        } finally {
            if (log.isTraceEnabled()) {
                log.trace("Time taken to getTtl in Valkey :" + (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    public long setLock(String key, String value) {
        return 0;
    }

    @Override
    public boolean setLockWithExpiry(String key, String value, long expiryTimeStamp) {
        return false;
    }

    @Override
    public long getKeyLockRetrievalTimeout() {
        return 0;
    }

    @Override
    public void removeLock(String key) {

    }

    private void saveMetricsToCsv() throws IOException {
        log.info("saving performance metrics to a csv file");

        // create a csv file write object first
        FileWriter csvWriter = new FileWriter(csvFileName);
        csvWriter.append("timestamp,operation,latency(ms)\n");
        csvWriter.flush();

        for (CounterLatencyMetrics metric: metrics) {
            csvWriter.append(String.format("%d,%s,%d\n", metric.timestamp, metric.operation, metric.latency()));
        }

        csvWriter.flush();
        csvWriter.close();

        // close the metric write scheduler
        metricsWriterService.shutdown();
    }

    public void shutdown() throws IOException {
        valkeyClient.close();
        saveMetricsToCsv();
        metricsWriterService.shutdown();
    }

    /**
     * DTO for holds the request latency, operation and its timestamp for performance metrics of the counter
     */
    private static class CounterLatencyMetrics {

        enum Operation {
            INCREMENT_AND_GET, RESET
        }

        private final long timestamp;
        private final Operation operation;
        private final long requestLatencyInMs;

        public CounterLatencyMetrics(long timestamp, Operation operation, long latency) {
            this.timestamp = timestamp;
            this.operation = operation;
            this.requestLatencyInMs = latency;
        }

        public Operation operation() {
            return operation;
        }

        public long timestamp() {
            return  timestamp;
        }

        public long latency() {
            return requestLatencyInMs;
        }
    }
}
