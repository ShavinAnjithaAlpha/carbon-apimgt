package org.wso2.carbon.apimgt.gateway.handlers.throttling;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakException;
import com.basho.riak.client.api.commands.datatypes.CounterUpdate;
import com.basho.riak.client.api.commands.datatypes.FetchCounter;
import com.basho.riak.client.api.commands.datatypes.UpdateCounter;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@code RiakCounter} class provides a distributed counter mechanism using Riak KV's CRDT (Conflict-free Replicated Data Type)
 * capabilities. This class is designed to support global throttling use cases in distributed API Manager setup.
 *
 * <p>Initialization requires specifying Riak host nodes and setting up a {@link RiakCluster} and {@link RiakClient}.
 * This class assumes counters are stored in a specified bucket type and name. All counter operations are logged,
 * and exceptions are handled gracefully to ensure system stability.</p>
 *
 * <p>Intended usage includes rate limiting, API usage metering, and other distributed coordination tasks where counters
 * need to be shared across nodes in a fault-tolerant way.</p>
 *
 * @see com.basho.riak.client.api.commands.datatypes.CounterUpdate
 * @see com.basho.riak.client.api.commands.datatypes.FetchCounter
 * @see com.basho.riak.client.api.commands.datatypes.UpdateCounter
 * @see com.basho.riak.client.api.commands.kv.DeleteValue
 */
public class RiakCounter {

    private final static Log log = LogFactory.getLog(RiakCounter.class);

    public static final int DEFAULT_RIAK_CONNECTION_TIMEOUT_MS = 1000;
    public static final int DEFAULT_MAX_CONNECTIONS_PER_NODE = 5;
    private final static String DEFAULT_RIAK_COUNTER_BUCKET_NAME = "HARD_THROTTLING_COUNTERS_APIM";
    // private final static String DEFAULT_COUNTER_BUCKET_TYPE = "throttle_counter";
    private final static String DEFAULT_COUNTER_BUCKET_TYPE = "test-cnt";

    private final static int RIAK_DEFAULT_PROTOBUF_PORT = 8087;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final ConcurrentSkipListSet<String> throttleKeys = new ConcurrentSkipListSet<>();

    private final List<String> hostAddresses;
    private int maxConnectionTimeOut;
    private int maxConnectionsPerNode;
    private RiakCluster cluster;
    private RiakClient client;
    private Namespace counterNamespace;
    private final AtomicBoolean initialized; // identified whether if the counters is initialized prior to perform queries
    private final int cleanupFrequencyInMs;

    // benchmark only related fields
    private boolean recordMetrics = false;
    private List<RiakCounterLatencyMetrics> metrics;
    private ScheduledExecutorService metricsWriterService;
    private String csvFileName;

    public RiakCounter(int cleanupFrequencyInMs) {
        this.hostAddresses = new LinkedList<>();
        this.cleanupFrequencyInMs = cleanupFrequencyInMs;

        this.initialized = new AtomicBoolean(false); // set the initialized property to false

        maxConnectionTimeOut = DEFAULT_RIAK_CONNECTION_TIMEOUT_MS;
        maxConnectionsPerNode = DEFAULT_MAX_CONNECTIONS_PER_NODE;
    }

    public  RiakCounter(int cleanupFrequencyInMs, int maxConnectionTimeOut) {
        this(cleanupFrequencyInMs);

        if (maxConnectionTimeOut <= 0)
            throw new IllegalArgumentException("Connection timeout must be positive value");
        this.maxConnectionTimeOut = maxConnectionTimeOut;
    }

    public RiakCounter(int cleanupFrequencyInMs, int maxConnectionTimeOut, int maxConnectionsPerNode) {
        this(cleanupFrequencyInMs, maxConnectionTimeOut);

        if (maxConnectionsPerNode <= 0)
            throw new IllegalArgumentException("Maximum connections per node must be a positive value");

        this.maxConnectionsPerNode = maxConnectionsPerNode;
    }

    public RiakCounter(int cleanupFrequencyInMs, int maxConnectionTimeOut, int maxConnectionsPerNode, boolean recordMetrics) {
        this(cleanupFrequencyInMs, maxConnectionTimeOut, maxConnectionsPerNode);
        this.recordMetrics = recordMetrics;
    }

    public void addHosts(String... hosts) {
        hostAddresses.addAll(Arrays.asList(hosts));
    }

    public void addHost(String host) {
        hostAddresses.add(host);
    }

    public void initRiak() throws UnknownHostException {
        if (hostAddresses.isEmpty()) {
            log.warn("no host addresses to connect. exit");
            return;
        }

        RiakNode.Builder builder = new RiakNode.Builder();
        builder.withMaxConnections(maxConnectionsPerNode);
        builder.withConnectionTimeout(maxConnectionTimeOut);

        List<RiakNode> nodes = RiakNode.Builder.buildNodes(builder, hostAddresses);

        // create a riak cluster object with the build node
        RiakCluster cluster = new RiakCluster.Builder(nodes)
                .build();

        // set the cluster object
        this.cluster = cluster;
        // start the cluster and initiate connection to the cluster
        cluster.start();
        log.info(String.format("Connected to the RiakCounter with %s", hostsToString()));

        // create a client object out of riak cluster object for CRUD operations
        client = new RiakClient(cluster);

        // set up the counter namespace in the riak kv
        counterNamespace = new Namespace( DEFAULT_COUNTER_BUCKET_TYPE, DEFAULT_RIAK_COUNTER_BUCKET_NAME);
        log.info("Namespace for distributed counters in riak initialized with name: " + counterNamespace.getBucketNameAsString());

        if (recordMetrics) initRecordMetrics();

        // start the throttle counters cleanup task
        loadExistingThrottlingKeys();
        startThrottleCounterCleanupTask();
        initialized.set(true);
    }

    private void loadExistingThrottlingKeys() {
        try {
            // create a new location object with the provided throttling key
            ListKeys listKeys = new ListKeys.Builder(counterNamespace).build();
            ListKeys.Response response = client.execute(listKeys);

            int c = 0;
            for (Location location: response) {
                c++;
                throttleKeys.add(location.getKeyAsString());
            }

            log.info("throttle keys populate with " + c + " keys");
        } catch (ExecutionException|InterruptedException exception) {
            log.error("Error while populating initial throttling keys");
        }
    }

    private void initRecordMetrics() {
        this.metrics = new ArrayList<>(8092);
        metricsWriterService = Executors.newScheduledThreadPool(1);
        csvFileName = "apim_riak_perf_" + System.currentTimeMillis() + ".csv";

        metricsWriterService.scheduleAtFixedRate(() -> {
            if (metrics.isEmpty()) return;

            try {
                // create a csv file write object first
                FileWriter csvWriter = new FileWriter(csvFileName);
                csvWriter.append("timestamp,operation,latency(ms)\n");
                csvWriter.flush();

                for (RiakCounterLatencyMetrics metric: metrics) {
                    csvWriter.append(String.format("%d,%s,%d\n", metric.timestamp, metric.operation, metric.latency()));
                }

                csvWriter.flush();
                csvWriter.close();

                // clear the metric data
                metrics.clear();
            } catch (IOException exception) {
                log.warn("Riak metric writing failed: " + exception.getMessage());
            }

        }, 5, 30, TimeUnit.SECONDS);
    }

    private void startThrottleCounterCleanupTask() {
        executor.scheduleAtFixedRate(() -> {
            log.info("starting riak throttling counter cleanup task");
            // start clearing throttling keys in the riak counter
            String throttleKey;
            while (null != (throttleKey = throttleKeys.pollFirst())) {
                try {
                    resetThrottlingCounter(throttleKey);
                } catch (Exception e) {
                    log.info("failed to reset throttling counter for key: " + throttleKey);
                    throttleKeys.add(throttleKey);
                }
            }

            // FOR TESTING ONLY
            log.info("finished riak throttle counters cleaning");
        }, 0, cleanupFrequencyInMs, TimeUnit.MILLISECONDS);
    }

    public long getThrottlingKey(String throttlingKey) throws Exception {
        if (!initialized.get()) {
            log.error("riak counters is not initialized");
            throw new RiakException("Riak Counter not initialized");
        }

        long startTime = 0;
        try {
            try {
                startTime = System.currentTimeMillis();
                // create a new location object with the provided throttling key
                Location location = new Location(counterNamespace, throttlingKey);

                // create a fetch counter query
                FetchCounter fetchCounter = new FetchCounter.Builder(location)
                        .build();

                // execute the fetch counter update using the client object
                FetchCounter.Response response = client.execute(fetchCounter);
                if (response != null && response.getDatatype() != null) {
                    return response.getDatatype().view();
                } else {
                    log.trace(String.format("Key %s does not exists in the Riak", throttlingKey));
                }
                log.trace("shared riak counter does not exists. but returning 0");
                return 0;
            } catch (ExecutionException|InterruptedException exception) {
                log.info("fetching throttling key is failed: " + throttlingKey);
                handleException(exception);
                throw new Exception("failed to fetch throttling key from riak: " + throttlingKey);
            }
        } finally {
            log.info("Time taken to to getDistributedRiakThrottlingKey : " + (System.currentTimeMillis() - startTime));
        }
    }

    public long incrementThrottlingCounter(String throttlingKey, long delta) throws Exception {
        if (!initialized.get()) {
            log.error("riak counters is not initialized");
            throw new RiakException("Riak Counter not initialized");
        }
        long startTime = 0;
        try {
            try {
                startTime = System.currentTimeMillis();
                // create a new location object with the provided throttling key
                Location location = new Location(counterNamespace, throttlingKey);

                // create a new counter update object
                CounterUpdate counterUpdate = new CounterUpdate(delta);

                UpdateCounter update = new UpdateCounter.Builder(location, counterUpdate).withReturnDatatype(true)
                        .build();

                // execute the counter increment query using the client
                UpdateCounter.Response response = client.execute(update);

                if (response != null && response.getDatatype() != null) {
                    throttleKeys.add(throttlingKey); // add to the throttling key queue
                    return response.getDatatype().view();
                } else {
                    log.info(String.format("failed to increment the counter with Key %s", throttlingKey));
                }
                return 0;
            } catch (ExecutionException|InterruptedException exception) {
                log.info("failed to increment the counter with throttling key: " + throttlingKey);
                handleException(exception);
                throw exception;
            }
        } finally {
            if (recordMetrics)
                metrics.add(new RiakCounterLatencyMetrics(System.currentTimeMillis(), RiakCounterLatencyMetrics.Operation.INCREMENT_AND_GET, (System.currentTimeMillis() - startTime)));
            log.trace("Time taken to incrementAndGetDistributedRiakCounter: " + (System.currentTimeMillis() - startTime));
        }
    }

    public void resetThrottlingCounter(String throttlingKey) throws Exception {
        if (!initialized.get()) {
            log.error("riak counters is not initialized");
            throw new RiakException("Riak Counter not initialized");
        }

        long startTime = 0;
        try {
            try {
                startTime = System.currentTimeMillis();
                // create a new location object with the provided throttling key
                Location location = new Location(counterNamespace, throttlingKey);
                DeleteValue deleteValue = new DeleteValue.Builder(location).build();

                client.execute(deleteValue);
            } catch (InterruptedException|ExecutionException exception) {
                handleException(exception);
                throw exception;
            }
        } finally {
            if (recordMetrics)
                metrics.add(new RiakCounterLatencyMetrics(System.currentTimeMillis(), RiakCounterLatencyMetrics.Operation.RESET, (System.currentTimeMillis() - startTime)));
            log.trace("Time taken for resetRiakDistributedCounterKey: " + (System.currentTimeMillis() - startTime));
        }
    }

    public void asyncIncrementThrottlingCounter(String throttlingKey) {
        // create a new location object with the provided throttling key
        Location location = new Location(counterNamespace, throttlingKey);

        // create a new counter update object
        CounterUpdate counterUpdate = new CounterUpdate(1);

        UpdateCounter update = new UpdateCounter.Builder(location, counterUpdate).withReturnDatatype(true)
                .build();

        // execute the counter increment query using the client
        RiakFuture<UpdateCounter.Response, Location> incrementFuture = client.executeAsync(update);
        incrementFuture.addListener(riakFuture -> {
            if (riakFuture.isDone() && !riakFuture.isSuccess()) {
                log.warn("fetching throttling key is failed");
            }
        });
    }

    public void handleException(Exception exception) {
        // exception handle logic goes here
    }

    @Override
    public String toString() {
        return String.format("RiakDistributedCounter(cluster = %s, maxConnectionsPerNode = %d, maxConnectionTimeOut = %d, port = %d, bucketType = %s, bucketName = %s)", hostsToString(),
                maxConnectionsPerNode, maxConnectionTimeOut, RIAK_DEFAULT_PROTOBUF_PORT, DEFAULT_COUNTER_BUCKET_TYPE, DEFAULT_RIAK_COUNTER_BUCKET_NAME);
    }

    private String hostsToString() {
        StringBuilder stringBuilder = new StringBuilder("RiakCluster(nodes = ");
        for (String host: hostAddresses) {
            stringBuilder.append(host);
            stringBuilder.append(", ");
        }

        stringBuilder.append(" )");
        return stringBuilder.toString();
    }

    private void saveMetricsToCsv() throws IOException {
        log.info("saving performance metrics to a csv file");

        // create a csv file write object first
        FileWriter csvWriter = new FileWriter(csvFileName);
        csvWriter.append("timestamp,operation,latency(ms)\n");
        csvWriter.flush();

        for (RiakCounterLatencyMetrics metric: metrics) {
            csvWriter.append(String.format("%d,%s,%d\n", metric.timestamp, metric.operation, metric.latency()));
        }

        csvWriter.flush();
        csvWriter.close();

        // close the metric write scheduler
        metricsWriterService.shutdown();
    }

    public void shutdown() throws IOException {
        if (!initialized.get()) return;

        if (recordMetrics) saveMetricsToCsv();

        // shutdown the cleaning executor
        executor.shutdownNow();

        try {
            Future<Boolean> shutdownFuture = cluster.shutdown();
            Boolean done = shutdownFuture.get(5000, TimeUnit.MILLISECONDS);
            if (done) {
                log.info("Riak Counter shutdown successfully");
                initialized.set(false);
            } else {
                log.info("Riak Counter shutdown failed");
            }
        } catch (InterruptedException exception) {
            log.info("interrupted while shutdown the Riak Counter");
            Thread.currentThread().interrupt();
            initialized.set(false);
        } catch (ExecutionException e) {
            log.info("Riak Counter shutdown failed due to execution failure");
        } catch (TimeoutException e) {
            log.info("Riak Counter shutdown failed due to timeout");
        }
    }

    /**
     * DTO for holds the request latency, operation and its timestamp for performance metrics of the counter
     */
    private static class RiakCounterLatencyMetrics {

        enum Operation {
            INCREMENT_AND_GET, RESET
        }

        private final long timestamp;
        private final Operation operation;
        private final long requestLatencyInMs;

        public RiakCounterLatencyMetrics(long timestamp, Operation operation, long latency) {
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