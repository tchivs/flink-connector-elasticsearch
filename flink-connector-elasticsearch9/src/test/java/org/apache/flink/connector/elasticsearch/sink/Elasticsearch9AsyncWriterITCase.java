/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.metrics.Gauge;

import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link Elasticsearch9AsyncWriter}. */
public class Elasticsearch9AsyncWriterITCase extends ElasticsearchSinkBaseITCase {
    private TestSinkInitContext context;

    private final Lock lock = new ReentrantLock();

    private final Condition completed = lock.newCondition();

    @BeforeEach
    void setUp() {
        this.context = new TestSinkInitContext();
    }

    @TestTemplate
    @Timeout(5)
    public void testBulkOnFlush()
            throws IOException, InterruptedException, org.apache.hc.core5.http.ParseException {
        String index = "test-bulk-on-flush";
        int maxBatchSize = 2;

        try (final Elasticsearch9AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);

            writer.flush(false);
            assertIdsAreWritten(index, new String[] {"test-1", "test-2"});

            writer.write(new DummyData("3", "test-3"), null);

            writer.flush(true);
            assertIdsAreWritten(index, new String[] {"test-3"});
        }
    }

    @TestTemplate
    @Timeout(5)
    public void testBulkOnBufferTimeFlush() throws Exception {
        String index = "test-bulk-on-time-in-buffer";
        int maxBatchSize = 3;

        try (final Elasticsearch9AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.flush(true);

            assertIdsAreWritten(index, new String[] {"test-1"});

            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            assertIdsAreNotWritten(index, new String[] {"test-2", "test-3"});
            context.getTestProcessingTimeService().advance(6000L);

            await();
        }

        assertIdsAreWritten(index, new String[] {"test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(5)
    public void testBytesSentMetric() throws Exception {
        String index = "test-bytes-sent-metrics";
        int maxBatchSize = 3;

        try (final Elasticsearch9AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            assertThat(context.getNumBytesOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumBytesOutCounter().getCount()).isGreaterThan(0);
        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(5)
    public void testRecordsSentMetric() throws Exception {
        String index = "test-records-sent-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch9AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(3);
        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(5)
    public void testSendTimeMetric() throws Exception {
        String index = "test-send-time-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch9AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            final Optional<Gauge<Long>> currentSendTime = context.getCurrentSendTimeGauge();

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();

            assertThat(currentSendTime).isPresent();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }

        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(5)
    public void testHandlePartiallyFailedBulk() throws Exception {
        String index = "test-partially-failed-bulk";
        int maxBatchSize = 2;

        Elasticsearch9AsyncSinkBuilder.OperationConverter<DummyData> elementConverter =
                new Elasticsearch9AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) ->
                                new UpdateOperation.Builder<>()
                                        .id(element.getId())
                                        .index(index)
                                        .action(
                                                ac ->
                                                        ac.doc(element)
                                                                .docAsUpsert(
                                                                        element.getId()
                                                                                .equals("test-2")))
                                        .build());

        try (final Elasticsearch9AsyncWriter<DummyData> writer =
                createWriter(maxBatchSize, elementConverter)) {
            writer.write(new DummyData("test-1", "test-1-updated"), null);
            writer.write(new DummyData("test-2", "test-2-updated"), null);
        }

        await();

        assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(1);
        assertIdsAreWritten(index, new String[] {"test-2"});
        assertIdsAreNotWritten(index, new String[] {"test-1"});
    }

    /**
     * {@code close()} must release the client's reactor pool. Each writer creates its own
     * {@code ElasticsearchAsyncClient}, so leaking it accumulates a whole reactor pool (33
     * {@code elasticsearch-rest-client-N-thread-M} threads) plus its Netty direct buffers per
     * writer, for the lifetime of the TaskManager JVM. That is not merely a thread leak: exhausted
     * direct memory makes unrelated Netty clients in the same JVM fail to allocate send buffers,
     * which was observed as a RocketMQ source logging {@code RemotingSendRequestException} forever
     * while silently consuming nothing.
     *
     * <p>Counts pools, not threads: this test class keeps its own assertion {@link Rest5Client},
     * whose threads share the {@code elasticsearch-rest-client} prefix and start lazily, so a raw
     * thread count drifts for reasons unrelated to the writer. Each client gets its own numbered
     * pool, so a pool surviving {@code close()} is exactly one leaked client.
     *
     * <p>Fails when {@code close()} calls {@code esClient.shutdown()} — the accessor for the
     * Elasticsearch Shutdown API namespace, which releases nothing.
     */
    @TestTemplate
    @Timeout(120)
    public void testCloseReleasesTheClientReactorThreads() throws Exception {
        String index = "test-close-releases-reactor-threads";
        Set<String> poolsBefore = restClientThreadPools();

        for (int round = 0; round < 3; round++) {
            try (Elasticsearch9AsyncWriter<DummyData> writer = createWriter(index, 1)) {
                writer.write(new DummyData("close-" + round, "close-" + round), null);
                writer.flush(true);
            }
        }

        long deadline = System.currentTimeMillis() + 30_000;
        Set<String> leaked = leakedPools(poolsBefore);
        while (!leaked.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(250);
            leaked = leakedPools(poolsBefore);
        }
        assertThat(leaked)
                .as("close() must release every reactor pool it created (before=%s)", poolsBefore)
                .isEmpty();
    }

    private static Set<String> leakedPools(Set<String> poolsBefore) {
        Set<String> leaked = new TreeSet<>(restClientThreadPools());
        leaked.removeAll(poolsBefore);
        return leaked;
    }

    /** Distinct {@code elasticsearch-rest-client-N} pool names currently alive; one per client. */
    private static Set<String> restClientThreadPools() {
        Set<String> pools = new TreeSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            String name = thread.getName();
            int threadSuffix = name.lastIndexOf("-thread-");
            if (name.startsWith("elasticsearch-rest-client") && threadSuffix > 0) {
                pools.add(name.substring(0, threadSuffix));
            }
        }
        return pools;
    }

    private Elasticsearch9AsyncWriter<DummyData> createWriter(String index, int maxBatchSize)
            throws IOException {
        return createWriter(
                maxBatchSize,
                new Elasticsearch9AsyncSinkBuilder.OperationConverter<>(
                        getElementConverterForDummyData(index)));
    }

    private NetworkConfig createNetworkConfig() {
        final List<HttpHost> esHost = Collections.singletonList(getHost());
        return secure
                ? new NetworkConfig(
                        esHost,
                        ES_CLUSTER_USERNAME,
                        ES_CLUSTER_PASSWORD,
                        null,
                        null,
                        null,
                        null,
                        null,
                        () -> ES_CONTAINER_SECURE.createSslContextFromCa(),
                        null)
                : new NetworkConfig(esHost, null, null, null, null, null, null, null, null, null);
    }

    private Elasticsearch9AsyncWriter<DummyData> createWriter(
            int maxBatchSize,
            Elasticsearch9AsyncSinkBuilder.OperationConverter<DummyData> elementConverter)
            throws IOException {

        Elasticsearch9AsyncSink<DummyData> sink =
                new Elasticsearch9AsyncSink<DummyData>(
                        elementConverter,
                        maxBatchSize,
                        50,
                        10_000,
                        5 * 1024 * 1024,
                        5000,
                        1024 * 1024,
                        createNetworkConfig()) {
                    @Override
                    public StatefulSinkWriter createWriter(WriterInitContext context) {
                        return new Elasticsearch9AsyncWriter<DummyData>(
                                getElementConverter(),
                                context,
                                maxBatchSize,
                                getMaxInFlightRequests(),
                                getMaxBufferedRequests(),
                                getMaxBatchSizeInBytes(),
                                getMaxTimeInBufferMS(),
                                getMaxRecordSizeInBytes(),
                                networkConfig,
                                Collections.emptyList()) {
                            @Override
                            protected void submitRequestEntries(
                                    List<Operation> requestEntries,
                                    ResultHandler<Operation> resultHandler) {
                                ResultHandler<Operation> wrappedHandler =
                                        new ResultHandler<Operation>() {
                                            @Override
                                            public void complete() {
                                                resultHandler.complete();
                                                signal();
                                            }

                                            @Override
                                            public void completeExceptionally(Exception e) {
                                                resultHandler.completeExceptionally(e);
                                                signal();
                                            }

                                            @Override
                                            public void retryForEntries(List<Operation> list) {
                                                resultHandler.retryForEntries(list);
                                                signal();
                                            }
                                        };
                                super.submitRequestEntries(requestEntries, wrappedHandler);
                            }
                        };
                    }
                };

        return (Elasticsearch9AsyncWriter<DummyData>) sink.createWriter(context);
    }

    private void signal() {
        lock.lock();
        try {
            completed.signal();
        } finally {
            lock.unlock();
        }
    }

    private void await() throws InterruptedException {
        lock.lock();
        try {
            completed.await();
        } finally {
            lock.unlock();
        }
    }
}
