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

import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Elasticsearch bulk-item failure classification. */
public class Elasticsearch9AsyncWriterTest {

    @Test
    public void testCloseCanBeRetriedAfterClientCloseFailure() {
        AtomicInteger closeAttempts = new AtomicInteger();
        Elasticsearch9AsyncWriter.ClientCloser closer =
                new Elasticsearch9AsyncWriter.ClientCloser(
                        () -> {
                            if (closeAttempts.incrementAndGet() == 1) {
                                throw new IOException("first close failed");
                            }
                        });

        assertThatThrownBy(closer::close)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Could not close the Elasticsearch client")
                .hasCauseInstanceOf(IOException.class);

        closer.close();
        closer.close();
        assertThat(closeAttempts).hasValue(2);
    }

    @ParameterizedTest
    @ValueSource(ints = {408, 429, 500, 502, 503, 504, 599})
    public void testRetryableStatuses(int status) {
        assertThat(Elasticsearch9AsyncWriter.isRetryableStatus(status)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 409, 422, 600})
    public void testNonRetryableStatuses(int status) {
        assertThat(Elasticsearch9AsyncWriter.isRetryableStatus(status)).isFalse();
    }

    @Test
    public void testOnlyRetryableBulkItemsAreRequeued() {
        Operation successful = operation("ok");
        Operation retryable = operation("retry");
        BulkResponse response =
                response(
                        successItem("ok"),
                        failedItem("retry", 429, "es_rejected_execution_exception"));

        List<Operation> result =
                Elasticsearch9AsyncWriter.retryableEntriesOrThrow(
                        List.of(successful, retryable), response);

        assertThat(result).containsExactly(retryable);
    }

    @Test
    public void testPermanentBulkFailureFailsWithoutExposingReasonOrDocumentId() {
        Operation operation = operation("sensitive-document-id");
        BulkResponse response =
                response(
                        failedItem(
                                "sensitive-document-id",
                                400,
                                "document_parsing_exception",
                                "failed to parse sensitive-field-value"));

        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncWriter.retryableEntriesOrThrow(
                                        List.of(operation), response))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("status=400")
                .hasMessageContaining("errorType=document_parsing_exception")
                .hasMessageNotContaining("sensitive-field-value")
                .hasMessageNotContaining("sensitive-document-id");
    }

    private static Operation operation(String id) {
        return new Operation(
                new UpdateOperation.Builder<Map<String, String>, Map<String, String>>()
                        .index("target-index")
                        .id(id)
                        .action(action -> action.doc(Map.of("field", "value")))
                        .build());
    }

    private static BulkResponse response(BulkResponseItem... items) {
        return new BulkResponse.Builder().errors(true).items(List.of(items)).took(1).build();
    }

    private static BulkResponseItem successItem(String id) {
        return new BulkResponseItem.Builder()
                .operationType(OperationType.Update)
                .index("target-index")
                .id(id)
                .status(200)
                .build();
    }

    private static BulkResponseItem failedItem(String id, int status, String errorType) {
        return failedItem(id, status, errorType, "transient failure");
    }

    private static BulkResponseItem failedItem(
            String id, int status, String errorType, String reason) {
        return new BulkResponseItem.Builder()
                .operationType(OperationType.Update)
                .index("target-index")
                .id(id)
                .status(status)
                .error(error -> error.type(errorType).reason(reason))
                .build();
    }
}
