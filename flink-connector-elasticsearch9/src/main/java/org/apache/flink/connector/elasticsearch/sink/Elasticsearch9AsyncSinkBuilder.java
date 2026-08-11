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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.function.SerializableSupplier;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.transport.TransportUtils;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContexts;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Elasticsearch9AsyncSinkBuilder The builder to construct the Elasticsearch9Sink {@link
 * Elasticsearch9AsyncSink}.
 *
 * @param <InputT> the type of records to be sunk into an Elasticsearch cluster
 */
public class Elasticsearch9AsyncSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, Operation, Elasticsearch9AsyncSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1024 * 1024;

    /** The hosts where the Elasticsearch cluster is reachable. */
    private List<HttpHost> hosts;

    /** The headers to be sent with the requests made to Elasticsearch cluster. */
    private List<Header> headers;

    /** The username to authenticate the connection with the Elasticsearch cluster. */
    private String username;

    /** The password to authenticate the connection with the Elasticsearch cluster. */
    private String password;

    /**
     * The element converter that will be called on every stream element to be processed and
     * buffered.
     */
    private ElementConverter<InputT, BulkOperationVariant> elementConverter;

    /** the path's prefix for every request. */
    private String connectionPathPrefix;

    private Integer connectionTimeout;

    private Integer connectionRequestTimeout;

    private Integer socketTimeout;

    private SerializableSupplier<SSLContext> sslContextSupplier;

    /**
     * setHosts set the hosts where the Elasticsearch cluster is reachable.
     *
     * @param hosts the hosts address
     * @return {@code Elasticsearch9AsyncSinkBuilder}
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkArgument(hosts.length > 0, "Hosts cannot be empty");
        this.hosts = Arrays.asList(hosts);
        return this;
    }

    public Elasticsearch9AsyncSinkBuilder<InputT> setConnectionPathPrefix(
            String connectionPathPrefix) {
        this.connectionPathPrefix = connectionPathPrefix;
        return this;
    }

    public Elasticsearch9AsyncSinkBuilder<InputT> setConnectionTimeout(Integer connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public Elasticsearch9AsyncSinkBuilder<InputT> setConnectionRequestTimeout(
            Integer connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
        return this;
    }

    public Elasticsearch9AsyncSinkBuilder<InputT> setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    /**
     * setHeaders set the headers to be sent with the requests made to Elasticsearch cluster..
     *
     * @param headers the headers
     * @return {@code Elasticsearch9AsyncSinkBuilder}
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setHeaders(Header... headers) {
        checkNotNull(headers);
        checkArgument(headers.length > 0, "Headers cannot be empty");
        this.headers = Arrays.asList(headers);
        return this;
    }

    /**
     * Allows to bypass the certificates chain validation and connect to insecure network endpoints
     * (for example, servers which use self-signed certificates).
     *
     * @return this builder
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> allowInsecure() {
        this.sslContextSupplier =
                () -> {
                    try {
                        return SSLContexts.custom()
                                .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                                .build();
                    } catch (final NoSuchAlgorithmException
                            | KeyStoreException
                            | KeyManagementException ex) {
                        throw new IllegalStateException("Unable to create custom SSL context", ex);
                    }
                };
        return this;
    }

    /**
     * Set the certificate fingerprint to be used to verify the HTTPS connection.
     *
     * @param certificateFingerprint the certificate fingerprint
     * @return {@code Elasticsearch9AsyncSinkBuilder}
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setCertificateFingerprint(
            String certificateFingerprint) {
        checkNotNull(certificateFingerprint, "certificateFingerprint must not be null");
        this.sslContextSupplier =
                () -> TransportUtils.sslContextFromCaFingerprint(certificateFingerprint);
        return this;
    }

    /**
     * Sets the supplier for getting an {@link SSLContext} instance.
     *
     * @param sslContextSupplier the serializable SSLContext supplier function
     * @return this builder
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setSslContextSupplier(
            SerializableSupplier<SSLContext> sslContextSupplier) {
        this.sslContextSupplier = checkNotNull(sslContextSupplier);
        return this;
    }

    /**
     * Not supported on the Elasticsearch 9 connector.
     *
     * <p>The HttpComponents 5 async transport used by the ES 9.x Java client does not expose a
     * builder-level hostname verifier; hostname verification is governed by the configured {@link
     * SSLContext} (or {@link #setCertificateFingerprint(String)}).
     *
     * @param sslHostnameVerifierSupplier unused
     * @return this builder
     * @throws UnsupportedOperationException always, so the limitation surfaces at build time
     *     instead of silently degrading to default hostname verification.
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setSslHostnameVerifier(
            SerializableSupplier<HostnameVerifier> sslHostnameVerifierSupplier) {
        throw new UnsupportedOperationException(
                "Elasticsearch 9 (HttpComponents 5 async transport) does not expose a builder-level "
                        + "SSL HostnameVerifier; configure hostname verification via "
                        + "setSslContextSupplier(...) or setCertificateFingerprint(...).");
    }

    /**
     * setUsername set the username to authenticate the connection with the Elasticsearch cluster.
     *
     * @param username the auth username
     * @return {@code Elasticsearch9AsyncSinkBuilder}
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setUsername(String username) {
        checkNotNull(username, "Username must not be null");
        this.username = username;
        return this;
    }

    /**
     * setPassword set the password to authenticate the connection with the Elasticsearch cluster.
     *
     * @param password the auth password
     * @return {@code Elasticsearch9AsyncSinkBuilder}
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setPassword(String password) {
        checkNotNull(password, "Password must not be null");
        this.password = password;
        return this;
    }

    /**
     * setElementConverter set the element converter that will be called at every stream element to
     * be processed and buffered.
     *
     * @param elementConverter elementConverter operation
     * @return {@code Elasticsearch9AsyncSinkBuilder}
     */
    public Elasticsearch9AsyncSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, BulkOperationVariant> elementConverter) {
        checkNotNull(elementConverter);
        this.elementConverter = elementConverter;
        return this;
    }

    public static <T> Elasticsearch9AsyncSinkBuilder<T> builder() {
        return new Elasticsearch9AsyncSinkBuilder<>();
    }

    /**
     * Creates an ElasticsearchSink instance.
     *
     * @return {@link Elasticsearch9AsyncSink}
     */
    @Override
    public Elasticsearch9AsyncSink<InputT> build() {
        return new Elasticsearch9AsyncSink<>(
                buildOperationConverter(elementConverter),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                buildNetworkConfig());
    }

    private OperationConverter<InputT> buildOperationConverter(
            ElementConverter<InputT, BulkOperationVariant> converter) {
        return converter != null ? new OperationConverter<>(converter) : null;
    }

    private NetworkConfig buildNetworkConfig() {
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");
        return new NetworkConfig(
                hosts,
                username,
                password,
                headers,
                connectionPathPrefix,
                connectionRequestTimeout,
                connectionTimeout,
                socketTimeout,
                sslContextSupplier,
                // HostnameVerifier is not supported by the ES 9.x (HttpComponents 5) transport;
                // setSslHostnameVerifier throws UnsupportedOperationException, so always null here.
                null);
    }

    /** A wrapper that evolves the Operation, since a BulkOperationVariant is not Serializable. */
    public static class OperationConverter<T> implements ElementConverter<T, Operation> {
        private final ElementConverter<T, BulkOperationVariant> converter;

        public OperationConverter(ElementConverter<T, BulkOperationVariant> converter) {
            this.converter = converter;
        }

        @Override
        public void open(WriterInitContext context) {
            // call converter.open() before calling converter.apply()
            converter.open(context);
        }

        @Override
        public Operation apply(T element, SinkWriter.Context context) {
            return new Operation(converter.apply(element, context));
        }
    }
}
