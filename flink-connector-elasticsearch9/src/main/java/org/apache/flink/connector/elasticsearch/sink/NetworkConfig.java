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

import org.apache.flink.util.function.SerializableSupplier;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalTimeSerializer;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsStore;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A factory that creates valid ElasticsearchClient instances.
 *
 * <p>ES 9.x port: builds the low-level client through {@link Rest5ClientBuilder} (Apache
 * HttpComponents 5). The SSL {@link HostnameVerifier} is not exposed by the HttpComponents 5 async
 * builder; when one is supplied a warning is logged and the default hostname verification (from the
 * configured {@link SSLContext}) applies.
 */
public class NetworkConfig implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkConfig.class);

    private final List<HttpHost> hosts;
    private final List<Header> headers;
    private final String username;
    private final String password;
    @Nullable private final String connectionPathPrefix;
    @Nullable Integer connectionRequestTimeout;
    @Nullable Integer connectionTimeout;
    @Nullable Integer socketTimeout;
    @Nullable private final SerializableSupplier<SSLContext> sslContextSupplier;
    @Nullable private final SerializableSupplier<HostnameVerifier> sslHostnameVerifier;
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    public NetworkConfig(
            List<HttpHost> hosts,
            String username,
            String password,
            List<Header> headers,
            @Nullable String connectionPathPrefix,
            @Nullable Integer connectionRequestTimeout,
            @Nullable Integer connectionTimeout,
            @Nullable Integer socketTimeout,
            @Nullable SerializableSupplier<SSLContext> sslContextSupplier,
            @Nullable SerializableSupplier<HostnameVerifier> sslHostnameVerifier) {
        checkState(!hosts.isEmpty(), "Hosts must not be empty");
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.headers = headers;
        this.connectionRequestTimeout = connectionRequestTimeout;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.connectionPathPrefix = connectionPathPrefix;
        this.sslContextSupplier = sslContextSupplier;
        this.sslHostnameVerifier = sslHostnameVerifier;
    }

    public ElasticsearchAsyncClient createEsClient() {
        // the JavaTimeModule is added to provide support for java 8 Time classes.
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(
                LocalDateTime.class, new LocalDateTimeSerializer(DATE_TIME_FORMATTER));
        javaTimeModule.addSerializer(LocalDate.class, new LocalDateSerializer(DATE_FORMATTER));
        javaTimeModule.addSerializer(LocalTime.class, new LocalTimeSerializer(TIME_FORMATTER));
        ObjectMapper mapper = JsonMapper.builder().addModule(javaTimeModule).build();
        return new ElasticsearchAsyncClient(
                new Rest5ClientTransport(this.getRest5Client(), new JacksonJsonpMapper(mapper)));
    }

    public ElasticsearchClient createEsSyncClient() {
        return new ElasticsearchClient(
                new Rest5ClientTransport(this.getRest5Client(), new JacksonJsonpMapper()));
    }

    private Rest5Client getRest5Client() {
        Rest5ClientBuilder rest5ClientBuilder = Rest5Client.builder(hosts.toArray(new HttpHost[0]));

        if (username != null && password != null) {
            rest5ClientBuilder.setHttpClientConfigCallback(
                    httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(getCredentials()));
        }

        if (sslContextSupplier != null) {
            rest5ClientBuilder.setSSLContext(sslContextSupplier.get());
        }

        if (sslHostnameVerifier != null) {
            LOG.warn(
                    "A custom SSL HostnameVerifier was supplied, but the Elasticsearch 9.x Java "
                            + "client (HttpComponents 5) does not expose a builder-level hostname "
                            + "verifier; the default verification of the configured SSLContext "
                            + "applies.");
        }

        if (headers != null) {
            rest5ClientBuilder.setDefaultHeaders(headers.toArray(new Header[0]));
        }

        if (connectionPathPrefix != null) {
            rest5ClientBuilder.setPathPrefix(connectionPathPrefix);
        }

        if (connectionRequestTimeout != null
                || connectionTimeout != null
                || socketTimeout != null) {
            rest5ClientBuilder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (connectionRequestTimeout != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    connectionRequestTimeout, TimeUnit.MILLISECONDS);
                        }
                        if (connectionTimeout != null) {
                            requestConfigBuilder.setConnectTimeout(
                                    connectionTimeout, TimeUnit.MILLISECONDS);
                        }
                        if (socketTimeout != null) {
                            requestConfigBuilder.setResponseTimeout(
                                    socketTimeout, TimeUnit.MILLISECONDS);
                        }
                    });
        }

        return rest5ClientBuilder.build();
    }

    private CredentialsStore getCredentials() {
        final CredentialsStore credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(
                new AuthScope((HttpHost) null),
                new UsernamePasswordCredentials(username, password.toCharArray()));

        return credentialsProvider;
    }
}
