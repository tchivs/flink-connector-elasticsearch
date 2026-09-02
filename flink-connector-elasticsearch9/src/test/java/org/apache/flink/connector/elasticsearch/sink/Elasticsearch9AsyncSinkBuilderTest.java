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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link Elasticsearch9AsyncSinkBuilder}. */
public class Elasticsearch9AsyncSinkBuilderTest {

    private HttpServer proxyServer;

    @AfterEach
    void stopProxyServer() {
        if (proxyServer != null) {
            proxyServer.stop(0);
        }
    }

    @Test
    void testRoutesRequestsThroughConfiguredHttpProxy() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> host = new AtomicReference<>();
        proxyServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        proxyServer.createContext(
                "/",
                exchange -> {
                    try {
                        method.set(exchange.getRequestMethod());
                        host.set(exchange.getRequestHeaders().getFirst("Host"));
                        respondSuccess(exchange);
                    } finally {
                        exchange.close();
                    }
                });
        proxyServer.start();

        HttpHost proxy = new HttpHost("http", "127.0.0.1", proxyServer.getAddress().getPort());
        NetworkConfig networkConfig =
                Elasticsearch9AsyncSinkBuilder.<String>builder()
                        .setHosts(new HttpHost("http", "127.0.0.1", 1))
                        .setHttpProxy(proxy)
                        .buildNetworkConfig();

        try (ElasticsearchClient client = networkConfig.createEsSyncClient()) {
            assertThat(client.indices().exists(request -> request.index("proxy-check")).value())
                    .isTrue();
        }

        assertThat(method.get()).isEqualTo("HEAD");
        assertThat(host.get()).isEqualTo("127.0.0.1:1");
    }

    @Test
    void testThrowExceptionIfHttpProxyIsNull() {
        assertThatThrownBy(
                        () -> Elasticsearch9AsyncSinkBuilder.<String>builder().setHttpProxy(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfElementConverterIsNotProvided() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncSinkBuilder.<String>builder()
                                        .setHosts(new HttpHost("localhost", 9200))
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfHostsAreNotProvided() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncSinkBuilder.<String>builder()
                                        .setElementConverter(
                                                (element, ctx) ->
                                                        new DeleteOperation.Builder()
                                                                .id("test")
                                                                .index("test")
                                                                .build())
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfHostsIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncSinkBuilder.<String>builder()
                                        .setHosts(null)
                                        .setElementConverter(
                                                (element, ctx) ->
                                                        new DeleteOperation.Builder()
                                                                .id("test")
                                                                .index("test")
                                                                .build())
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfUsernameIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncSinkBuilder.<String>builder()
                                        .setUsername(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfPasswordIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncSinkBuilder.<String>builder()
                                        .setPassword(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfHeadersAreNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncSinkBuilder.<String>builder()
                                        .setHeaders(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfCertificateFingerprintIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch9AsyncSinkBuilder.<String>builder()
                                        .setCertificateFingerprint(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    private static void respondSuccess(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().add("X-Elastic-Product", "Elasticsearch");
        exchange.sendResponseHeaders(200, -1);
    }
}
