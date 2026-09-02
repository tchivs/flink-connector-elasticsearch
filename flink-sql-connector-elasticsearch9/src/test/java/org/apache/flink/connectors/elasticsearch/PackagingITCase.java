/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.elasticsearch;

import org.apache.flink.packaging.PackagingTestUtils;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;

class PackagingITCase {

    @Test
    void testPackaging() throws Exception {
        final Path jar =
                ResourceTestUtils.getResource(".*/flink-sql-connector-elasticsearch9-[^/]*\\.jar");

        PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                jar,
                Arrays.asList(
                        "META-INF/",
                        "org/apache/flink/connector/base/",
                        "org/apache/flink/connector/elasticsearch/",
                        "org/apache/flink/elasticsearch9/",
                        // Not relocated, although the shade configuration claims to relocate
                        // everything it bundles: jackson 3 moved to the `tools.jackson` namespace
                        // (the relocation only covers `com.fasterxml.jackson`) and httpclient5
                        // brings the public-suffix list. Listed so the layout is asserted rather
                        // than assumed; relocating them changes the artifact and needs its own
                        // verification against a real cluster.
                        "tools/jackson/",
                        "org/publicsuffix/"));
        PackagingTestUtils.assertJarContainsServiceEntry(jar, Factory.class);
    }
}
