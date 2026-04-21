// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import groovy.json.JsonSlurper

suite("test_clear_file_cache_on_load_failure", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    // Clear any existing debug points
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def getAliveBackends = {
        def aliveBackends = sql("""SHOW BACKENDS""").findAll { it[9].toString().equals("true") }
        logger.info("Alive backends for cache test: ${aliveBackends}")
        assertTrue(!aliveBackends.isEmpty(), "No alive backends found for cache test")
        return aliveBackends
    }

    // Helper function to clear file cache on all backends
    def clearFileCache = { ip, port ->
        def url = "http://${ip}:${port}/api/file_cache?op=clear&sync=true"
        def response = new URL(url).text
        def json = new JsonSlurper().parseText(response)
        if (json.status != "OK") {
            throw new RuntimeException("Clear cache on ${ip}:${port} failed: ${json.status}")
        }
    }

    def clearFileCacheOnAllBackends = {
        def backends = getAliveBackends()
        for (be in backends) {
            def ip = be[1]
            def port = be[4]
            clearFileCache(ip, port)
        }
        // Wait for async clear to complete
        sleep(5000)
    }

    def metricSuffixes = [
        "file_cache_cache_size",
        "file_cache_index_queue_cache_size",
        "file_cache_ttl_cache_size",
        "file_cache_normal_queue_cache_size",
        "file_cache_disposable_queue_cache_size"
    ]

    // Helper function to get brpc metric sum by suffix and log all matched lines.
    def getBrpcMetricSum = { ip, port, metricSuffix ->
        def url = "http://${ip}:${port}/brpc_metrics"
        try {
            def metrics = new URL(url).text
            long sum = 0L
            def matchedLines = []
            metrics.eachLine { line ->
                if (!line.startsWith("#")) {
                    def parts = line.trim().split(/\s+/)
                    if (parts.size() >= 2 && parts[0].endsWith(metricSuffix)) {
                        matchedLines.add(line.trim())
                        sum += parts[1].toLong()
                    }
                }
            }
            if (matchedLines.isEmpty()) {
                logger.warn("No metric matched suffix ${metricSuffix} on BE ${ip}:${port}")
            }
            logger.info("BE ${ip}:${port} metric ${metricSuffix} matched=${matchedLines} sum=${sum}")
            return sum
        } catch (Exception e) {
            logger.warn("Failed to get brpc metrics ${metricSuffix} from ${ip}:${port}: ${e.message}")
        }
        return 0L
    }

    def getClusterCacheMetricsSnapshot = {
        def backends = getAliveBackends()
        def detail = [:]
        def total = [:]
        metricSuffixes.each { suffix ->
            total[suffix] = 0L
        }
        for (be in backends) {
            def ip = be[1]
            def brpcPort = be[5]
            def backendMetrics = [:]
            metricSuffixes.each { suffix ->
                def value = getBrpcMetricSum(ip, brpcPort, suffix)
                backendMetrics[suffix] = value
                total[suffix] += value
            }
            detail["${ip}:${brpcPort}"] = backendMetrics
        }
        logger.info("Cluster cache metrics snapshot: total=${total}, detail=${detail}")
        return [total: total, detail: detail]
    }

    def getSnapshotDiff = { beforeSnapshot, afterSnapshot ->
        def diff = [:]
        metricSuffixes.each { suffix ->
            diff[suffix] = afterSnapshot.total[suffix] - beforeSnapshot.total[suffix]
        }
        return diff
    }

    def waitForCacheGrowth = { baselineSnapshot, long timeoutMs = 30000L, long intervalMs = 5000L ->
        long start = System.currentTimeMillis()
        def latestSnapshot = baselineSnapshot
        while (System.currentTimeMillis() - start < timeoutMs) {
            latestSnapshot = getClusterCacheMetricsSnapshot()
            if (latestSnapshot.total["file_cache_cache_size"] > baselineSnapshot.total["file_cache_cache_size"]
                    || latestSnapshot.total["file_cache_ttl_cache_size"] > baselineSnapshot.total["file_cache_ttl_cache_size"]
                    || latestSnapshot.total["file_cache_normal_queue_cache_size"] > baselineSnapshot.total["file_cache_normal_queue_cache_size"]) {
                return latestSnapshot
            }
            sleep(intervalMs)
        }
        return latestSnapshot
    }

    // Create test table with file cache enabled
    def tableName = "test_load_failure_cache"
    sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE ${tableName} (
            k1 INT NOT NULL,
            v1 VARCHAR(100),
            v2 INT
        ) UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "file_cache_ttl_seconds" = "3600",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    try {
        // Disable auto analyze to avoid internal loads affecting cache size
        sql """SET GLOBAL enable_auto_analyze = false"""

        // Clear file cache and wait for it to complete
        clearFileCacheOnAllBackends()
        sleep(3000)

        def initialSnapshot = getClusterCacheMetricsSnapshot()
        logger.info("Initial cache snapshot: ${initialSnapshot}")

        // First, do a successful load to establish baseline
        sql """
            INSERT INTO ${tableName}
            SELECT number, concat('test_', cast(number as string)), number
            FROM numbers("number"="10000")
        """
        sql """SELECT SUM(v2), SUM(LENGTH(v1)) FROM ${tableName}"""

        def afterSuccessfulLoadSnapshot = waitForCacheGrowth(initialSnapshot)
        logger.info("Cache snapshot after successful load: ${afterSuccessfulLoadSnapshot}")
        logger.info("Cache snapshot diff after successful load: ${getSnapshotDiff(initialSnapshot, afterSuccessfulLoadSnapshot)}")
        assertTrue(afterSuccessfulLoadSnapshot.total["file_cache_cache_size"] > initialSnapshot.total["file_cache_cache_size"]
                || afterSuccessfulLoadSnapshot.total["file_cache_ttl_cache_size"] > initialSnapshot.total["file_cache_ttl_cache_size"],
            "Cache should increase after successful load. Initial: ${initialSnapshot}, After: ${afterSuccessfulLoadSnapshot}")

        // Clear cache again to reset
        clearFileCacheOnAllBackends()
        sleep(3000)

        def afterClearSnapshot = getClusterCacheMetricsSnapshot()
        logger.info("Cache snapshot after clear: ${afterClearSnapshot}")

        // Enable debug point to make commit_rowset return error
        GetDebugPoint().enableDebugPointForAllBEs("LoadChannel.add_batch.failed")

        // Try to insert data - this should fail due to injection point
        boolean loadFailed = false
        try {
            sql """
                INSERT INTO ${tableName}
                SELECT number + 100000, concat('failed_', cast(number as string)), number
                FROM numbers("number"="10000")
            """
        } catch (Exception e) {
            loadFailed = true
            logger.info("Expected load failure occurred: ${e.message}")
        }
        assertTrue(loadFailed, "Insert should fail when debug point LoadChannel.add_batch.failed is enabled")

        def rowCountAfterFailedInsertRows = sql """SELECT COUNT(*) FROM ${tableName}"""
        long rowCountAfterFailedInsert = rowCountAfterFailedInsertRows[0][0].toString().toLong()
        logger.info("Row count after failed insert: rows=${rowCountAfterFailedInsertRows}, count=${rowCountAfterFailedInsert}")
        assertEquals(10000L, rowCountAfterFailedInsert)

        // Wait for cleanup to complete and cache metrics to update
        sleep(5000)

        def afterFailedLoadSnapshot = getClusterCacheMetricsSnapshot()
        logger.info("Cache snapshot after failed load: ${afterFailedLoadSnapshot}")
        def diffAfterFailedLoad = getSnapshotDiff(afterClearSnapshot, afterFailedLoadSnapshot)
        logger.info("Cache snapshot diff after failed load vs clear: ${diffAfterFailedLoad}")

        // Verify cache size has not materially increased.
        def allowedMetricGrowth = [
            "file_cache_cache_size"             : 65536L,
            "file_cache_index_queue_cache_size" : 65536L,
            "file_cache_ttl_cache_size"         : 65536L,
            "file_cache_normal_queue_cache_size": 65536L,
            "file_cache_disposable_queue_cache_size": 65536L
        ]
        allowedMetricGrowth.each { suffix, maxGrowth ->
            assertTrue(diffAfterFailedLoad[suffix] <= maxGrowth,
                "Metric ${suffix} should not materially increase after failed load. " +
                "BeforeClear=${afterClearSnapshot}, AfterFailed=${afterFailedLoadSnapshot}, " +
                "diff=${diffAfterFailedLoad[suffix]}, maxGrowth=${maxGrowth}")
        }

        logger.info("Test passed: File cache was properly cleared after load failure")
    } finally {
        sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
