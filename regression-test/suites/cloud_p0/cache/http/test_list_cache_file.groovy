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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_list_cache_file") {
    def custoBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99
    ]

    setBeConfigTemporary(custoBeConfig) {

    String[][] backends = sql """ show backends """
    def backendSockets = []
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    assertTrue(backendIdToBackendIP.size() > 0, "No alive backends found")

    backendIdToBackendIP.each { backendId, ip ->
        def socket = ip + ":" + backendIdToBackendHttpPort.get(backendId)
        backendSockets.add(socket)
    }

    sql "drop table IF EXISTS `user`"

    sql """
        CREATE TABLE IF NOT EXISTS `user` (
            `id` int NULL,
            `name` string NULL
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "file_cache_ttl_seconds" = "2884"
        )
    """

    sql "insert into user select number, cast(rand() as varchar(32)) from numbers(\"number\"=\"1000000\")"
    sql "sync"
    sql "select count(*) from user"
    sql "select sum(length(name)) from user"

    Thread.sleep(10000)

    def get_tablets = { String tbl_name ->
        def res = sql "show tablets from ${tbl_name}"
        List<Long> tablets = new ArrayList<>()
        for (final def line in res) {
            tablets.add(Long.valueOf(line[0].toString()))
        }
        return tablets
    }

    def get_rowsets = { long tablet_id ->
        var ret = []
        httpTest {
            endpoint ""
            uri backendSockets[0] + "/api/compaction/show?tablet_id=" + tablet_id
            op "get"
            check {respCode, body ->
                assertEquals(respCode, 200)
                var map = parseJson(body)
                logger.info("compaction show result for tablet ${tablet_id}: ${map}")
                for (final def line in map.get("rowsets")) {
                    var tokens = line.toString().split(" ")
                    assertTrue(tokens.size() > 4, "Unexpected rowset format for tablet ${tablet_id}: ${line}")
                    ret.add(tokens[4])
                }
            }
        }
        return ret
    }

    var tablets = get_tablets("user")
    assertTrue(!tablets.isEmpty(), "No tablets found for table user")
    var rowsets = get_rowsets(tablets.get(0))
    assertTrue(!rowsets.isEmpty(), "No rowsets found for tablet ${tablets.get(0)}")
    var segment_file = rowsets[rowsets.size() - 1] + "_0.dat"
    logger.info("tablets=${tablets}, rowsets=${rowsets}, selected segment_file=${segment_file}")

    def clearResults = []

    def getListCacheEntries = { socket, cacheFile ->
        def arr = []
        httpTest {
            endpoint ""
            uri socket + "/api/file_cache?op=list_cache&value=" + cacheFile
            op "get"
            check {respCode, body ->
                assertEquals(respCode, 200)
                arr = parseJson(body)
            }
        }
        logger.info("list_cache socket=${socket}, cacheFile=${cacheFile}, result=${arr}")
        return arr
    }

    def waitUntilAnyBackendHasCacheFile = { cacheFile, long timeoutMs = 60000L, long intervalMs = 5000L ->
        def lastResults = [:]
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            boolean found = false
            backendSockets.each { socket ->
                def entries = getListCacheEntries(socket, cacheFile)
                lastResults[socket] = entries.size()
                if (entries.size() > 0) {
                    found = true
                }
            }
            logger.info("cache poll for ${cacheFile}: ${lastResults}")
            if (found) {
                return
            }
            sql "select sum(length(name)) from user"
            sleep(intervalMs)
        }
        assertTrue(false, "At least one backend should have cache file, segment_file=${cacheFile}, rowsets=${rowsets}, results=${lastResults}")
    }

    def waitUntilAllBackendsClearCacheFile = { cacheFile, long timeoutMs = 30000L, long intervalMs = 3000L ->
        def lastResults = [:]
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            boolean allCleared = true
            backendSockets.each { socket ->
                def entries = getListCacheEntries(socket, cacheFile)
                lastResults[socket] = entries.size()
                if (entries.size() > 0) {
                    allCleared = false
                }
            }
            logger.info("clear-cache poll for ${cacheFile}: ${lastResults}")
            if (allCleared) {
                return
            }
            sleep(intervalMs)
        }
        assertTrue(false, "Cache file should be cleared on all backends, segment_file=${cacheFile}, results=${lastResults}")
    }

    waitUntilAnyBackendHasCacheFile(segment_file)

    // Clear cache on all backends
    backendSockets.each { socket ->
        httpTest {
            endpoint ""
            uri socket + "/api/file_cache?op=clear&value=" + segment_file + "&sync=true"
            op "get"
            check {respCode, body ->
                assertEquals(respCode, 200, "clear local cache fail, maybe you can find something in respond: " + parseJson(body))
                logger.info("clear cache response socket=${socket}, cacheFile=${segment_file}, body=${body}")
                clearResults.add(true)
            }
        }
    }
    assertEquals(clearResults.size(), backendSockets.size(), "Failed to clear cache on some backends")

    waitUntilAllBackendsClearCacheFile(segment_file)
    }
}
