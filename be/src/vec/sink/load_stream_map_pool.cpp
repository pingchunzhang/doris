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

#include "vec/sink/load_stream_map_pool.h"

#include "util/debug_points.h"

namespace doris {
#include "common/compile_check_begin.h"
class TExpr;

LoadStreamMap::LoadStreamMap(UniqueId load_id, int64_t src_id, int num_streams, int num_use,
                             LoadStreamMapPool* pool)
        : _load_id(load_id),
          _src_id(src_id),
          _num_streams(num_streams),
          _use_cnt(num_use),
          _pool(pool),
          _tablet_schema_for_index(std::make_shared<IndexToTabletSchema>()),
          _enable_unique_mow_for_index(std::make_shared<IndexToEnableMoW>()) {
    DCHECK(num_streams > 0) << "stream num should be greater than 0";
    DCHECK(num_use > 0) << "use num should be greater than 0";
}

std::shared_ptr<LoadStreamStubs> LoadStreamMap::get_or_create(int64_t dst_id, bool incremental) {
    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<LoadStreamStubs> streams = _streams_for_node[dst_id];
    if (streams != nullptr) {
        return streams;
    }
    streams = std::make_shared<LoadStreamStubs>(_num_streams, _load_id, _src_id,
                                                _tablet_schema_for_index,
                                                _enable_unique_mow_for_index, incremental);
    _streams_for_node[dst_id] = streams;
    return streams;
}

std::shared_ptr<LoadStreamStubs> LoadStreamMap::at(int64_t dst_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _streams_for_node.at(dst_id);
}

bool LoadStreamMap::contains(int64_t dst_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _streams_for_node.contains(dst_id);
}

void LoadStreamMap::for_each(std::function<void(int64_t, LoadStreamStubs&)> fn) {
    decltype(_streams_for_node) snapshot;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        snapshot = _streams_for_node;
    }
    for (auto& [dst_id, streams] : snapshot) {
        fn(dst_id, *streams);
    }
}

Status LoadStreamMap::for_each_st(std::function<Status(int64_t, LoadStreamStubs&)> fn) {
    decltype(_streams_for_node) snapshot;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        snapshot = _streams_for_node;
    }
    Status status = Status::OK();
    for (auto& [dst_id, streams] : snapshot) {
        auto st = fn(dst_id, *streams);
        if (!st.ok() && status.ok()) {
            status = st;
        }
    }
    return status;
}

void LoadStreamMap::save_tablets_to_commit(int64_t dst_id,
                                           const std::vector<PTabletID>& tablets_to_commit) {
    std::lock_guard<std::mutex> lock(_tablets_to_commit_mutex);
    auto& tablets = _tablets_to_commit[dst_id];
    for (const auto& tablet : tablets_to_commit) {
        tablets.emplace(tablet.tablet_id(), tablet);
    }
}

bool LoadStreamMap::release() {
    int num_use = --_use_cnt;
    if (num_use == 0) {
        LOG(INFO) << "releasing streams, load_id=" << _load_id;
        _pool->erase(_load_id);
        return true;
    }
    LOG(INFO) << "keeping streams, load_id=" << _load_id << ", use_cnt=" << num_use;
    return false;
}

void LoadStreamMap::close_load(bool incremental) {
    for (auto& [dst_id, streams] : _streams_for_node) {
        if (streams->is_incremental() != incremental) {
            continue;
        }
        std::vector<PTabletID> tablets_to_commit;
        const auto& tablets = _tablets_to_commit[dst_id];
        tablets_to_commit.reserve(tablets.size());
        for (const auto& [tablet_id, tablet] : tablets) {
            tablets_to_commit.push_back(tablet);
            tablets_to_commit.back().set_num_segments(_segments_for_tablet[tablet_id]);
        }
        auto st = streams->close_load(tablets_to_commit);
        if (!st.ok()) {
            LOG(WARNING) << "close_load for " << (incremental ? "incremental" : "non-incremental")
                         << " streams failed: " << st << ", load_id=" << _load_id;
        }
    }
}

LoadStreamMapPool::LoadStreamMapPool() = default;

LoadStreamMapPool::~LoadStreamMapPool() = default;
std::shared_ptr<LoadStreamMap> LoadStreamMapPool::get_or_create(UniqueId load_id, int64_t src_id,
                                                                int num_streams, int num_use) {
    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<LoadStreamMap> streams = _pool[load_id];
    if (streams != nullptr) {
        return streams;
    }
    streams = std::make_shared<LoadStreamMap>(load_id, src_id, num_streams, num_use, this);
    _pool[load_id] = streams;
    return streams;
}

void LoadStreamMapPool::erase(UniqueId load_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    _pool.erase(load_id);
}

} // namespace doris
