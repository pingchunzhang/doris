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

#pragma once

#include "operator.h"
#include "vec/sink/writer/vtablet_writer.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class OlapTableSinkOperatorX;

class OlapTableSinkLocalState final
        : public AsyncWriterSink<vectorized::VTabletWriter, OlapTableSinkOperatorX> {
public:
    using Base = AsyncWriterSink<vectorized::VTabletWriter, OlapTableSinkOperatorX>;
    using Parent = OlapTableSinkOperatorX;
    ENABLE_FACTORY_CREATOR(OlapTableSinkLocalState);
    OlapTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {};
    friend class OlapTableSinkOperatorX;
};
class OlapTableSinkOperatorX final : public DataSinkOperatorX<OlapTableSinkLocalState> {
public:
    using Base = DataSinkOperatorX<OlapTableSinkLocalState>;
    OlapTableSinkOperatorX(ObjectPool* pool, int operator_id, const RowDescriptor& row_desc,
                           const std::vector<TExpr>& t_output_expr)
            : Base(operator_id, 0, 0),
              _row_desc(row_desc),
              _t_output_expr(t_output_expr),
              _pool(pool) {};

    Status init(const TDataSink& thrift_sink) override {
        RETURN_IF_ERROR(Base::init(thrift_sink));
        // From the thrift expressions create the real exprs.
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Base::prepare(state));
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
        return vectorized::VExpr::open(_output_vexpr_ctxs, state);
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override {
        auto& local_state = get_local_state(state);
        SCOPED_TIMER(local_state.exec_time_counter());
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        return local_state.sink(state, in_block, eos);
    }

private:
    friend class OlapTableSinkLocalState;
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;
    const RowDescriptor& _row_desc;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
    ObjectPool* _pool = nullptr;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
