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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_pushback'.
 */
public class ArrayPushBack extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNullable {

    public static final List<FunctionSignature> FOLLOW_DATATYPE_SIGNATURE = ImmutableList.of(
            FunctionSignature.retArgType(0)
                    .args(ArrayType.of(new AnyDataType(0)), new FollowToAnyDataType(0))
    );

    public static final List<FunctionSignature> MIN_COMMON_TYPE_SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0)
                    .args(ArrayType.of(new AnyDataType(0)), new AnyDataType(0))
    );

    /**
     * constructor with 1 argument.
     */
    public ArrayPushBack(Expression arg0, Expression arg1) {
        super("array_pushback", arg0, arg1);
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayPushBack withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ArrayPushBack(children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayPushBack(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (getArgument(0).getDataType().isArrayType()
                &&
                ((ArrayType) getArgument(0).getDataType()).getItemType()
                        .isSameTypeForComplexTypeParam(getArgument(1).getDataType())) {
            // return least common type
            return MIN_COMMON_TYPE_SIGNATURES;
        }
        return FOLLOW_DATATYPE_SIGNATURE;
    }
}
