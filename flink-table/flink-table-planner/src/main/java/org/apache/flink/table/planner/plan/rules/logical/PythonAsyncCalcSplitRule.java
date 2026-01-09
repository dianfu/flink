/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import scala.Option;

/**
 * Rule to split Python async scalar functions from a Calc node into a separate AsyncCalc node. This
 * ensures Python async scalar functions are executed in a dedicated async operator.
 *
 * <p>Similar to {@link AsyncCalcSplitRule}, but specifically handles Python async scalar functions
 * by checking if the function is a {@link PythonFunction} and has async scalar function kind.
 */
public class PythonAsyncCalcSplitRule {

    private static final RemoteCallFinder PYTHON_ASYNC_CALL_FINDER =
            new PythonAsyncRemoteCallFinder();

    public static final RelOptRule SPLIT_CONDITION =
            new RemoteCalcSplitConditionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_PROJECT =
            new RemoteCalcSplitProjectionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_PROJECTION_REX_FIELD =
            new RemoteCalcSplitProjectionRexFieldRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_CONDITION_REX_FIELD =
            new RemoteCalcSplitConditionRexFieldRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule EXPAND_PROJECT =
            new RemoteCalcExpandProjectRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule PUSH_CONDITION =
            new RemoteCalcPushConditionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule REWRITE_PROJECT =
            new RemoteCalcRewriteProjectionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule NESTED_SPLIT =
            new PythonAsyncCalcSplitNestedRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule ONE_PER_CALC_SPLIT =
            new PythonAsyncCalcSplitOnePerCalcRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule NO_ASYNC_JOIN_CONDITIONS =
            new SplitRemoteConditionFromJoinRule(
                    PYTHON_ASYNC_CALL_FINDER,
                    JavaScalaConversionUtil.toScala(
                            Optional.of(
                                    "Python AsyncScalarFunction not supported for non inner join condition")));

    /**
     * Finder for Python async scalar functions. Extends AsyncRemoteCallFinder to also check if the
     * function is a Python function.
     */
    private static class PythonAsyncRemoteCallFinder extends AsyncUtil.AsyncRemoteCallFinder {

        public PythonAsyncRemoteCallFinder() {
            super(FunctionKind.ASYNC_SCALAR);
        }

        @Override
        public boolean containsRemoteCall(RexNode node) {
            if (!super.containsRemoteCall(node)) {
                return false;
            }

            // Additional check: must be a Python function
            if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                if (call.getOperator() instanceof BridgingSqlFunction) {
                    BridgingSqlFunction function = (BridgingSqlFunction) call.getOperator();
                    Object definition = ShortcutUtils.unwrapFunctionDefinition(call);
                    if (definition instanceof PythonFunction) {
                        return function.getDefinition().getKind() == FunctionKind.ASYNC_SCALAR;
                    }
                }
            }
            return false;
        }
    }

    private static boolean hasNestedCalls(List<RexNode> projects, RemoteCallFinder callFinder) {
        return projects.stream()
                .filter(callFinder::containsRemoteCall)
                .filter(expr -> expr instanceof RexCall)
                .map(expr -> (RexCall) expr)
                .anyMatch(
                        rexCall ->
                                rexCall.getOperands().stream()
                                        .anyMatch(callFinder::containsRemoteCall));
    }

    /**
     * Splits nested call <- asyncCall chains so that nothing is immediately waiting on an async
     * Python call in a single calc.
     *
     * <p>For Example: Calc(select=[syncCall(asyncPythonCall()]) -> Source
     *
     * <p>becomes
     *
     * <p>Calc(select=[syncCall(f0)]) -> AsyncCalc(select=[asyncPythonCall() as f0]) -> Source
     */
    public static class PythonAsyncCalcSplitNestedRule extends RemoteCalcSplitRuleBase<Void> {

        public PythonAsyncCalcSplitNestedRule(RemoteCallFinder callFinder) {
            super("PythonAsyncCalcSplitNestedRule", callFinder);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            FlinkLogicalCalc calc = call.rel(0);

            // Matches if we have nested Python async calls
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(calc.getProgram()::expandLocalRef)
                            .collect(Collectors.toList());
            return hasNestedCalls(projects, callFinder());
        }

        // We convert not on the outermost call, but anything within it
        @Override
        public boolean needConvert(RexProgram program, RexNode node, Option<Void> matchState) {
            return node instanceof RexCall
                    && !((RexCall) node)
                            .getOperands().stream().anyMatch(callFinder()::containsRemoteCall);
        }

        @Override
        public SplitComponents split(RexProgram program, ScalarFunctionSplitter splitter) {
            return new SplitComponents(
                    JavaScalaConversionUtil.toScala(Optional.<RexNode>empty()),
                    JavaScalaConversionUtil.toScala(
                            Optional.ofNullable(program.getCondition())
                                    .map(program::expandLocalRef)),
                    JavaScalaConversionUtil.toScala(
                            program.getProjectList().stream()
                                    .map(program::expandLocalRef)
                                    .map(n -> n.accept(splitter))
                                    .collect(Collectors.toList())));
        }
    }

    /**
     * Splits Python async calls if there are multiple across projections, so that there's one per
     * calc. This assumes that the nested rule has been run first, so there is just one per
     * projection.
     *
     * <p>For Example: Calc(select=[asyncPythonCall(), asyncPythonCall()]) -> Source
     *
     * <p>becomes
     *
     * <p>AsyncCalc(select=[asyncPythonCall(), f0]) -> AsyncCalc(select=[asyncPythonCall() as f0])
     * -> Source
     */
    public static class PythonAsyncCalcSplitOnePerCalcRule
            extends RemoteCalcSplitProjectionRuleBase<PythonAsyncCalcSplitOnePerCalcRule.State> {

        public PythonAsyncCalcSplitOnePerCalcRule(RemoteCallFinder callFinder) {
            super("PythonAsyncCalcSplitOnePerCalcRule", callFinder);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            FlinkLogicalCalc calc = call.rel(0);
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(calc.getProgram()::expandLocalRef)
                            .collect(Collectors.toList());

            // If this has no nested calls, then this can be called to split up separate projections
            // into two different calcs. We don't want the splitter to be called with nested
            // calls since it won't behave correctly, so this must be used in conjunction with the
            // nested rule.
            return !hasNestedCalls(projects, callFinder())
                    && projects.stream().filter(callFinder()::containsRemoteCall).count() >= 2;
        }

        @Override
        public boolean needConvert(RexProgram program, RexNode node, Option<State> matchState) {
            if (callFinder().containsRemoteCall(node) && !matchState.get().foundMatch) {
                matchState.get().foundMatch = true;
                return true;
            }
            return false;
        }

        @Override
        public Option<State> getMatchState() {
            return Option.apply(new State());
        }

        /** State object used to keep track of whether a match has been found yet. */
        public static class State {
            boolean foundMatch = false;
        }
    }
}
