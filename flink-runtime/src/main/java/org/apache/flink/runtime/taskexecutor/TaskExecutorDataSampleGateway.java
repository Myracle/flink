/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.sampling.DataSampleRequest;
import org.apache.flink.runtime.sampling.TaskDataSampleResponse;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** RPC gateway for requesting data samples from tasks. */
@Internal
public interface TaskExecutorDataSampleGateway {

    /**
     * Request data samples from the given tasks.
     *
     * @param taskExecutionAttemptIds identifying the tasks to sample
     * @param request parameters of the sampling request
     * @param timeout of the request
     * @return Future of data sample response
     */
    CompletableFuture<TaskDataSampleResponse> requestDataSamples(
            Collection<ExecutionAttemptID> taskExecutionAttemptIds,
            DataSampleRequest request,
            @RpcTimeout Duration timeout);
}
