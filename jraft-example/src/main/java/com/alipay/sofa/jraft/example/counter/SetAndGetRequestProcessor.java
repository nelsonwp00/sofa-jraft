/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.example.counter;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.example.counter.CounterOutter.SetAndGetRequest;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

public class SetAndGetRequestProcessor implements RpcProcessor<SetAndGetRequest> {

    private final CounterService counterService;

    public SetAndGetRequestProcessor(CounterService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final SetAndGetRequest request) {
        final CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.counterService.setAndGet(request.getDelta(), closure);
    }

    @Override
    public String interest() {
        return SetAndGetRequest.class.getName();
    }
}
