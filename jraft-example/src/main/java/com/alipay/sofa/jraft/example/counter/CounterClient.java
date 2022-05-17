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

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.example.counter.CounterOutter.SetAndGetRequest;
import com.alipay.sofa.jraft.example.counter.CounterOutter.GetValueRequest;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class CounterClient {

    public static void main(final String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Usage : java com.alipay.sofa.jraft.example.counter.CounterClient {groupId} {conf} {action} {value} {intervals}");
            System.out.println("Example: java com.alipay.sofa.jraft.example.counter.CounterClient counter 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083 + 1 10");
            System.exit(1);
        }
        final String groupId = args[0];
        final String confStr = args[1];
        final String action = args[2];
        final int value = Integer.parseInt(args[3]);
        final int interval = Integer.parseInt(args[4]);

        System.out.println("sending request to RAFT Cluster : counter " + args[2] + " " + args[3] + " by " + args[4] + " times");

        CounterGrpcHelper.initGRpc();

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        final int n = interval + 2;
        final CountDownLatch latch = new CountDownLatch(n);
        System.out.println("Before start, checking current counter value ... \n");
        get(cliClientService, leader, latch);

        final long start = System.currentTimeMillis();

        if (action.equals("+")) {
            for (int i = 0; i < n; i++)
                incrementAndGet(cliClientService, leader, value, latch);
        }
        else if (action.equals("-")) {
            for (int i = 0; i < n; i++)
                decrementAndGet(cliClientService, leader, value, latch);
        }

        latch.await();
        System.out.println("Completed operations : " + n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.\n");

        System.exit(0);
    }

    private static void incrementAndGet(final CliClientServiceImpl cliClientService, final PeerId leader, final long delta, CountDownLatch latch) throws RemotingException, InterruptedException {
        SetAndGetRequest request = SetAndGetRequest.newBuilder().setDelta(delta).build();
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("operation : incrementAndGet, current counter value = " + result);
                }
                else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }
            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    private static void decrementAndGet(final CliClientServiceImpl cliClientService, final PeerId leader, final long delta, CountDownLatch latch) throws RemotingException, InterruptedException {
        SetAndGetRequest request = SetAndGetRequest.newBuilder().setDelta(-delta).build();
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("operation : decrementAndGet, current counter value = " + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }
            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    private static void get(final CliClientServiceImpl cliClientService, final PeerId leader, CountDownLatch latch) throws RemotingException, InterruptedException
    {
        GetValueRequest request = GetValueRequest.newBuilder().setReadOnlySafe(true).build();

        InvokeCallback callback = new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("operation : get,  current counter value = " + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        };

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, callback, 5000);
    }
}
