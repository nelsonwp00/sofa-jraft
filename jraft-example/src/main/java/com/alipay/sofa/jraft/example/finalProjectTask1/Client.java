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
package com.alipay.sofa.jraft.example.finalProjectTask1;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.example.finalProjectTask1.rpc.GrpcHelper;
import com.alipay.sofa.jraft.example.finalProjectTask1.rpc.TradingOutter.CreateAccountRequest;
import com.alipay.sofa.jraft.example.finalProjectTask1.rpc.TradingOutter.SendPaymentRequest;
import com.alipay.sofa.jraft.example.finalProjectTask1.rpc.TradingOutter.QueryRequest;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class Client {
    private static int operationTimeout = 5000;
    public static void main(final String[] args) throws Exception {
        System.out.println("CreateAccount Usage : provide args {GroupId} {Conf} {Action} {AccountID} {Balance}");
        System.out.println("SendPayment Usage : provide args {GroupId} {Conf} {Action} {From AccountID} {To AccountID} {Balance}");
        System.out.println("QueryAccount Usage : provide args {GroupId} {Conf} {Action} {AccountID}");

        final String groupId = args[0];
        final String confStr = args[1];
        final String action = args[2];
        String fromAccount = null;
        String toAccount = null;
        int amount = 0;
        System.out.println("Action : " + action);

        if (action.equals("CreateAccount") && args.length == 5) {
            fromAccount = args[3];
            amount = Integer.parseInt(args[4]);
            assert (amount >= 0);
        }
        else if (action.equals("SendPayment") && args.length == 5) {
            fromAccount = args[3];
            toAccount = args[4];
            amount = Integer.parseInt(args[5]);
            assert (amount > 0);
        }
        else if (action.equals("QueryAccount") && args.length == 4) {
            fromAccount = args[3];
        }
        else {
            System.out.println("Action does not have enough args.");
            System.exit(0);
        }

        //System.out.println("sending request to RAFT Cluster : counter " + args[2] + " " + args[3] + " by " + args[4] + " times");

        GrpcHelper.initGRpc();

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

        final int n = 300;
        final CountDownLatch latch = new CountDownLatch(n);

        System.out.println("Leader is " + leader + ". Number of Operation = " + n);

        final long start = System.currentTimeMillis();

        //createAccount(cliClientService, leader, latch, fromAccount, amount);
        //queryAccount(cliClientService, leader, latch, fromAccount);
        for (int i = 0; i < n; i++) {
            sendPayment(cliClientService, leader, latch, "acc2", "acc1", 1);
        }

        latch.await();
        System.out.println("Completed operations : " + n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.\n");
    }

    private static void createAccount(
            final CliClientServiceImpl cliClientService,
            final PeerId leader,
            CountDownLatch latch,
            final String accountID,
            final int balance) throws RemotingException, InterruptedException
    {
        CreateAccountRequest request = CreateAccountRequest.newBuilder()
                .setAccountID(accountID)
                .setBalance(balance)
                .build();

        InvokeCallback callback = new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("Operation : Create Account, " + result);
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

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, callback, operationTimeout);
    }

    private static void sendPayment(
            final CliClientServiceImpl cliClientService,
            final PeerId leader,
            CountDownLatch latch,
            final String fromAccountID,
            final String toAccountID,
            final int amount) throws RemotingException, InterruptedException
    {

        SendPaymentRequest request = SendPaymentRequest.newBuilder()
                .setFromAccountID(fromAccountID)
                .setToAccountID(toAccountID)
                .setAmount(amount)
                .build();

        InvokeCallback callback = new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("Operation : Send Payment. Server returns " + result);
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

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, callback, operationTimeout);
    }

    private static void queryAccount(
            final CliClientServiceImpl cliClientService,
            final PeerId leader,
            CountDownLatch latch,
            final String accountID) throws RemotingException, InterruptedException
    {
        QueryRequest request = QueryRequest.newBuilder()
                .setAccountID(accountID)
                .build();

        InvokeCallback callback = new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("Operation : Query Account. Server returns " + result);
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

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, callback, operationTimeout);
    }
}
