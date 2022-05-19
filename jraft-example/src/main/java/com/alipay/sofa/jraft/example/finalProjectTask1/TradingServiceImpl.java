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

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

public class TradingServiceImpl implements TradingService {
    private static final Logger LOG = LoggerFactory.getLogger(TradingServiceImpl.class);

    private final Server server;
    private final Executor      readIndexExecutor;

    public TradingServiceImpl(Server server) {
        this.server = server;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    private Executor createReadIndexExecutor() {
        final StoreEngineOptions opts = new StoreEngineOptions();
        return StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
    }

    private boolean isLeader() {
        return this.server.getFsm().isLeader();
    }

    private long getValue() {
        return this.server.getFsm().getValue();
    }

    private String getRedirect() {
        return this.server.redirect().getRedirect();
    }

    @Override
    public void createAccount(String accountID, int balance, TradingClosure closure) {
        applyOperation(TradingOperation.createCreate_Account(accountID, balance), closure);
    }

    @Override
    public void sendPayment(String fromAccountID, String toAccountID, int amount, TradingClosure closure) {
        applyOperation(TradingOperation.createSend_Payment(fromAccountID, toAccountID, amount), closure);
    }

    @Override
    public void queryAccount(String accountID, TradingClosure closure) {
        applyOperation(TradingOperation.createQuery_Account(accountID), closure);
    }

    private void applyOperation(final TradingOperation op, final TradingClosure closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        try {
            closure.setCounterOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            task.setDone(closure);
            this.server.getNode().apply(task);
        }
        catch (CodecException e) {
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final TradingClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }
}
