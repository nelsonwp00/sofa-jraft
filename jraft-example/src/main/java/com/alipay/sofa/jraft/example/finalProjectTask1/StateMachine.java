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
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.example.finalProjectTask1.storage.SnapshotFile;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.sofa.jraft.example.finalProjectTask1.TradingOperation.*;

public class StateMachine extends StateMachineAdapter {

    private static final Logger LOG        = LoggerFactory.getLogger(StateMachine.class);

    private final AtomicLong    value      = new AtomicLong(0);

    private final AtomicLong    leaderTerm = new AtomicLong(-1);

    private final Map<String, Integer> accounts = new ConcurrentHashMap<String, Integer>();

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    /**
     * Returns current value.
     */
    public long getValue() {
        return this.value.get();
    }

    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            int newBalance = 0;
            TradingOperation tradingOperation = null;
            String errorMsg = null;

            TradingClosure closure = null;
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (TradingClosure) iter.done();
                tradingOperation = closure.getCounterOperation();
            }
            else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    tradingOperation = SerializerManager.getSerializer(SerializerManager.Hessian2)
                            .deserialize(data.array(), TradingOperation.class.getName());
                } catch (final CodecException e) {
                    LOG.error("Fail to decode IncrementAndGetRequest", e);
                }
                // follower ignore read operation
                if (tradingOperation != null && tradingOperation.isReadOp()) {
                    iter.next();
                    continue;
                }
            }
            if (tradingOperation != null) {
                switch (tradingOperation.getOp()) {
                    case CREATE_ACCOUNT:
                        final String accountID = tradingOperation.getFromAccountID();
                        newBalance = tradingOperation.getAmount();

                        boolean accountExist = accounts.getOrDefault(accountID, null) != null;

                        if (accountExist) {
                            LOG.info("CREATE_ACCOUNT: Account ID = {} already exists, logIndex = {}", accountID, iter.getIndex());
                            errorMsg = "Account " + accountID + " already exists";
                        }
                        else {
                            accounts.put(accountID, newBalance);
                            LOG.info("CREATE_ACCOUNT: Create Account ID = {}, balance = ${}, logIndex = {}", accountID, newBalance, iter.getIndex());
                        }
                        break;

                    case SEND_PAYMENT:
                        final String fromAccountID = tradingOperation.getFromAccountID();
                        final String toAccountID = tradingOperation.getToAccountID();
                        final int payment = tradingOperation.getAmount();

                        boolean fromAccountExist = accounts.getOrDefault(fromAccountID, null) != null;
                        boolean toAccountExist = accounts.getOrDefault(toAccountID, null) != null;

                        if (fromAccountExist && toAccountExist) {
                            int fromAccountBalance = accounts.get(fromAccountID);
                            if (fromAccountBalance < payment) {
                                errorMsg = "Account does not have enough balance : $" + fromAccountBalance;
                                LOG.info("SEND_PAYMENT: " + errorMsg);
                                break;
                            }

                            accounts.computeIfPresent(fromAccountID, (key, balance) -> balance - payment);
                            accounts.computeIfPresent(toAccountID, (key, balance) -> balance + payment);

                            newBalance = fromAccountBalance - payment;

                            LOG.info("SEND_PAYMENT: Acount {} transfer ${} to Account {}", fromAccountID, toAccountID, payment);
                            printAccounts();
                        }
                        else {
                            errorMsg = "Account does not exist.";
                            LOG.info("SEND_PAYMENT: From Account Exist : {}, To Account Exist : {}", fromAccountExist, toAccountExist);
                        }
                        break;

                    case QUERY_ACCOUNT:
                        final String queryAccountID = tradingOperation.getFromAccountID();
                        boolean queryAccountExist = accounts.getOrDefault(queryAccountID, null) != null;

                        if (queryAccountExist) {
                            int queryAccountBalance = accounts.get(queryAccountID);
                            newBalance = queryAccountBalance;
                            LOG.info("QUERY_ACCOUNT: Account ID = {}. Balance = ${}.", queryAccountID, queryAccountBalance);
                        }
                        else {
                            errorMsg = "Account does not exist.";
                            LOG.info("QUERY_ACCOUNT: " + errorMsg);
                        }
                        break;
                }

                if (closure != null) {
                    if (errorMsg == null) {
                        closure.success(newBalance);
                        closure.run(Status.OK());
                    }
                    else {
                        closure.failure(errorMsg, StringUtils.EMPTY);
                        closure.run(new Status(RaftError.EINTERNAL, errorMsg));
                    }
                }
            }
            iter.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        Utils.runInThread(() -> {
            final SnapshotFile snapshot = new SnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(accounts)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e, e);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final SnapshotFile snapshot = new SnapshotFile(reader.getPath() + File.separator + "data");
        try {
            Map<String, Integer> savedAccounts = snapshot.load();
            accounts.putAll(savedAccounts);

            return true;
        } catch (final IOException | ClassNotFoundException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

    private void printAccounts() {
        for (Map.Entry<String, Integer> entry : accounts.entrySet()) {
            LOG.info(entry.getKey() + ":" + entry.getValue());
        }
    }

}
