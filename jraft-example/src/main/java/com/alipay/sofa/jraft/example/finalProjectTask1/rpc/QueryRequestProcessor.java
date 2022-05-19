package com.alipay.sofa.jraft.example.finalProjectTask1.rpc;

import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.example.finalProjectTask1.TradingClosure;
import com.alipay.sofa.jraft.example.finalProjectTask1.TradingService;
import com.alipay.sofa.jraft.example.finalProjectTask1.rpc.TradingOutter.QueryRequest;
import com.alipay.sofa.jraft.rpc.RpcContext;

public class QueryRequestProcessor implements RpcProcessor<QueryRequest> {
    private final TradingService tradingService;

    public QueryRequestProcessor(TradingService tradingService) {
        super();
        this.tradingService = tradingService;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final QueryRequest request) {
        final TradingClosure closure = new TradingClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.tradingService.queryAccount(
                request.getAccountID(),
                closure
        );
    }

    @Override
    public String interest() {
        return QueryRequest.class.getName();
    }
}
