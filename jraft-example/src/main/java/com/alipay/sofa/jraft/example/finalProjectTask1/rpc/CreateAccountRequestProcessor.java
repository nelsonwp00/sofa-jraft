package com.alipay.sofa.jraft.example.finalProjectTask1.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.example.finalProjectTask1.TradingClosure;
import com.alipay.sofa.jraft.example.finalProjectTask1.TradingService;
import com.alipay.sofa.jraft.example.finalProjectTask1.rpc.TradingOutter.CreateAccountRequest;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

public class CreateAccountRequestProcessor implements RpcProcessor<CreateAccountRequest> {
    private final TradingService tradingService;

    public CreateAccountRequestProcessor(TradingService tradingService) {
        super();
        this.tradingService = tradingService;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final CreateAccountRequest request) {
        final TradingClosure closure = new TradingClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.tradingService.createAccount(
                request.getAccountID(),
                request.getBalance(),
                closure
        );
    }

    @Override
    public String interest() {
        return CreateAccountRequest.class.getName();
    }
}
