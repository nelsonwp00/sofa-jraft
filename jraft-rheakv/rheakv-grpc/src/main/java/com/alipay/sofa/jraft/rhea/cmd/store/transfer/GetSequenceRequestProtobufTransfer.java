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
package com.alipay.sofa.jraft.rhea.cmd.store.transfer;

import com.alipay.sofa.jraft.rhea.cmd.store.GetSequenceRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class GetSequenceRequestProtobufTransfer
                                               implements
                                               GrpcSerializationTransfer<GetSequenceRequest, RheakvRpc.GetSequenceRequest> {

    @Override
    public GetSequenceRequest protoBufTransJavaBean(RheakvRpc.GetSequenceRequest getSequenceRequest) {
        final GetSequenceRequest request = new GetSequenceRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, getSequenceRequest.getBaseRequest());
        request.setSeqKey(getSequenceRequest.getSeqKey().toByteArray());
        request.setStep(getSequenceRequest.getStep());
        return request;
    }

    @Override
    public RheakvRpc.GetSequenceRequest javaBeanTransProtobufBean(GetSequenceRequest getSequenceRequest) {
        return RheakvRpc.GetSequenceRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(getSequenceRequest))
            .setSeqKey(ByteString.copyFrom(getSequenceRequest.getSeqKey())).setStep(getSequenceRequest.getStep())
            .build();
    }
}
