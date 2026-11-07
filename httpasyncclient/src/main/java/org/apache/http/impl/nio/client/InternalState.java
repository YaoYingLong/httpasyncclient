/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */
package org.apache.http.impl.nio.client;

import java.nio.ByteBuffer;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

// 每次普通 execute 独享的协议状态，由 DefaultClientExchangeHandlerImpl 创建。
// MainClientExec 可被多个请求共享，本对象则随一个逻辑请求跨越多次 CONNECT/认证/重定向。
// 本类无独立同步机制，依赖 handler 的事件调用约定；不能当作并发共享的请求容器。
class InternalState {

    private final long id;
    private final HttpAsyncRequestProducer requestProducer;
    private final HttpAsyncResponseConsumer<?> responseConsumer;
    private final HttpClientContext localContext;

    // mainRequest 是当前业务请求（重定向后更新）；handler.currentRequest 还可能是 CONNECT。
    // finalResponse 非 null 才代表交付用户的响应；中间响应存在于 handler.currentResponse。
    private HttpRequestWrapper mainRequest;
    private HttpResponse finalResponse;
    private ByteBuffer tmpbuf;
    // 一旦进入过 producer.produceContent 即置 true，与 producer.isRepeatable 共同决定能否重发。
    // 此标记不因 resetRequest 清零；重置读指针不等于实体本身可重复读取。
    private boolean requestContentProduced;
    private int execCount;

    // redirectCount 与 execCount 分开：认证重发会增加发送次数，却不是一次重定向。
    // redirect 暂存响应头阶段决定的下一跳，等当前响应体消费完后才真正应用并清空。
    private int redirectCount;
    private HttpUriRequest redirect;

    public InternalState(
            final long id,
            final HttpAsyncRequestProducer requestProducer,
            final HttpAsyncResponseConsumer<?> responseConsumer,
            final HttpClientContext localContext) {
        super();
        this.id = id;
        this.requestProducer = requestProducer;
        this.responseConsumer = responseConsumer;
        this.localContext = localContext;
    }

    public long getId() {
        return id;
    }

    public HttpAsyncRequestProducer getRequestProducer() {
        return requestProducer;
    }

    public HttpAsyncResponseConsumer<?> getResponseConsumer() {
        return responseConsumer;
    }

    public HttpClientContext getLocalContext() {
        return localContext;
    }

    public HttpRequestWrapper getMainRequest() {
        return mainRequest;
    }

    public void setMainRequest(final HttpRequestWrapper mainRequest) {
        this.mainRequest = mainRequest;
    }

    public HttpResponse getFinalResponse() {
        return finalResponse;
    }

    public void setFinalResponse(final HttpResponse finalResponse) {
        this.finalResponse = finalResponse;
    }

    public ByteBuffer getTmpbuf() {
        if (tmpbuf == null) {
            tmpbuf = ByteBuffer.allocate(4 * 1024);
        }
        return tmpbuf;
    }

    public boolean isRequestContentProduced() {
        return requestContentProduced;
    }

    public void setRequestContentProduced() {
        this.requestContentProduced = true;
    }

    public int getExecCount() {
        return execCount;
    }

    public void incrementExecCount() {
        this.execCount++;
    }

    public int getRedirectCount() {
        return redirectCount;
    }

    public void incrementRedirectCount() {
        this.redirectCount++;
    }

    public HttpUriRequest getRedirect() {
        return redirect;
    }

    public void setRedirect(final HttpUriRequest redirect) {
        this.redirect = redirect;
    }

    @Override
    public String toString() {
        return Long.toString(id);
    }

}
