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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.Configurable;
import org.apache.http.client.methods.HttpExecutionAware;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.BasicFuture;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;

/**
 * Default implementation of {@link org.apache.http.nio.protocol.HttpAsyncClientExchangeHandler}.
 * <p>
 * Instances of this class are expected to be accessed by one thread at a time only.
 * The {@link #cancel()} method can be called concurrently by multiple threads.
 */
class MinimalClientExchangeHandlerImpl<T> extends AbstractClientExchangeHandler {

    // 一次普通请求持有固定的 producer/consumer；与 pipeline 的整批队列不同，
    // 原始请求在 start 中生成，发送阶段 generateRequest 只返回准备好的请求包装器。

    private final HttpAsyncRequestProducer requestProducer;
    private final HttpAsyncResponseConsumer<T> responseConsumer;
    private final HttpClientContext localContext;
    private final BasicFuture<T> resultFuture;
    private final HttpProcessor httpProcessor;

    public MinimalClientExchangeHandlerImpl(
            final Log log,
            final HttpAsyncRequestProducer requestProducer,
            final HttpAsyncResponseConsumer<T> responseConsumer,
            final HttpClientContext localContext,
            final BasicFuture<T> resultFuture,
            final NHttpClientConnectionManager connmgr,
            final HttpProcessor httpProcessor,
            final ConnectionReuseStrategy connReuseStrategy,
            final ConnectionKeepAliveStrategy keepaliveStrategy) {
        super(log, localContext, connmgr, connReuseStrategy, keepaliveStrategy);
        this.requestProducer = requestProducer;
        this.responseConsumer = responseConsumer;
        this.localContext = localContext;
        this.resultFuture = resultFuture;
        this.httpProcessor = httpProcessor;
    }

    @Override
    void releaseResources() {
        try {
            this.requestProducer.close();
        } catch (final IOException ex) {
            this.log.debug("I/O error closing request producer", ex);
        }
        try {
            this.responseConsumer.close();
        } catch (final IOException ex) {
            this.log.debug("I/O error closing response consumer", ex);
        }
    }

    @Override
    void executionFailed(final Exception ex) {
        // 此实现只通知 producer/consumer；与 Default handler 不同，这个方法没有
        // resultFuture.failed(ex)。阅读失败完成路径时需以该实现为准，不能套用 Default 的逻辑。
        this.requestProducer.failed(ex);
        this.responseConsumer.failed(ex);
    }

    @Override
    boolean executionCancelled() {
        // consumer.cancel 的布尔值与结果 Future 的最终状态不是同一件事：
        // consumer 已有异常则发布失败，已有结果则发布成功，否则才发布取消。
        final boolean cancelled = this.responseConsumer.cancel();

        final T result = this.responseConsumer.getResult();
        final Exception ex = this.responseConsumer.getException();
        if (ex != null) {
            this.resultFuture.failed(ex);
        } else if (result != null) {
            this.resultFuture.completed(result);
        } else {
            this.resultFuture.cancel();
        }
        return cancelled;
    }

    public void start() throws HttpException, IOException {
        // 由 MinimalHttpAsyncClient.execute 同步调用。target 来自 producer，原始 request
        // 也在租借连接之前产生；这里拿不到本次发送实际使用的连接远端。
        final HttpHost target = this.requestProducer.getTarget();
        final HttpRequest original = this.requestProducer.generateRequest();

        if (original instanceof HttpExecutionAware) {
            // 将 HttpUriRequest.abort 等请求级取消连接到同一个 handler，和 Future.cancel
            // 共用后续取消/资源清理路径；不是另建一个只取消 Future 的标记。
            ((HttpExecutionAware) original).setCancellable(this);
        }
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] start execution");
        }

        if (original instanceof Configurable) {
            final RequestConfig config = ((Configurable) original).getConfig();
            if (config != null) {
                this.localContext.setRequestConfig(config);
            }
        }

        final HttpRequestWrapper request = HttpRequestWrapper.wrap(original);
        // minimal 直接以 target 创建路由，不调用完整客户端的 HttpRoutePlanner。
        // 请求配置、上下文和协议拦截器在 start 中准备完成后才开始申请连接。
        final HttpRoute route = new HttpRoute(target);
        setCurrentRequest(request);
        setRoute(route);

        this.localContext.setAttribute(HttpCoreContext.HTTP_REQUEST, request);
        this.localContext.setAttribute(HttpCoreContext.HTTP_TARGET_HOST, target);
        this.localContext.setAttribute(HttpClientContext.HTTP_ROUTE, route);

        this.httpProcessor.process(request, this.localContext);

        requestConnection();
    }

    @Override
    public HttpRequest generateRequest() throws IOException, HttpException {
        // 这是 HttpCore 发送阶段的 handler 入口，连接此时已分配给该 exchange。
        // 如果只在 DefaultClientExchangeHandlerImpl 上扩展，此普通 minimal 路径不会被覆盖。
        verifytRoute();
        if (!isRouteEstablished()) {
            onRouteToTarget();
            onRouteComplete();
        }

        final NHttpClientConnection localConn = getConnection();
        this.localContext.setAttribute(HttpCoreContext.HTTP_CONNECTION, localConn);
        final RequestConfig config = this.localContext.getRequestConfig();
        if (config.getSocketTimeout() > 0) {
            localConn.setSocketTimeout(config.getSocketTimeout());
        }
        return getCurrentRequest();
    }

    @Override
    public void produceContent(
            final ContentEncoder encoder, final IOControl ioControl) throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] produce content");
        }
        this.requestProducer.produceContent(encoder, ioControl);
        if (encoder.isCompleted()) {
            this.requestProducer.resetRequest();
        }
    }

    @Override
    public void requestCompleted() {
        // 输出完成通知直接调用 producer，尚未等待响应；context 仍是本次执行的可变上下文。
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Request completed");
        }
        this.requestProducer.requestCompleted(this.localContext);
    }

    @Override
    public void responseReceived(
            final HttpResponse response) throws IOException, HttpException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Response received " + response.getStatusLine());
        }
        this.localContext.setAttribute(HttpCoreContext.HTTP_RESPONSE, response);
        this.httpProcessor.process(response, this.localContext);

        setCurrentResponse(response);

        this.responseConsumer.responseReceived(response);
    }

    @Override
    public void consumeContent(
            final ContentDecoder decoder, final IOControl ioControl) throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Consume content");
        }
        this.responseConsumer.consumeContent(decoder, ioControl);
        if (!decoder.isCompleted() && this.responseConsumer.isDone()) {
            // consumer 提前结束而响应体未读完，连接不能回池复用，否则残留字节会污染下次响应。
            // 此分支取消结果并走统一 close，而不是把“consumer 已完成”直接当成请求成功。
            markConnectionNonReusable();
            try {
                markCompleted();
                releaseConnection();
                this.resultFuture.cancel();
            } finally {
                close();
            }
        }
    }

    @Override
    public void responseCompleted() throws IOException, HttpException {
        manageConnectionPersistence();
        this.responseConsumer.responseCompleted(this.localContext);
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Response processed");
        }
        try {
            markCompleted();
            // 先归还/关闭连接，再读取 consumer 结果并完成 BasicFuture；用户 callback 执行时
            // handler 的连接引用已被清空，context 中残留的连接也可能已脱离本次租约。
            // 因此在终态回调时读取 context.getConnection() 不能可靠还原本次实际 peer。
            releaseConnection();
            final T result = this.responseConsumer.getResult();
            final Exception ex = this.responseConsumer.getException();
            if (ex == null) {
                this.resultFuture.completed(result);
            } else {
                this.resultFuture.failed(ex);
            }
        } finally {
            close();
        }
    }

    @Override
    public void inputTerminated() {
        close();
    }

    public void abortConnection() {
        discardConnection();
    }

}
