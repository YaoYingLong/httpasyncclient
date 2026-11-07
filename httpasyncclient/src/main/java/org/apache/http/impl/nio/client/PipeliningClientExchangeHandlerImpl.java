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
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.http.ConnectionClosedException;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
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
import org.apache.http.nio.protocol.Pipelined;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.util.Args;
import org.apache.http.util.Asserts;

/**
 * {@link org.apache.http.nio.protocol.HttpAsyncClientExchangeHandler} implementation
 * that supports HTTP message pipelining.
 * <p>
 * Instances of this class are expected to be accessed by one thread at a time only.
 * The {@link #cancel()} method can be called concurrently by multiple threads.
 */
@Pipelined
class PipeliningClientExchangeHandlerImpl<T> extends AbstractClientExchangeHandler {

    /*
     * 一个 handler 对应整个 pipeline 批次，不是一条请求。@Pipelined 供 HttpCore executor
     * 识别流水线模式，使前一响应尚未到达时仍可发送后续请求；它不是多连接并发调度器。
     * 正常收发事件按连接顺序推进，cancel 可以从其他线程并发进入；原子引用用于领取/
     * 清空当前资源，并不意味着允许任意线程并发调用 generateRequest 或 responseReceived。
     *
     * 四个 FIFO 各有职责：producerQueue 保存待发送项，consumerQueue 保存待接收项，
     * requestQueue 保存已生成但尚未匹配最终响应的请求，resultQueue 按响应顺序累积结果。
     * 发送与接收进度可以错开，不能用“当前 producer”识别正在到达的响应。
     */

    private final HttpHost target;
    private final Queue<HttpAsyncRequestProducer> requestProducerQueue;
    private final Queue<HttpAsyncResponseConsumer<T>> responseConsumerQueue;
    private final Queue<HttpRequest> requestQueue;
    private final Queue<T> resultQueue;
    private final HttpClientContext localContext;
    private final BasicFuture<List<T>> resultFuture;
    private final HttpProcessor httpProcessor;
    private final AtomicReference<HttpAsyncRequestProducer> requestProducerRef;
    private final AtomicReference<HttpAsyncResponseConsumer<T>> responseConsumerRef;

    public PipeliningClientExchangeHandlerImpl(
            final Log log,
            final HttpHost target,
            final List<? extends HttpAsyncRequestProducer> requestProducers,
            final List<? extends HttpAsyncResponseConsumer<T>> responseConsumers,
            final HttpClientContext localContext,
            final BasicFuture<List<T>> resultFuture,
            final NHttpClientConnectionManager connmgr,
            final HttpProcessor httpProcessor,
            final ConnectionReuseStrategy connReuseStrategy,
            final ConnectionKeepAliveStrategy keepaliveStrategy) {
        super(log, localContext, connmgr, connReuseStrategy, keepaliveStrategy);
        Args.notNull(target, "HTTP target");
        Args.notEmpty(requestProducers, "Request producer list");
        Args.notEmpty(responseConsumers, "Response consumer list");
        Args.check(requestProducers.size() == responseConsumers.size(),
                "Number of request producers does not match that of response consumers");
        // 两个列表按索引配对；这里只复制容器，不复制 producer/consumer 对象。
        // 整个批次使用显式 target，不逐项调用 producer.getTarget() 重新决定路由。
        this.target = target;
        this.requestProducerQueue = new ConcurrentLinkedQueue<HttpAsyncRequestProducer>(requestProducers);
        this.responseConsumerQueue = new ConcurrentLinkedQueue<HttpAsyncResponseConsumer<T>>(responseConsumers);
        this.requestQueue = new ConcurrentLinkedQueue<HttpRequest>();
        this.resultQueue = new ConcurrentLinkedQueue<T>();
        this.localContext = localContext;
        this.resultFuture = resultFuture;
        this.httpProcessor = httpProcessor;
        this.requestProducerRef = new AtomicReference<HttpAsyncRequestProducer>(null);
        this.responseConsumerRef = new AtomicReference<HttpAsyncResponseConsumer<T>>(null);
    }

    private void closeProducer(final HttpAsyncRequestProducer requestProducer) {
        if (requestProducer != null) {
            try {
                requestProducer.close();
            } catch (final IOException ex) {
                this.log.debug("I/O error closing request producer", ex);
            }
        }
    }

    private void closeConsumer(final HttpAsyncResponseConsumer<?> responseConsumer) {
        if (responseConsumer != null) {
            try {
                responseConsumer.close();
            } catch (final IOException ex) {
                this.log.debug("I/O error closing response consumer", ex);
            }
        }
    }

    @Override
    void releaseResources() {
        // 当前项已从队列取出，必须先关闭 Ref 中的资源，再关闭尚未处理的队列项。
        // 正常完成的 producer/consumer 在各自完成回调中关闭；这里承担提前结束时的清理。
        closeProducer(this.requestProducerRef.getAndSet(null));
        closeConsumer(this.responseConsumerRef.getAndSet(null));
        while (!this.requestProducerQueue.isEmpty()) {
            closeProducer(this.requestProducerQueue.remove());
        }
        while (!this.responseConsumerQueue.isEmpty()) {
            closeConsumer(this.responseConsumerQueue.remove());
        }
        this.requestQueue.clear();
        this.resultQueue.clear();
    }

    @Override
    void executionFailed(final Exception ex) {
        // 当前 producer/consumer 收到 failed；未开始接收的 consumer 收到 cancel。
        // 批次只有一个结果 Future，单项失败会使整批失败，不返回“已成功部分”的列表。
        try {
            final HttpAsyncRequestProducer requestProducer = this.requestProducerRef.get();
            if (requestProducer != null) {
                requestProducer.failed(ex);
            }
            final HttpAsyncResponseConsumer<T> responseConsumer = this.responseConsumerRef.get();
            if (responseConsumer != null) {
                responseConsumer.failed(ex);
            }
            for (final HttpAsyncResponseConsumer<T> cancellable: this.responseConsumerQueue) {
                cancellable.cancel();
            }
        } finally {
            this.resultFuture.failed(ex);
        }
    }

    @Override
    boolean executionCancelled() {
        // 与普通请求不同，这里直接取消整批 Future，不从当前 consumer 提取一个成功结果。
        final HttpAsyncResponseConsumer<T> responseConsumer = this.responseConsumerRef.get();
        final boolean cancelled = responseConsumer != null && responseConsumer.cancel();
        this.resultFuture.cancel();
        return cancelled;
    }

    public void start() throws HttpException, IOException {
        // execute 调用线程只建立批次路由并申请连接；尚未调用任何 producer.generateRequest。
        // 与 Default/Minimal 单请求 handler 在 start 中生成原始请求的时机不同。
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] start execution");
        }

        final HttpRoute route = new HttpRoute(this.target);
        setRoute(route);

        this.localContext.setAttribute(HttpCoreContext.HTTP_TARGET_HOST, this.target);
        this.localContext.setAttribute(HttpClientContext.HTTP_ROUTE, route);

        requestConnection();
    }

    @Override
    public HttpRequest generateRequest() throws IOException, HttpException {
        // HttpCore 在当前连接可发送时调用此入口。首次调用完成必要的路由初始化，
        // 之后在同一连接上继续取下一个 producer；队列耗尽返回 null，让 executor 暂停输出。
        verifytRoute();
        if (!isRouteEstablished()) {
            onRouteToTarget();
            onRouteComplete();
        }
        final NHttpClientConnection localConn = getConnection();
        this.localContext.setAttribute(HttpCoreContext.HTTP_CONNECTION, localConn);

        Asserts.check(this.requestProducerRef.get() == null, "Inconsistent state: currentRequest producer is not null");
        final HttpAsyncRequestProducer requestProducer = this.requestProducerQueue.poll();
        if (requestProducer == null) {
            return null;
        }
        this.requestProducerRef.set(requestProducer);

        // 同步调用链为 HttpCore requestReady(conn) -> 本方法 -> producer.generateRequest，
        // 而普通请求的 producer 早在 start 中被调用。分析连接与单项请求的关联时，
        // 必须保留这一区别；后续请求体和响应的事件不一定仍在这次 requestReady 调用栈内。
        // localContext 是整批复用的可变对象；producer 是用户扩展点，可在其中嵌套调用客户端，
        // 因而不能把回调之后的 context 属性当作不可变的“本项连接快照”。
        final HttpRequest original = requestProducer.generateRequest();
        final HttpRequestWrapper currentRequest = HttpRequestWrapper.wrap(original);
        final RequestConfig config = this.localContext.getRequestConfig();
        if (config.getSocketTimeout() > 0) {
            localConn.setSocketTimeout(config.getSocketTimeout());
        }

        this.httpProcessor.process(currentRequest, this.localContext);

        this.requestQueue.add(currentRequest);
        // 请求完成发送后仍留在 requestQueue，直到 responseReceived 按响应顺序取走。
        setCurrentRequest(currentRequest);

        return currentRequest;
    }

    @Override
    public void produceContent(
            final ContentEncoder encoder, final IOControl ioControl) throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] produce content");
        }
        final HttpAsyncRequestProducer requestProducer = this.requestProducerRef.get();
        Asserts.check(requestProducer != null, "Inconsistent state: request producer is null");
        requestProducer.produceContent(encoder, ioControl);
        if (encoder.isCompleted()) {
            requestProducer.resetRequest();
        }
    }

    @Override
    public void requestCompleted() {
        // 只表示当前请求的输出阶段完成，不表示对应响应已完成，更不表示整个批次完成。
        // 先清空当前 producer，再通知并关闭它，后续 generateRequest 才能领取下一个项。
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Request completed");
        }
        final HttpAsyncRequestProducer requestProducer = this.requestProducerRef.getAndSet(null);
        Asserts.check(requestProducer != null, "Inconsistent state: request producer is null");
        requestProducer.requestCompleted(this.localContext);
        try {
            requestProducer.close();
        } catch (final IOException ioex) {
            this.log.debug(ioex.getMessage(), ioex);
        }
    }

    @Override
    public void responseReceived(
            final HttpResponse response) throws IOException, HttpException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Response received " + response.getStatusLine());
        }

        Asserts.check(this.responseConsumerRef.get() == null, "Inconsistent state: response consumer is not null");

        final HttpAsyncResponseConsumer<T> responseConsumer = this.responseConsumerQueue.poll();
        // HTTP/1.x pipeline 的最终响应按请求顺序返回；consumer 与 request 两个 FIFO
        // 同步出队建立对应关系。此时发送侧可能已经推进到更后的 producer。
        Asserts.check(responseConsumer != null, "Inconsistent state: response consumer queue is empty");
        this.responseConsumerRef.set(responseConsumer);

        final HttpRequest request = this.requestQueue.poll();
        Asserts.check(request != null, "Inconsistent state: request queue is empty");

        this.localContext.setAttribute(HttpCoreContext.HTTP_REQUEST, request);
        // 请求/响应上下文在这里切换到“正在接收”的项，不能据此推断最新发送项。
        this.localContext.setAttribute(HttpCoreContext.HTTP_RESPONSE, response);
        this.httpProcessor.process(response, this.localContext);

        responseConsumer.responseReceived(response);

        setCurrentResponse(response);
    }

    @Override
    public void consumeContent(
            final ContentDecoder decoder, final IOControl ioControl) throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Consume content");
        }
        final HttpAsyncResponseConsumer<T> responseConsumer = this.responseConsumerRef.get();
        Asserts.check(responseConsumer != null, "Inconsistent state: response consumer is null");
        responseConsumer.consumeContent(decoder, ioControl);
    }

    @Override
    public void responseCompleted() throws IOException, HttpException {
        // 每个响应都更新连接复用策略并完成自己的 consumer；只有所有响应消费成功才发布
        // 整批 List<T>。consumer 的非空结果是这里的成功条件，null 会进入 failed 分支。
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + getId() + "] Response processed");
        }

        final boolean keepAlive = manageConnectionPersistence();

        final HttpAsyncResponseConsumer<T> responseConsumer = this.responseConsumerRef.getAndSet(null);
        Asserts.check(responseConsumer != null, "Inconsistent state: response consumer is null");
        try {
            responseConsumer.responseCompleted(this.localContext);
            final T result = responseConsumer.getResult();
            final Exception ex = responseConsumer.getException();
            try {
                responseConsumer.close();
            } catch (final IOException ioex) {
                this.log.debug(ioex.getMessage(), ioex);
            }
            if (result != null) {
                this.resultQueue.add(result);
            } else {
                failed(ex);
            }
            if (!this.resultFuture.isDone() && this.responseConsumerQueue.isEmpty()) {
                // BasicFuture 同步执行用户 callback；与普通 handler 不同，此处尚未 close。
                // 因而不能假设所有成功回调发生在归还连接之后，也不能依赖统一的释放时序。
                this.resultFuture.completed(new ArrayList<T>(this.resultQueue));
                this.resultQueue.clear();
            }

            if (this.resultFuture.isDone()) {
                close();
            } else {
                if (!keepAlive) {
                    // 还有响应未完成而服务器不保持连接时，当前实现直接失败整个批次，
                    // 不把未收到响应的已发送请求自动重发，不能期待自动重发补齐剩余结果。
                    failed(new ConnectionClosedException("Connection closed"));
                } else {
                    final NHttpClientConnection localConn = getConnection();
                    if (localConn != null) {
                        localConn.requestOutput();
                    } else {
                        requestConnection();
                    }
                }
            }
        } catch (final RuntimeException ex) {
            failed(ex);
            throw ex;
        }
    }

    @Override
    public void inputTerminated() {
        failed(new ConnectionClosedException("Connection closed"));
    }

    public void abortConnection() {
        discardConnection();
    }

}
