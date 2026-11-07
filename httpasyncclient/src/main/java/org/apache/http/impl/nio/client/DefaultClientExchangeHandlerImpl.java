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
import org.apache.http.client.methods.HttpExecutionAware;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.BasicFuture;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

/**
 * Default implementation of {@link org.apache.http.nio.protocol.HttpAsyncClientExchangeHandler}.
 * <p>
 * Instances of this class are expected to be accessed by one thread at a time only.
 * The {@link #cancel()} method can be called concurrently by multiple threads.
 */
// 普通客户端一次逻辑 execute 的协调者：一个 producer、consumer、Future 和 InternalState
// 贯穿 CONNECT、认证重发、重定向等多次 HTTP 交换；并不是每次发送都创建一个新 handler。
// 阅读主线：InternalHttpAsyncClient.execute -> start -> 父类 requestConnection /
// connectionAllocated -> HttpCore HttpAsyncRequestExecutor.requestReady -> generateRequest ->
// MainClientExec -> responseCompleted。连接租赁由父类负责，协议策略由 exec 负责。
// start 在调用 execute 的线程执行；后续网络事件通常在 reactor 工作线程执行。
// cancel 可以并发进入，因此不可把这些回调理解为单线程执行器上的任务队列。
class DefaultClientExchangeHandlerImpl<T> extends AbstractClientExchangeHandler {

    // 保留本次 execute 传入的同一个 producer。重定向改变的是 state 中的 request/route，
    // 不会替换这个关联对象；观察一次逻辑请求的所有发送时可以沿此字段找到其入口包装器。
    private final HttpAsyncRequestProducer requestProducer;
    private final HttpAsyncResponseConsumer<T> responseConsumer;
    private final BasicFuture<T> resultFuture;
    private final InternalClientExec exec;
    private final InternalState state;

    public DefaultClientExchangeHandlerImpl(
            final Log log,
            final HttpAsyncRequestProducer requestProducer,
            final HttpAsyncResponseConsumer<T> responseConsumer,
            final HttpClientContext localContext,
            final BasicFuture<T> resultFuture,
            final NHttpClientConnectionManager connmgr,
            final ConnectionReuseStrategy connReuseStrategy,
            final ConnectionKeepAliveStrategy keepaliveStrategy,
            final InternalClientExec exec) {
        super(log, localContext, connmgr, connReuseStrategy, keepaliveStrategy);
        this.requestProducer = requestProducer;
        this.responseConsumer = responseConsumer;
        this.resultFuture = resultFuture;
        this.exec = exec;
        this.state = new InternalState(getId(), requestProducer, responseConsumer, localContext);
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
        try {
            this.requestProducer.failed(ex);
            this.responseConsumer.failed(ex);
        } finally {
            // 即便 producer/consumer 的失败通知抛异常，也尝试完成结果 Future。
            // BasicFuture 会同步调用 callback；父类 failed 的 finally 随后清理连接和资源。
            this.resultFuture.failed(ex);
        }
    }

    @Override
    boolean executionCancelled() {
        // 取消 consumer 不必然得到 cancelled Future：consumer 可能已经有结果或异常，
        // 此时优先交付该结果/异常。返回值来自 consumer.cancel，不能等同 Future 的终态。
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
        // 第一次生成的是用户的逻辑请求，此时本 handler 尚未申请连接。
        // producer.getTarget 提供路由规划输入，generateRequest 提供 method/URI/headers/entity。
        // 即便调用者恰好是另一次请求的 I/O 回调，也不能借用该线程外层请求的连接。
        final HttpHost target = this.requestProducer.getTarget();
        final HttpRequest original = this.requestProducer.generateRequest();

        if (original instanceof HttpExecutionAware) {
            // 将 HttpUriRequest.abort 等入口绑定到本次 exchange 的取消与资源清理。
            ((HttpExecutionAware) original).setCancellable(this);
        }
        this.exec.prepare(target, original, this.state, this);
        requestConnection();
    }

    @Override
    public HttpRequest generateRequest() throws IOException, HttpException {
        // 与上面 producer.generateRequest 不是同一个阶段：HttpCore 已拿到连接，
        // 从连接 context 取出本 handler 后才调用这里。exec 可返回 CONNECT 或业务请求；
        // 同一 handler 会因认证/重定向多次到达这里，因此这是每次实际发送前的关联点。
        // 这里不重新调用用户 producer.generateRequest，后续请求保存在 InternalState 中。
        return this.exec.generateRequest(this.state, this);
    }

    @Override
    public void produceContent(
            final ContentEncoder encoder, final IOControl ioControl) throws IOException {
        this.exec.produceContent(this.state, encoder, ioControl);
    }

    @Override
    public void requestCompleted() {
        this.exec.requestCompleted(this.state, this);
    }

    @Override
    public void responseReceived(
            final HttpResponse response) throws IOException, HttpException {
        this.exec.responseReceived(response, this.state, this);
    }

    @Override
    public void consumeContent(
            final ContentDecoder decoder, final IOControl ioControl) throws IOException {
        this.exec.consumeContent(this.state, decoder, ioControl);
        if (!decoder.isCompleted() && this.responseConsumer.isDone()) {
            // consumer 提前结束，但响应体尚未读完：余下字节不能作为下个响应复用，
            // 因而标记不可复用、关闭连接并取消 Future；不是正常的完整响应成功路径。
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
        // exec 先处理持久连接、认证和重定向。finalResponse 为 null 表示仍需后续交换，
        // 并非“没有收到响应”；中间响应已经消费/丢弃完毕。
        this.exec.responseCompleted(this.state, this);

        if (this.state.getFinalResponse() != null || this.resultFuture.isDone()) {
            try {
                markCompleted();
                releaseConnection();
                // 连接在 Future callback 之前已归还（exec 内也可能更早释放）。
                // 若完成时需要连接 IP/端口，应使用此前保存的值，不能持有池代理到这里再读。
                // BasicFuture 先设置终态并唤醒 get，再同步调用 callback；get 返回不代表
                // callback 已执行完毕，也不代表下面 finally 的 close 已完成。
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
        } else {
            // 同 route 且仍打开的连接可以继续发送；换 route 或连接已关闭时重新租赁。
            // 用户 producer 与最终 Future 保持不变，变化的是当前请求和连接。
            NHttpClientConnection localConn = getConnection();
            if (localConn != null && !localConn.isOpen()) {
                releaseConnection();
                localConn = null;
            }
            if (localConn != null) {
                localConn.requestOutput();
            } else {
                requestConnection();
            }
        }
    }

    @Override
    public void inputTerminated() {
        // 输入端终止时，若逻辑交换还未标记完成则重新申请连接。
        // 这只是此事件的恢复路径；不能据此推断所有 I/O 异常都有自动重试。
        if (!isCompleted()) {
            requestConnection();
        } else {
            close();
        }
    }

    public void abortConnection() {
        discardConnection();
    }

}
