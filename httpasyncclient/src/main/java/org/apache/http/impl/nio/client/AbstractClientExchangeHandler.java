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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.http.ConnectionClosedException;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.RouteTracker;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.protocol.HttpAsyncClientExchangeHandler;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Asserts;

/**
 * Abstract {@link org.apache.http.nio.protocol.HttpAsyncClientExchangeHandler} class
 * that implements connection management aspects shared by all HTTP exchange handlers.
 * <p>
 * Instances of this class are expected to be accessed by one thread at a time only.
 * The {@link #cancel()} method can be called concurrently by multiple threads.
 */
// 一个 exchange 的连接所有权与终止控制。普通、minimal、pipeline handler 共享此实现。
// routeRef 是计划走的路由；routeTrackerRef 是当前已完成的直连/代理/隧道/TLS 步骤；
// managedConnRef 是本次租赁的连接代理，三者不能互相替代，更不能由 route 推导真实 peer。
// 原子引用保护跨线程发布和取消竞争；它们不使任意两个协议回调都可以并发调用。
// 尤其不要并发复用同一个用户 HttpContext：它还保存了可变的 request/response/connection。
abstract class AbstractClientExchangeHandler implements HttpAsyncClientExchangeHandler {

    private static final AtomicLong COUNTER = new AtomicLong(1);

    protected final Log log;

    private final long id;
    private final HttpClientContext localContext;
    private final NHttpClientConnectionManager connmgr;
    private final ConnectionReuseStrategy connReuseStrategy;
    private final ConnectionKeepAliveStrategy keepaliveStrategy;
    // 租连接的 Future 与应用拿到的结果 Future 是两个对象：前者用于取消排队/建连，
    // 后者由子类在响应消费结束后完成。
    private final AtomicReference<Future<NHttpClientConnection>> connectionFutureRef;
    private final AtomicReference<NHttpClientConnection> managedConnRef;
    private final AtomicReference<HttpRoute> routeRef;
    private final AtomicReference<RouteTracker> routeTrackerRef;
    private final AtomicBoolean routeEstablished;
    // null 表示不可复用；非 null 才能归还为可复用连接。
    // 数值 <= 0 表示 keep-alive 未指定有限时长，并不等于“不可复用”，池 TTL 仍可限制寿命。
    private final AtomicReference<Long> validDurationRef;
    private final AtomicReference<HttpRequestWrapper> requestRef;
    private final AtomicReference<HttpResponse> responseRef;
    // completed 是协议层 isDone 所见的完成标记；closed 是终止/清理的一次性闸门。
    // failed/cancel 走 closed，但并不设置 completed，不能将二者当作同一状态。
    private final AtomicBoolean completed;
    private final AtomicBoolean closed;

    AbstractClientExchangeHandler(
            final Log log,
            final HttpClientContext localContext,
            final NHttpClientConnectionManager connmgr,
            final ConnectionReuseStrategy connReuseStrategy,
            final ConnectionKeepAliveStrategy keepaliveStrategy) {
        super();
        this.log = log;
        this.id = COUNTER.getAndIncrement();
        this.localContext = localContext;
        this.connmgr = connmgr;
        this.connReuseStrategy = connReuseStrategy;
        this.keepaliveStrategy = keepaliveStrategy;
        this.connectionFutureRef = new AtomicReference<Future<NHttpClientConnection>>(null);
        this.managedConnRef = new AtomicReference<NHttpClientConnection>(null);
        this.routeRef = new AtomicReference<HttpRoute>(null);
        this.routeTrackerRef = new AtomicReference<RouteTracker>(null);
        this.routeEstablished = new AtomicBoolean(false);
        this.validDurationRef = new AtomicReference<Long>(null);
        this.requestRef = new AtomicReference<HttpRequestWrapper>(null);
        this.responseRef = new AtomicReference<HttpResponse>(null);
        this.completed = new AtomicBoolean(false);
        this.closed = new AtomicBoolean(false);
    }

    final long getId() {
        return this.id;
    }

    final boolean isCompleted() {
        return this.completed.get();
    }

    final void markCompleted() {
        this.completed.set(true);
    }

    final void markConnectionNonReusable() {
        this.validDurationRef.set(null);
    }

    final boolean isRouteEstablished() {
        return this.routeEstablished.get();
    }

    final HttpRoute getRoute() {
        return this.routeRef.get();
    }

    final void setRoute(final HttpRoute route) {
        this.routeRef.set(route);
    }

    final HttpRequestWrapper getCurrentRequest() {
        return this.requestRef.get();
    }

    final void setCurrentRequest(final HttpRequestWrapper request) {
        this.requestRef.set(request);
    }

    final HttpResponse getCurrentResponse() {
        return this.responseRef.get();
    }

    final void setCurrentResponse(final HttpResponse response) {
        this.responseRef.set(response);
    }

    final HttpRoute getActualRoute() {
        final RouteTracker routeTracker = this.routeTrackerRef.get();
        return routeTracker != null ? routeTracker.toRoute() : null;
    }

    final void verifytRoute() {
        // 从池中取出的连接可能已经完成 CONNECT/TLS；复用时无需再次执行路由建立。
        // 新连接则建立 tracker，交给 MainClientExec 的 BasicRouteDirector 逐步推进。
        if (!this.routeEstablished.get() && this.routeTrackerRef.get() == null) {
            final NHttpClientConnection managedConn = this.managedConnRef.get();
            Asserts.check(managedConn != null, "Inconsistent state: managed connection is null");
            final boolean routeComplete = this.connmgr.isRouteComplete(managedConn);
            this.routeEstablished.set(routeComplete);
            if (!routeComplete) {
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + this.id + "] Start connection routing");
                }
                final HttpRoute route = this.routeRef.get();
                this.routeTrackerRef.set(new RouteTracker(route));
            } else {
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + this.id + "] Connection route already established");
                }
            }
        }
    }

    final void onRouteToTarget() throws IOException {
        // TCP 连接已在租赁阶段建立。startRoute 在已有 IOSession 上启动所需协议层，
        // 这里的 CONNECT_TARGET 是路由状态机步骤，不是再创建一个 TCP socket。
        final NHttpClientConnection managedConn = this.managedConnRef.get();
        Asserts.check(managedConn != null, "Inconsistent state: managed connection is null");
        final HttpRoute route = this.routeRef.get();
        Asserts.check(route != null, "Inconsistent state: HTTP route is null");
        final RouteTracker routeTracker = this.routeTrackerRef.get();
        Asserts.check(routeTracker != null, "Inconsistent state: HTTP route tracker");
        this.connmgr.startRoute(managedConn, route, this.localContext);
        routeTracker.connectTarget(route.isSecure());
    }

    final void onRouteToProxy() throws IOException {
        final NHttpClientConnection managedConn = this.managedConnRef.get();
        Asserts.check(managedConn != null, "Inconsistent state: managed connection is null");
        final HttpRoute route = this.routeRef.get();
        Asserts.check(route != null, "Inconsistent state: HTTP route is null");
        final RouteTracker routeTracker = this.routeTrackerRef.get();
        Asserts.check(routeTracker != null, "Inconsistent state: HTTP route tracker");
        this.connmgr.startRoute(managedConn, route, this.localContext);
        final HttpHost proxy  = route.getProxyHost();
        routeTracker.connectProxy(proxy, false);
    }

    final void onRouteUpgrade() throws IOException {
        // 典型路径是代理 CONNECT 成功后在现有会话上叠加 TLS；TCP 对端仍然是代理。
        final NHttpClientConnection managedConn = this.managedConnRef.get();
        Asserts.check(managedConn != null, "Inconsistent state: managed connection is null");
        final HttpRoute route = this.routeRef.get();
        Asserts.check(route != null, "Inconsistent state: HTTP route is null");
        final RouteTracker routeTracker = this.routeTrackerRef.get();
        Asserts.check(routeTracker != null, "Inconsistent state: HTTP route tracker");
        this.connmgr.upgrade(managedConn, route, this.localContext);
        routeTracker.layerProtocol(route.isSecure());
    }

    final void onRouteTunnelToTarget() {
        final RouteTracker routeTracker = this.routeTrackerRef.get();
        Asserts.check(routeTracker != null, "Inconsistent state: HTTP route tracker");
        routeTracker.tunnelTarget(false);
    }

    final void onRouteComplete() {
        final NHttpClientConnection managedConn = this.managedConnRef.get();
        Asserts.check(managedConn != null, "Inconsistent state: managed connection is null");
        final HttpRoute route = this.routeRef.get();
        Asserts.check(route != null, "Inconsistent state: HTTP route is null");
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + this.id + "] route completed");
        }
        this.connmgr.routeComplete(managedConn, route, this.localContext);
        this.routeEstablished.set(true);
        this.routeTrackerRef.set(null);
    }

    final NHttpClientConnection getConnection() {
        return this.managedConnRef.get();
    }

    final void releaseConnection() {
        // 先解除本 handler 的所有权，避免取消和完成两条路径重复归还同一次租赁。
        final NHttpClientConnection localConn = this.managedConnRef.getAndSet(null);
        if (localConn != null) {
            if (this.log.isDebugEnabled()) {
                this.log.debug("[exchange: " + this.id + "] releasing connection");
            }
            // 这是连接自身的 context，不是应用传入的 localContext。
            // HTTP_HANDLER 将协议事件路由到当前 exchange；归还之前必须解除此关联。
            localConn.getContext().removeAttribute(HttpAsyncRequestExecutor.HTTP_HANDLER);
            final Long validDuration = this.validDurationRef.get();
            if (validDuration != null) {
                final Object userToken = this.localContext.getUserToken();
                this.connmgr.releaseConnection(localConn, userToken, validDuration, TimeUnit.MILLISECONDS);
            } else {
                try {
                    localConn.close();
                    if (this.log.isDebugEnabled()) {
                        this.log.debug("[exchange: " + this.id + "] connection discarded");
                    }
                } catch (final IOException ex) {
                    if (this.log.isDebugEnabled()) {
                        this.log.debug("[exchange: " + this.id + "] " + ex.getMessage(), ex);
                    }
                } finally {
                    this.connmgr.releaseConnection(localConn, null, 0, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    final void discardConnection() {
        // 异常/取消的强制终止路径：shutdown 后归还池槽位，不能复用未完成的协议流。
        // 连接管理器会 detach 代理；保存过该代理的应用 context 不因此变成稳定快照。
        final NHttpClientConnection localConn = this.managedConnRef.getAndSet(null);
        if (localConn != null) {
            try {
                localConn.shutdown();
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + this.id + "] connection aborted");
                }
            } catch (final IOException ex) {
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + this.id + "] " + ex.getMessage(), ex);
                }
            } finally {
                this.connmgr.releaseConnection(localConn, null, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    final boolean manageConnectionPersistence() {
        // 响应结束后的两个独立问题：协议是否允许 keep-alive，以及允许空闲保留多久。
        // 开着的 socket 不一定可复用（例如响应要求 Connection: close）。
        final HttpResponse response = this.responseRef.get();
        Asserts.check(response != null, "Inconsistent state: HTTP response");
        final NHttpClientConnection managedConn = this.managedConnRef.get();
        Asserts.check(managedConn != null, "Inconsistent state: managed connection is null");
        final boolean keepAlive = managedConn.isOpen() &&
                this.connReuseStrategy.keepAlive(response, this.localContext);
        if (keepAlive) {
            final long validDuration = this.keepaliveStrategy.getKeepAliveDuration(
                    response, this.localContext);
            if (this.log.isDebugEnabled()) {
                final String s;
                if (validDuration > 0) {
                    s = "for " + validDuration + " " + TimeUnit.MILLISECONDS;
                } else {
                    s = "indefinitely";
                }
                this.log.debug("[exchange: " + this.id + "] Connection can be kept alive " + s);
            }
            this.validDurationRef.set(validDuration);
        } else {
            if (this.log.isDebugEnabled()) {
                this.log.debug("[exchange: " + this.id + "] Connection cannot be kept alive");
            }
            this.validDurationRef.set(null);
        }
        return keepAlive;
    }

    private void connectionAllocated(final NHttpClientConnection managedConn) {
        /*
         * 连接池已经把一条连接交给本次请求了：新建时 TCP 已连好，复用时 TCP 早已存在。
         * 例如直连 example.com 时，它连向目标；配置 HTTP 代理时，它连向代理。
         * HTTPS 请求经 HTTP 代理时，CONNECT 和 TLS 仍可能尚未开始，由 MainClientExec 继续处理。
         *
         * 下面做两次关联：本 handler 保存连接；连接的 context 保存本 handler。
         * 这样“本次请求”能找到连接，“收到网络事件的连接”也能找到本次请求。
         */
        // manager 的 FutureCallback 将租赁结果交到这里。复用连接可能同步回调，
        // 新建连接则经 reactor 完成；不能仅凭“异步 API”认定这里总是另一个线程。
        try {
            if (this.log.isDebugEnabled()) {
                this.log.debug("[exchange: " + this.id + "] Connection allocated: " + managedConn);
            }
            this.connectionFutureRef.set(null);
            // 从这一行起，handler.getConnection() 就能取得本次分配到的连接。
            this.managedConnRef.set(managedConn);

            if (this.closed.get()) {
                // 申请连接期间用户可能已取消；刚到达的连接也必须被回收，不能再激活发送。
                discardConnection();
                return;
            }

            if (this.connmgr.isRouteComplete(managedConn)) {
                this.routeEstablished.set(true);
                this.routeTrackerRef.set(null);
            }

            final HttpContext context = managedConn.getContext();
            synchronized (context) {
                // 与 HttpCore requestReady 在同一连接 context 上的同步相配合：先发布
                // handler，再请求 OP_WRITE。requestOutput 只通知 reactor，不直接发送字节。
                // this 就是 execute() 创建的那个 handler；HttpCore 稍后通过这个键把它取回。
                context.setAttribute(HttpAsyncRequestExecutor.HTTP_HANDLER, this);
                if (managedConn.isStale()) {
                    failed(new ConnectionClosedException("Connection closed"));
                } else {
                    // 底层只设置 SelectionKey.OP_WRITE，意思是“请继续处理这条连接的输出”。
                    // 写事件进入 InternalIODispatch.onOutputReady；新连接的 onConnected
                    // 也会触发首轮 requestReady。真正写字节的代码在 HttpCore 的连接实现中。
                    managedConn.requestOutput();
                }
            }
        } catch (final RuntimeException runex) {
            failed(runex);
            throw runex;
        }
    }

    private void connectionRequestFailed(final Exception ex) {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + this.id + "] connection request failed");
        }
        this.connectionFutureRef.set(null);
        failed(ex);
    }

    private void connectionRequestCancelled() {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + this.id + "] Connection request cancelled");
        }
        this.connectionFutureRef.set(null);
        try {
            executionCancelled();
        } finally {
            close();
        }
    }

    final void requestConnection() {
        /*
         * 这里的申请方式是“给我连接，准备好后调用 completed(conn)”。
         * 所以下面的 FutureCallback 要分两步理解：现在创建并传给管理器；连接交付时，
         * 管理器再调用它的 completed 方法。没有在此通过 Future.get() 阻塞等待连接。
         * 若池中已有合适的空闲连接，completed 也可能在本次调用尚未返回时立即执行。
         */
        final HttpRoute route = this.routeRef.get();
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + this.id + "] Request connection for " + route);
        }

        // 开始新一次租赁时先丢弃旧连接并重置“路由已建立/可复用”状态。
        // 若新目的地建连失败，后续不会进入 generateRequest；发送点观察者也不会得到
        // 新 peer。是否清除上次记录属于观察者自己的语义，不能从此路径自动推得。
        discardConnection();

        this.validDurationRef.set(null);
        this.routeTrackerRef.set(null);
        this.routeEstablished.set(false);

        final Object userToken = this.localContext.getUserToken();
        final RequestConfig config = this.localContext.getRequestConfig();
        // userToken 是连接池状态匹配键（例如绑定认证身份），不是 tracing context。
        // connectTimeout 限制建连，connectionRequestTimeout 限制池租赁等待；
        // socketTimeout 在发送前另行设置。三者对应不同阶段。
        this.connectionFutureRef.set(this.connmgr.requestConnection(
                route,
                userToken,
                config.getConnectTimeout(),
                config.getConnectionRequestTimeout(),
                TimeUnit.MILLISECONDS,
                new FutureCallback<NHttpClientConnection>() {

                    @Override
                    public void completed(final NHttpClientConnection managedConn) {
                        // 这是“连接拿到了”的回调；不是用户传入 execute 的“响应处理完成”回调。
                        connectionAllocated(managedConn);
                    }

                    @Override
                    public void failed(final Exception ex) {
                        connectionRequestFailed(ex);
                    }

                    @Override
                    public void cancelled() {
                        connectionRequestCancelled();
                    }

                }));
    }

    abstract void releaseResources();

    abstract void executionFailed(final Exception ex);

    abstract boolean executionCancelled();

    @Override
    public final void close() {
        // close 本身只清理，不负责替子类完成结果 Future；调用方必须先决定结果终态。
        if (this.closed.compareAndSet(false, true)) {
            discardConnection();
            releaseResources();
        }
    }

    @Override
    public final boolean isDone() {
        return this.completed.get();
    }

    @Override
    public final void failed(final Exception ex) {
        // 先占有终止权，再通知子类/Future；即使通知抛异常，finally 仍归还连接并关流。
        if (this.closed.compareAndSet(false, true)) {
            try {
                executionFailed(ex);
            } finally {
                discardConnection();
                releaseResources();
            }
        }
    }

    @Override
    public final boolean cancel() {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + this.id + "] Cancelled");
        }
        if (this.closed.compareAndSet(false, true)) {
            try {
                // 用户取消可能与租赁完成、读取响应竞争。取消租赁 Future 只是其中一步，
                // executionCancelled 决定结果，finally 负责已经分配到的连接和 producer/consumer。
                final Future<NHttpClientConnection> connectionFuture = this.connectionFutureRef.getAndSet(null);
                if (connectionFuture != null) {
                    connectionFuture.cancel(true);
                }
                return executionCancelled();
            } finally {
                discardConnection();
                releaseResources();
            }
        }
        return false;
    }

}
