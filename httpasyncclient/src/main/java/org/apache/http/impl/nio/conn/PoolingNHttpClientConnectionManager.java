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
package org.apache.http.impl.nio.conn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.concurrent.BasicFuture;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Lookup;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.SchemePortResolver;
import org.apache.http.conn.UnsupportedSchemeException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.ManagedNHttpClientConnection;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NHttpConnectionFactory;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.pool.NIOConnFactory;
import org.apache.http.nio.pool.SocketAddressResolver;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOSession;
import org.apache.http.pool.ConnPoolControl;
import org.apache.http.pool.PoolStats;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Args;
import org.apache.http.util.Asserts;

/**
 * {@code PoolingNHttpClientConnectionManager} maintains a pool of
 * {@link NHttpClientConnection}s and is able to service connection requests
 * from multiple execution threads. Connections are pooled on a per route
 * basis. A request for a route which already the manager has persistent
 * connections for available in the pool will be services by leasing
 * a connection from the pool rather than creating a brand new connection.
 * <p>
 * {@code PoolingNHttpClientConnectionManager} maintains a maximum limit
 * of connection on a per route basis and in total. Per default this
 * implementation will create no more than than 2 concurrent connections
 * per given route and no more 20 connections in total. For many real-world
 * applications these limits may prove too constraining, especially if they
 * use HTTP as a transport protocol for their services. Connection limits,
 * however, can be adjusted using {@link ConnPoolControl} methods.
 *
 * @since 4.0
 */
@Contract(threading = ThreadingBehavior.SAFE)
public class PoolingNHttpClientConnectionManager
       implements NHttpClientConnectionManager, ConnPoolControl<HttpRoute> {

    /*
     * 连接管理分为三层：本类把 HttpRoute、租赁回调和 TLS 路由建立衔接起来；CPool /
     * HttpCore AbstractNIOConnPool 管理连接容量、等待队列及复用；ConnectingIOReactor
     * 完成实际的非阻塞 TCP 连接。HttpRoute 是池的分组依据，不能直接当作已连接的 peer。
     *
     * 池中长期保存 CPoolEntry 和真实连接，每次租赁另建 CPoolProxy；释放先让本次代理失效，
     * 可复用的真实连接仍留在池中。HTTP exchange 与物理连接因此不是一对一关系。
     */
    private final Log log = LogFactory.getLog(getClass());

    static final String IOSESSION_FACTORY_REGISTRY = "http.ioSession-factory-registry";

    private final ConnectingIOReactor ioReactor;
    private final ConfigData configData;
    private final CPool pool;
    private final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry;

    private static Registry<SchemeIOSessionStrategy> getDefaultRegistry() {
        return RegistryBuilder.<SchemeIOSessionStrategy>create()
                .register("http", NoopIOSessionStrategy.INSTANCE)
                .register("https", SSLIOSessionStrategy.getDefaultStrategy())
                .build();
    }

    public PoolingNHttpClientConnectionManager(final ConnectingIOReactor ioReactor) {
        this(ioReactor, getDefaultRegistry());
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry) {
        this(ioReactor, null, ioSessionFactoryRegistry, (DnsResolver) null);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory,
            final DnsResolver dnsResolver) {
        this(ioReactor, connFactory, getDefaultRegistry(), dnsResolver);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory,
            final SocketAddressResolver<HttpRoute> socketAddressResolver) {
        this(ioReactor, connFactory, getDefaultRegistry(), socketAddressResolver);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory) {
        this(ioReactor, connFactory, getDefaultRegistry(), (DnsResolver) null);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory,
            final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry) {
        this(ioReactor, connFactory, ioSessionFactoryRegistry, (DnsResolver) null);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory,
            final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry,
            final DnsResolver dnsResolver) {
        this(ioReactor, connFactory, ioSessionFactoryRegistry, null, dnsResolver,
            -1, TimeUnit.MILLISECONDS);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory,
            final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry,
            final SocketAddressResolver<HttpRoute> socketAddressResolver) {
        this(ioReactor, connFactory, ioSessionFactoryRegistry, socketAddressResolver,
                -1, TimeUnit.MILLISECONDS);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory,
            final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry,
            final SchemePortResolver schemePortResolver,
            final DnsResolver dnsResolver,
            final long timeToLive, final TimeUnit timeUnit) {
        this(ioReactor, connFactory, ioSessionFactoryRegistry,
                new InternalAddressResolver(schemePortResolver, dnsResolver), timeToLive, timeUnit);
    }

    public PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory,
            final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry,
            final SocketAddressResolver<HttpRoute> socketAddressResolver,
            final long timeToLive, final TimeUnit timeUnit) {
        super();
        Args.notNull(ioReactor, "I/O reactor");
        Args.notNull(ioSessionFactoryRegistry, "I/O session factory registry");
        Args.notNull(socketAddressResolver, "Socket address resolver");
        this.ioReactor = ioReactor;
        this.configData = new ConfigData();
        // 自定义 SocketAddressResolver 可完全接管 route 到 socket 地址的转换；只有使用
        // InternalAddressResolver 的构造路径才遵循本文件末尾的默认端口和 DNS 解析规则。
        this.pool = new CPool(ioReactor,
                new InternalConnectionFactory(this.configData, connFactory),
                socketAddressResolver,
                2, 20, timeToLive, timeUnit != null ? timeUnit : TimeUnit.MILLISECONDS);
        this.ioSessionFactoryRegistry = ioSessionFactoryRegistry;
    }

    PoolingNHttpClientConnectionManager(
            final ConnectingIOReactor ioReactor,
            final CPool pool,
            final Registry<SchemeIOSessionStrategy> ioSessionFactoryRegistry) {
        super();
        this.ioReactor = ioReactor;
        this.configData = new ConfigData();
        this.pool = pool;
        this.ioSessionFactoryRegistry = ioSessionFactoryRegistry;
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            shutdown();
        } finally {
            super.finalize();
        }
    }

    @Override
    public void execute(final IOEventDispatch eventDispatch) throws IOException {
        this.ioReactor.execute(eventDispatch);
    }

    public void shutdown(final long waitMs) throws IOException {
        this.log.debug("Connection manager is shutting down");
        this.pool.shutdown(waitMs);
        this.log.debug("Connection manager shut down");
    }

    @Override
    public void shutdown() throws IOException {
        this.log.debug("Connection manager is shutting down");
        this.pool.shutdown(2000);
        this.log.debug("Connection manager shut down");
    }

    private String format(final HttpRoute route, final Object state) {
        final StringBuilder buf = new StringBuilder();
        buf.append("[route: ").append(route).append("]");
        if (state != null) {
            buf.append("[state: ").append(state).append("]");
        }
        return buf.toString();
    }

    private String formatStats(final HttpRoute route) {
        final StringBuilder buf = new StringBuilder();
        final PoolStats totals = this.pool.getTotalStats();
        final PoolStats stats = this.pool.getStats(route);
        buf.append("[total kept alive: ").append(totals.getAvailable()).append("; ");
        buf.append("route allocated: ").append(stats.getLeased() + stats.getAvailable());
        buf.append(" of ").append(stats.getMax()).append("; ");
        buf.append("total allocated: ").append(totals.getLeased() + totals.getAvailable());
        buf.append(" of ").append(totals.getMax()).append("]");
        return buf.toString();
    }

    private String format(final CPoolEntry entry) {
        final StringBuilder buf = new StringBuilder();
        buf.append("[id: ").append(entry.getId()).append("]");
        buf.append("[route: ").append(entry.getRoute()).append("]");
        final Object state = entry.getState();
        if (state != null) {
            buf.append("[state: ").append(state).append("]");
        }
        return buf.toString();
    }

    /*
     * 普通 exchange 由 AbstractClientExchangeHandler.requestConnection 调用：route 来自
     * 路由规划，state 是 HttpClientContext 的 user token，用于连接亲和性，而不是请求 ID。
     * connectTimeout 约束新建 TCP 连接；leaseTimeout 约束池中尚未分配连接的等待请求。
     * 它们与请求/响应读写阶段的 socket timeout 是不同的控制点。
     */
    @Override
    public Future<NHttpClientConnection> requestConnection(
            final HttpRoute route,
            final Object state,
            final long connectTimeout,
            final long leaseTimeout,
            final TimeUnit timeUnit,
            final FutureCallback<NHttpClientConnection> callback) {
        Args.notNull(route, "HTTP route");
        if (this.log.isDebugEnabled()) {
            this.log.debug("Connection request: " + format(route, state) + formatStats(route));
        }
        final BasicFuture<NHttpClientConnection> resultFuture = new BasicFuture<NHttpClientConnection>(callback);
        // 这里先检查第一跳的传输协议是否受支持；此时尚未证明建连成功，也没有获得 socket 地址。
        final HttpHost host;
        if (route.getProxyHost() != null) {
            host = route.getProxyHost();
        } else {
            host = route.getTargetHost();
        }
        final SchemeIOSessionStrategy sf = this.ioSessionFactoryRegistry.lookup(
                host.getSchemeName());
        if (sf == null) {
            resultFuture.failed(new UnsupportedSchemeException(host.getSchemeName() +
                    " protocol is not supported"));
            return resultFuture;
        }
        /*
         * lease 可以理解为“从池里借一条连接供这次请求使用”。实际选择逻辑在依赖 HttpCore
         * 的 AbstractNIOConnPool.processPendingRequest 中，CPool 继承了这套逻辑：
         *
         * 1. 找到同 route、状态匹配且未关闭/过期的空闲连接：直接借出，不再创建 socket。
         * 2. 没找到，但连接数限制允许创建：解析地址，调用 ioReactor.connect 提交建连任务。
         * 3. 暂时无法借出或创建：申请留在等待队列中，等释放连接等事件重新推进，或等待超时。
         *
         * 第 2 种情况下，DefaultConnectingIOReactor.connect 先把任务入队、唤醒事件循环。
         * Reactor 的 processSessionRequests 才执行 SocketChannel.open/connect；若尚未连好，
         * 再等连接完成事件并执行 finishConnect。连接成功后创建 IOSession，通知池的
         * requestCompleted；池通过下面的 InternalConnectionFactory 创建 HTTP 连接对象。
         * 池随后交付 CPoolEntry，执行这里的 completed(entry)，再把连接传给请求的 handler。
         *
         * HttpCore 4.4.11 的池先找相同 route、匹配 state（或未绑定 state）的空闲 entry，
         * 淘汰关闭/过期项；找不到且容量允许时才解析地址并调用 ioReactor.connect。
         * 复用不重新 DNS 解析，因此即使同一路由的 DNS 已变化，现存连接仍连接原来的 peer。
         *
         * lease 不是强制异步切线程的边界：立即复用可在当前调用栈内完成；排队请求也可能
         * 在归还连接的线程完成；新连接通常在 reactor 工作线程注册 IOSession 时完成。
         * 4.4.11 的 lease/release 复用路径退出自身池锁临界区后调用 fireCallbacks；新连接
         * 则经 requestCompleted -> RouteSpecificPool.completed 在池锁内通知，不能统一假定锁外。
         */
        final Future<CPoolEntry> leaseFuture = this.pool.lease(route, state,
                connectTimeout, leaseTimeout, timeUnit != null ? timeUnit : TimeUnit.MILLISECONDS,
                new FutureCallback<CPoolEntry>() {

                    @Override
                    public void completed(final CPoolEntry entry) {
                        Asserts.check(entry.getConnection() != null, "Pool entry with no connection");
                        if (log.isDebugEnabled()) {
                            log.debug("Connection leased: " + format(entry) + formatStats(entry.getRoute()));
                        }
                        // 即使复用同一个 entry，也为当前租赁创建独立代理；回调拿到的不是池内实体。
                        // CPoolProxy 是管理借出/归还生命周期的 Java 包装对象，与 HTTP 代理服务器无关。
                        final NHttpClientConnection managedConn = CPoolProxy.newProxy(entry);
                        // BasicFuture 先发布结果/唤醒 get()，随后在当前线程直接调用 callback。
                        // 返回 false 表示外层 Future 已终结（例如取消抢先发生），需要归还 entry。
                        // 调用本方法接收的 callback.completed(managedConn)，于是进入
                        // AbstractClientExchangeHandler.connectionAllocated，后续发送便接上了。
                        if (!resultFuture.completed(managedConn)) {
                            pool.release(entry, true);
                        }
                    }

                    @Override
                    public void failed(final Exception ex) {
                        if (log.isDebugEnabled()) {
                            log.debug("Connection request failed", ex);
                        }
                        resultFuture.failed(ex);
                    }

                    @Override
                    public void cancelled() {
                        log.debug("Connection request cancelled");
                        resultFuture.cancel(true);
                    }

                });
        return new Future<NHttpClientConnection>() {

            @Override
            public boolean cancel(final boolean mayInterruptIfRunning) {
                try {
                    leaseFuture.cancel(mayInterruptIfRunning);
                } finally {
                    return resultFuture.cancel(mayInterruptIfRunning);
                }
            }

            @Override
            public boolean isCancelled() {
                return resultFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return resultFuture.isDone();
            }

            @Override
            public NHttpClientConnection get() throws InterruptedException, ExecutionException {
                return resultFuture.get();
            }

            @Override
            public NHttpClientConnection get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return resultFuture.get(timeout, unit);
            }

        };
    }

    @Override
    public void releaseConnection(
            final NHttpClientConnection managedConn,
            final Object state,
            final long keepalive,
            final TimeUnit timeUnit) {
        Args.notNull(managedConn, "Managed connection");
        synchronized (managedConn) {
            // 先切断租赁代理与池项的关联。此后从这个旧代理读取地址、context 等会抛
            // ConnectionShutdownException，即使其真实连接随后被池复用，也不能继续借旧代理访问。
            // 同一代理上的锁与 startRoute/upgrade/routeComplete 协调；重复归还在此直接返回。
            final CPoolEntry entry = CPoolProxy.detach(managedConn);
            if (entry == null) {
                return;
            }
            if (this.log.isDebugEnabled()) {
                this.log.debug("Releasing connection: " + format(entry) + formatStats(entry.getRoute()));
            }
            final NHttpClientConnection conn = entry.getConnection();
            try {
                if (conn.isOpen()) {
                    // state 保存到连接而非请求上，后续租赁据此匹配；默认调用方传入 user token。
                    entry.setState(state);
                    // PoolEntry 的有效期限取“本次归还时间 + keepalive”和创建时 TTL 上限的较小值。
                    // keepalive <= 0 只是本次不增加闲置期限限制，仍不能越过创建时设定的 TTL。
                    entry.updateExpiry(keepalive, timeUnit != null ? timeUnit : TimeUnit.MILLISECONDS);
                    if (this.log.isDebugEnabled()) {
                        final String s;
                        if (keepalive > 0) {
                            s = "for " + (double) keepalive / 1000 + " seconds";
                        } else {
                            s = "indefinitely";
                        }
                        this.log.debug("Connection " + format(entry) + " can be kept alive " + s);
                    }
                }
            } finally {
                // TCP 尚开着仍不充分：未完成 CONNECT/协议分层等路由步骤的连接不能作为成品复用。
                // pool.release 可能立即满足另一个等待者并触发其回调，不只是把 entry 放回队列。
                this.pool.release(entry, conn.isOpen() && entry.isRouteComplete());
                if (this.log.isDebugEnabled()) {
                    this.log.debug("Connection released: " + format(entry) + formatStats(entry.getRoute()));
                }
            }
        }
    }

    private Lookup<SchemeIOSessionStrategy> getIOSessionFactoryRegistry(final HttpContext context) {
        @SuppressWarnings("unchecked")
        Lookup<SchemeIOSessionStrategy> reg = (Lookup<SchemeIOSessionStrategy>) context.getAttribute(
                IOSESSION_FACTORY_REGISTRY);
        if (reg == null) {
            reg = this.ioSessionFactoryRegistry;
        }
        return reg;
    }

    @Override
    public void startRoute(
            final NHttpClientConnection managedConn,
            final HttpRoute route,
            final HttpContext context) throws IOException {
        Args.notNull(managedConn, "Managed connection");
        Args.notNull(route, "HTTP route");
        // lease 已拿到 TCP 连接；此处为第一跳应用协议层。有代理时是代理这一跳，
        // 没有代理时是目标这一跳。默认 HTTP 策略不分层，HTTPS 策略包装现有 IOSession。
        // 因而普通 HTTP 走到本方法时，不需要再次 connect；直连 HTTPS 则在已有连接上启动 TLS。
        final HttpHost host;
        if (route.getProxyHost() != null) {
            host = route.getProxyHost();
        } else {
            host = route.getTargetHost();
        }
        final Lookup<SchemeIOSessionStrategy> reg = getIOSessionFactoryRegistry(context);
        final SchemeIOSessionStrategy sf = reg.lookup(host.getSchemeName());
        if (sf == null) {
            throw new UnsupportedSchemeException(host.getSchemeName() +
                    " protocol is not supported");
        }
        if (sf.isLayeringRequired()) {
            synchronized (managedConn) {
                final CPoolEntry entry = CPoolProxy.getPoolEntry(managedConn);
                final ManagedNHttpClientConnection conn = entry.getConnection();
                final IOSession ioSession = conn.getIOSession();
                final IOSession currentSession = sf.upgrade(host, ioSession);
                conn.bind(currentSession);
            }
        }
    }

    @Override
    public void upgrade(
            final NHttpClientConnection managedConn,
            final HttpRoute route,
            final HttpContext context) throws IOException {
        Args.notNull(managedConn, "Managed connection");
        Args.notNull(route, "HTTP route");
        // 与 startRoute 的第一跳选择不同，协议升级按最终 target 执行。HTTPS 经 HTTP 代理时，
        // MainClientExec 在 CONNECT 成功后走到这里；TLS 面向 target，底层 TCP peer 仍是代理。
        final HttpHost host  = route.getTargetHost();
        final Lookup<SchemeIOSessionStrategy> reg = getIOSessionFactoryRegistry(context);
        final SchemeIOSessionStrategy sf = reg.lookup(host.getSchemeName());
        if (sf == null) {
            throw new UnsupportedSchemeException(host.getSchemeName() +
                    " protocol is not supported");
        }
        if (!sf.isLayeringRequired()) {
            throw new UnsupportedSchemeException(host.getSchemeName() +
                    " protocol does not support connection upgrade");
        }
        synchronized (managedConn) {
            final CPoolEntry entry = CPoolProxy.getPoolEntry(managedConn);
            final ManagedNHttpClientConnection conn = entry.getConnection();
            final IOSession currentSession = sf.upgrade(host, conn.getIOSession());
            conn.bind(currentSession);
        }
    }

    @Override
    public void routeComplete(
            final NHttpClientConnection managedConn,
            final HttpRoute route,
            final HttpContext context) {
        Args.notNull(managedConn, "Managed connection");
        Args.notNull(route, "HTTP route");
        synchronized (managedConn) {
            final CPoolEntry entry = CPoolProxy.getPoolEntry(managedConn);
            // 记录路由步骤已建立，供释放/再次租赁判断。它不表示当前 HTTP 响应已经完成，
            // 也不等价于 TLS 握手已全部结束：非阻塞 TLS 握手由后续 reactor 事件继续推进。
            entry.markRouteComplete();
        }
    }

    @Override
    public boolean isRouteComplete(
            final NHttpClientConnection managedConn) {
        Args.notNull(managedConn, "Managed connection");
        synchronized (managedConn) {
            final CPoolEntry entry = CPoolProxy.getPoolEntry(managedConn);
            return entry.isRouteComplete();
        }
    }

    @Override
    public void closeIdleConnections(final long idleTimeout, final TimeUnit timeUnit) {
        if (this.log.isDebugEnabled()) {
            this.log.debug("Closing connections idle longer than " + idleTimeout + " " + timeUnit);
        }
        this.pool.closeIdle(idleTimeout, timeUnit);
    }

    @Override
    public void closeExpiredConnections() {
        log.debug("Closing expired connections");
        this.pool.closeExpired();
    }

    public void validatePendingRequests() {
        log.debug("Validating pending requests");
        this.pool.validatePendingRequests();
    }

    @Override
    public int getMaxTotal() {
        return this.pool.getMaxTotal();
    }

    @Override
    public void setMaxTotal(final int max) {
        this.pool.setMaxTotal(max);
    }

    @Override
    public int getDefaultMaxPerRoute() {
        return this.pool.getDefaultMaxPerRoute();
    }

    @Override
    public void setDefaultMaxPerRoute(final int max) {
        this.pool.setDefaultMaxPerRoute(max);
    }

    @Override
    public int getMaxPerRoute(final HttpRoute route) {
        return this.pool.getMaxPerRoute(route);
    }

    @Override
    public void setMaxPerRoute(final HttpRoute route, final int max) {
        this.pool.setMaxPerRoute(route, max);
    }

    @Override
    public PoolStats getTotalStats() {
        return this.pool.getTotalStats();
    }

    @Override
    public PoolStats getStats(final HttpRoute route) {
        return this.pool.getStats(route);
    }

    /**
     * @since 4.1
     */
    public Set<HttpRoute> getRoutes() {
        return this.pool.getRoutes();
    }

    public ConnectionConfig getDefaultConnectionConfig() {
        return this.configData.getDefaultConnectionConfig();
    }

    public void setDefaultConnectionConfig(final ConnectionConfig defaultConnectionConfig) {
        this.configData.setDefaultConnectionConfig(defaultConnectionConfig);
    }

    public ConnectionConfig getConnectionConfig(final HttpHost host) {
        return this.configData.getConnectionConfig(host);
    }

    public void setConnectionConfig(final HttpHost host, final ConnectionConfig connectionConfig) {
        this.configData.setConnectionConfig(host, connectionConfig);
    }

    static class ConfigData {

        private final Map<HttpHost, ConnectionConfig> connectionConfigMap;
        private volatile ConnectionConfig defaultConnectionConfig;

        ConfigData() {
            super();
            this.connectionConfigMap = new ConcurrentHashMap<HttpHost, ConnectionConfig>();
        }

        public ConnectionConfig getDefaultConnectionConfig() {
            return this.defaultConnectionConfig;
        }

        public void setDefaultConnectionConfig(final ConnectionConfig defaultConnectionConfig) {
            this.defaultConnectionConfig = defaultConnectionConfig;
        }

        public ConnectionConfig getConnectionConfig(final HttpHost host) {
            return this.connectionConfigMap.get(host);
        }

        public void setConnectionConfig(final HttpHost host, final ConnectionConfig connectionConfig) {
            this.connectionConfigMap.put(host, connectionConfig);
        }

    }

    static class InternalConnectionFactory implements NIOConnFactory<HttpRoute, ManagedNHttpClientConnection> {

        private final ConfigData configData;
        private final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory;

        InternalConnectionFactory(
                final ConfigData configData,
                final NHttpConnectionFactory<ManagedNHttpClientConnection> connFactory) {
            super();
            this.configData = configData != null ? configData : new ConfigData();
            this.connFactory = connFactory != null ? connFactory :
                ManagedNHttpClientConnectionFactory.INSTANCE;
        }

        @Override
        public ManagedNHttpClientConnection create(
                final HttpRoute route, final IOSession ioSession) throws IOException {
            // 新 TCP session 已建立后才进入工厂；连接配置优先取代理，其次 target，再到默认值。
            // 这里选的是编解码/缓冲区等 ConnectionConfig，不会重新决定网络目的地址。
            ConnectionConfig config = null;
            if (route.getProxyHost() != null) {
                config = this.configData.getConnectionConfig(route.getProxyHost());
            }
            if (config == null) {
                config = this.configData.getConnectionConfig(route.getTargetHost());
            }
            if (config == null) {
                config = this.configData.getDefaultConnectionConfig();
            }
            if (config == null) {
                config = ConnectionConfig.DEFAULT;
            }
            final ManagedNHttpClientConnection conn = this.connFactory.create(ioSession, config);
            // reactor 的事件分发通过此键取回真实连接，HttpAsyncRequestExecutor 的连接参数
            // 沿该路径来自此对象；池向 exchange 租出的 CPoolProxy 则委托到同一个对象。
            ioSession.setAttribute(IOEventDispatch.CONNECTION_KEY, conn);
            return conn;
        }

    }

    static class InternalAddressResolver implements SocketAddressResolver<HttpRoute> {

        private final SchemePortResolver schemePortResolver;
        private final DnsResolver dnsResolver;

        public InternalAddressResolver(
                final SchemePortResolver schemePortResolver,
                final DnsResolver dnsResolver) {
            super();
            this.schemePortResolver = schemePortResolver != null ? schemePortResolver :
                DefaultSchemePortResolver.INSTANCE;
            this.dnsResolver = dnsResolver != null ? dnsResolver :
                    SystemDefaultDnsResolver.INSTANCE;
        }

        @Override
        public SocketAddress resolveLocalAddress(final HttpRoute route) throws IOException {
            // 指定本地网卡地址时由系统选择临时源端口；null 表示不显式绑定本地地址。
            return route.getLocalAddress() != null ? new InetSocketAddress(route.getLocalAddress(), 0) : null;
        }

        @Override
        public SocketAddress resolveRemoteAddress(final HttpRoute route) throws IOException {
            // 这是 TCP 第一跳：直连才使用 target，有代理时连 proxy，CONNECT 不会在客户端
            // 再创建一条直达 target 的 socket。这里只生成连接候选地址，成功后的 peer 要从连接读取。
            final HttpHost host;
            // 例：请求 https://example.com/users，代理是 http://proxy.example:8080，
            // 则此处解析 proxy.example 并连接其 8080 端口。example.com:443 留给 CONNECT 请求。
            if (route.getProxyHost() != null) {
                host = route.getProxyHost();
            } else {
                host = route.getTargetHost();
            }
            // 默认 SchemePortResolver 保留显式正端口，否则按 http/https 补 80/443；可注入替换。
            final int port = this.schemePortResolver.resolve(host);
            // 当前实现按 hostName 调用 DnsResolver（默认系统 DNS），没有读取 HttpHost.getAddress()。
            // 数组只取首项，本方法没有依次尝试所有解析地址的回退循环；不能把 DNS 列表视为已连接 peer。
            final InetAddress[] addresses = this.dnsResolver.resolve(host.getHostName());
            return new InetSocketAddress(addresses[0], port);
        }

    }
}
