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

import javax.net.ssl.SSLSession;

import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.impl.conn.ConnectionShutdownException;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.ManagedNHttpClientConnection;
import org.apache.http.nio.reactor.IOSession;
import org.apache.http.protocol.HttpContext;

class CPoolProxy implements ManagedNHttpClientConnection {

    /*
     * 一次租赁的访问句柄，不拥有独立 socket。CPoolEntry/真实连接可跨多个 exchange 复用，
     * 但本代理在 releaseConnection 中 detach 后永久失效；下次租赁会创建另一个代理。
     * volatile 让其他线程看见解绑，不会把“查 entry + 使用真实连接”组合成原子操作。
     */
    private volatile CPoolEntry poolEntry;

    CPoolProxy(final CPoolEntry entry) {
        super();
        this.poolEntry = entry;
    }

    CPoolEntry getPoolEntry() {
        return this.poolEntry;
    }

    CPoolEntry detach() {
        // 管理器在 synchronized(managedConn) 内调用，串行化同一代理的重复释放及路由变更。
        // 此处仅移交池项，是否关闭或复用真实连接由管理器随后决定。
        final CPoolEntry local = this.poolEntry;
        this.poolEntry = null;
        return local;
    }

    ManagedNHttpClientConnection getConnection() {
        final CPoolEntry local = this.poolEntry;
        if (local == null) {
            return null;
        }
        return local.getConnection();
    }

    ManagedNHttpClientConnection getValidConnection() {
        final ManagedNHttpClientConnection conn = getConnection();
        // “有效”指代理仍附着于池项，不是额外的 socket 存活检测；失效代理直接拒绝访问。
        if (conn == null) {
            throw new ConnectionShutdownException();
        }
        return conn;
    }

    @Override
    public void close() throws IOException {
        // close/shutdown 操作的是底层连接，但不会 detach 或归还 entry；租赁释放仍由管理器负责。
        final CPoolEntry local = this.poolEntry;
        if (local != null) {
            local.closeConnection();
        }
    }

    @Override
    public void shutdown() throws IOException {
        final CPoolEntry local = this.poolEntry;
        if (local != null) {
            local.shutdownConnection();
        }
    }

    @Override
    public HttpConnectionMetrics getMetrics() {
        return getValidConnection().getMetrics();
    }

    @Override
    public void requestInput() {
        final NHttpClientConnection conn = getConnection();
        if (conn != null) {
            conn.requestInput();
        }
    }

    @Override
    public void suspendInput() {
        final NHttpClientConnection conn = getConnection();
        if (conn != null) {
            conn.suspendInput();
        }
    }

    @Override
    public void requestOutput() {
        final NHttpClientConnection conn = getConnection();
        if (conn != null) {
            conn.requestOutput();
        }
    }

    @Override
    public void suspendOutput() {
        final NHttpClientConnection conn = getConnection();
        if (conn != null) {
            conn.suspendOutput();
        }
    }

    @Override
    public InetAddress getLocalAddress() {
        return getValidConnection().getLocalAddress();
    }

    @Override
    public int getLocalPort() {
        return getValidConnection().getLocalPort();
    }

    @Override
    public InetAddress getRemoteAddress() {
        // 委托真实连接的 HttpInetConnection 视图，不从 route/HttpHost 推导。
        // release 后即使 context 或用户代码还持有本代理，这条路径也会因 detach 而抛异常。
        return getValidConnection().getRemoteAddress();
    }

    @Override
    public int getRemotePort() {
        return getValidConnection().getRemotePort();
    }

    @Override
    public boolean isOpen() {
        final CPoolEntry local = this.poolEntry;
        return local != null ? !local.isClosed() : false;
    }

    @Override
    public boolean isStale() {
        final NHttpClientConnection conn = getConnection();
        return conn != null ? !conn.isOpen() : false;
    }

    @Override
    public void setSocketTimeout(final int i) {
        getValidConnection().setSocketTimeout(i);
    }

    @Override
    public int getSocketTimeout() {
        return getValidConnection().getSocketTimeout();
    }

    @Override
    public void submitRequest(final HttpRequest request) throws IOException, HttpException {
        getValidConnection().submitRequest(request);
    }

    @Override
    public boolean isRequestSubmitted() {
        return getValidConnection().isRequestSubmitted();
    }

    @Override
    public void resetOutput() {
        getValidConnection().resetOutput();
    }

    @Override
    public void resetInput() {
        getValidConnection().resetInput();
    }

    @Override
    public int getStatus() {
        return getValidConnection().getStatus();
    }

    @Override
    public HttpRequest getHttpRequest() {
        return getValidConnection().getHttpRequest();
    }

    @Override
    public HttpResponse getHttpResponse() {
        return getValidConnection().getHttpResponse();
    }

    @Override
    public HttpContext getContext() {
        return getValidConnection().getContext();
    }

    public static NHttpClientConnection newProxy(final CPoolEntry poolEntry) {
        // 代理的生命周期比连接短：不能用代理对象的长期存活推断其租赁仍然有效。
        return new CPoolProxy(poolEntry);
    }

    private static CPoolProxy getProxy(final NHttpClientConnection conn) {
        if (!CPoolProxy.class.isInstance(conn)) {
            throw new IllegalStateException("Unexpected connection proxy class: " + conn.getClass());
        }
        return CPoolProxy.class.cast(conn);
    }

    public static CPoolEntry getPoolEntry(final NHttpClientConnection proxy) {
        final CPoolEntry entry = getProxy(proxy).getPoolEntry();
        if (entry == null) {
            throw new ConnectionShutdownException();
        }
        return entry;
    }

    public static CPoolEntry detach(final NHttpClientConnection proxy) {
        return getProxy(proxy).detach();
    }

    @Override
    public String getId() {
        return getValidConnection().getId();
    }

    @Override
    public void bind(final IOSession ioSession) {
        getValidConnection().bind(ioSession);
    }

    @Override
    public IOSession getIOSession() {
        return getValidConnection().getIOSession();
    }

    @Override
    public SSLSession getSSLSession() {
        return getValidConnection().getSSLSession();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CPoolProxy{");
        final ManagedNHttpClientConnection conn = getConnection();
        if (conn != null) {
            sb.append(conn);
        } else {
            sb.append("detached");
        }
        sb.append('}');
        return sb.toString();
    }

}
