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

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.ManagedNHttpClientConnection;
import org.apache.http.nio.pool.AbstractNIOConnPool;
import org.apache.http.nio.pool.NIOConnFactory;
import org.apache.http.nio.pool.SocketAddressResolver;
import org.apache.http.nio.reactor.ConnectingIOReactor;

@Contract(threading = ThreadingBehavior.SAFE)
class CPool extends AbstractNIOConnPool<HttpRoute, ManagedNHttpClientConnection, CPoolEntry> {

    /*
     * 复用算法在 HttpCore 的 AbstractNIOConnPool 中：按 HttpRoute 建子池，以 state 匹配
     * 空闲连接，并用池锁保护 leased/available/pending 等集合；本类只补充 entry 与超时策略。
     * 达到容量上限时租赁可能排队，释放连接可推进等待者，所以“异步租赁”不保证另起线程。
     */
    private final Log log = LogFactory.getLog(CPool.class);

    private final long timeToLive;
    private final TimeUnit timeUnit;

    public CPool(
            final ConnectingIOReactor ioReactor,
            final NIOConnFactory<HttpRoute, ManagedNHttpClientConnection> connFactory,
            final SocketAddressResolver<HttpRoute> addressResolver,
            final int defaultMaxPerRoute, final int maxTotal,
            final long timeToLive, final TimeUnit timeUnit) {
        super(ioReactor, connFactory, addressResolver, defaultMaxPerRoute, maxTotal);
        this.timeToLive = timeToLive;
        this.timeUnit = timeUnit;
    }

    @Override
    protected CPoolEntry createEntry(final HttpRoute route, final ManagedNHttpClientConnection conn) {
        // entry 与真实连接一起创建，TTL 从此时起算；后续复用不重建 entry，也不会重置 TTL。
        final CPoolEntry entry =  new CPoolEntry(this.log, conn.getId(), route, conn, this.timeToLive, this.timeUnit);
        // 保存新连接的基线 socket timeout；单次请求配置可能另行覆盖真实连接上的值。
        entry.setSocketTimeout(conn.getSocketTimeout());
        return entry;
    }

    @Override
    protected void onLease(final CPoolEntry entry) {
        final NHttpClientConnection conn = entry.getConnection();
        // 重新租出时恢复基线，避免把上一请求临时设置的 timeout 当成下一请求的默认值。
        conn.setSocketTimeout(entry.getSocketTimeout());
    }

    @Override
    protected void onRelease(final CPoolEntry entry) {
        final NHttpClientConnection conn = entry.getConnection();
        // 可复用连接回池后关闭 I/O 空闲超时；keep-alive/TTL 的淘汰由池的 expiry 管理，
        // 与 selector 上针对读写的 socket timeout 分开，避免闲置池连接沿用业务请求超时。
        conn.setSocketTimeout(0);
    }

}
