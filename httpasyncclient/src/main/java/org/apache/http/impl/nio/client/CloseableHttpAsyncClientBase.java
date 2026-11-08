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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.nio.NHttpClientEventHandler;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.util.Asserts;

abstract class CloseableHttpAsyncClientBase extends CloseableHttpPipeliningClient {

    /*
     * 客户端生命周期与每次请求的 handler 生命周期分离。非共享管理器模式拥有一个
     * reactor 驱动线程；默认的多 worker reactor 还会建立自己的 I/O 工作线程。
     * 不能把 reactorThread 当作所有请求回调唯一所在的线程，调用线程的同步准备失败、
     * 取消线程以及 I/O 事件线程都可能触发终态通知。
     */

    private final Log log = LogFactory.getLog(getClass());

    static enum Status {INACTIVE, ACTIVE, STOPPED}

    private final NHttpClientConnectionManager connmgr;
    private final Thread reactorThread;

    private final AtomicReference<Status> status;

    public CloseableHttpAsyncClientBase(
            final NHttpClientConnectionManager connmgr,
            final ThreadFactory threadFactory,
            final NHttpClientEventHandler handler) {
        super();
        this.connmgr = connmgr;
        if (threadFactory != null && handler != null) {
            this.reactorThread = threadFactory.newThread(new Runnable() {

                @Override
                public void run() {
                    try {
                        // handler一般默认为HttpAsyncRequestExecutor
                        final IOEventDispatch ioEventDispatch = new InternalIODispatch(handler);
                        // 阻塞运行管理器的事件循环：InternalIODispatch 把可读/可写事件交给
                        // 连接的 consumeInput/produceOutput，再进入 HttpCore 的事件处理器。
                        // 默认处理器为 HttpAsyncRequestExecutor，按连接上的 handler 驱动请求。
                        connmgr.execute(ioEventDispatch);
                    } catch (final Exception ex) {
                        log.error("I/O reactor terminated abnormally", ex);
                    } finally {
                        status.set(Status.STOPPED);
                    }
                }

            });
        } else {
            // 共享管理器模式由 builder 传入 null；客户端不接管外部 reactor 的启动与关闭。
            this.reactorThread = null;
        }
        this.status = new AtomicReference<Status>(Status.INACTIVE);
    }

    @Override
    public void start() {
        // 应在调用 execute 前启动客户端。这里启动的是持续处理网络事件的后台循环，
        // 后续多次 execute 都由它驱动；每次 execute 不会另外创建一套网络线程。
        // 事件如何进入发送代码，可接着看 InternalIODispatch.onConnected/onOutputReady。
        // CAS 使启动只生效一次；STOPPED 不会重新变为 ACTIVE。
        // 共享模式仍需调用 start，但 ACTIVE 本身不能证明外部 reactor 已在运行。
        if (this.status.compareAndSet(Status.INACTIVE, Status.ACTIVE)) {
            if (this.reactorThread != null) {
                this.reactorThread.start();
            }
        }
    }

    protected void ensureRunning() {
        final Status currentStatus = this.status.get();
        Asserts.check(currentStatus == Status.ACTIVE, "Request cannot be executed; " +
                "I/O reactor status: %s", currentStatus);
    }

    @Override
    public void close() {
        // 只有 ACTIVE -> STOPPED 的调用执行关闭；未 start 的 INACTIVE 实例不会进入此分支。
        // 拥有 reactorThread 才 shutdown 管理器并等待驱动线程退出；共享模式只停止本客户端。
        if (this.status.compareAndSet(Status.ACTIVE, Status.STOPPED)) {
            if (this.reactorThread != null) {
                try {
                    this.connmgr.shutdown();
                } catch (final IOException ex) {
                    this.log.error("I/O error shutting down connection manager", ex);
                }
                try {
                    this.reactorThread.join();
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public boolean isRunning() {
        return this.status.get() == Status.ACTIVE;
    }

}
