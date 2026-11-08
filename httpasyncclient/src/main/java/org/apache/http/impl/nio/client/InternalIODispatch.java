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
import java.nio.channels.CancelledKeyException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.impl.nio.DefaultNHttpClientConnection;
import org.apache.http.impl.nio.reactor.AbstractIODispatch;
import org.apache.http.nio.NHttpClientEventHandler;
import org.apache.http.nio.reactor.IOSession;

class InternalIODispatch extends AbstractIODispatch<DefaultNHttpClientConnection> {

    /*
     * 这里是“网络事件”和“请求代码”的接头。client.start() 启动的事件循环调用这些方法，
     * 传入的 conn 就是发生事件的连接。例如：连接刚建好 -> onConnected；可写 -> onOutputReady。
     *
     * 注意源码中两个不同的 handler：
     * - 本类的 handler 是公共网络事件处理器，默认是 HttpCore 的 HttpAsyncRequestExecutor
     *   （开启 debug 时外面再包一层 InternalRequestExecutor）。
     * - execute() 创建的 DefaultClientExchangeHandlerImpl 是某一次请求的处理器，保存在
     *   conn.getContext() 的 HTTP_HANDLER 属性中。公共处理器从连接上取回它，再继续本次请求。
     */

    private final Log log = LogFactory.getLog(InternalIODispatch.class);

    private final NHttpClientEventHandler handler;

    public InternalIODispatch(final NHttpClientEventHandler handler) {
        super();
        if (this.log.isDebugEnabled()) {
            // handler一般默认为HttpAsyncRequestExecutor
            this.handler = new InternalRequestExecutor(this.log, handler);
        } else {
            // handler一般默认为HttpAsyncRequestExecutor
            this.handler = handler;
        }
    }

    @Override
    protected DefaultNHttpClientConnection createConnection(final IOSession session) {
        // This method should never get called under normal circumstances
        // Connection object should be created by the connection manager
        // upon completion of the session request
        log.debug("Unexpected invocation of #createConnection");
        session.close();
        throw new CancelledKeyException();
    }

    @Override
    protected void onConnected(final DefaultNHttpClientConnection conn) {
        final Object attachment = conn.getContext().getAttribute(IOSession.ATTACHMENT_KEY);
        try {
            // 默认执行器的 connected 先初始化这条连接的 HTTP 状态，再直接调用 requestReady(conn)。
            // requestReady 从 conn.context 取出本次请求的 handler，依次调用：
            //   HttpRequest request = exchangeHandler.generateRequest();
            //   conn.submitRequest(request);
            // 前者决定发 GET 还是 CONNECT；后者将请求行/请求头编码到输出缓冲区。
            // 因此新连接的第一轮请求准备可以在这里发生，实际字节写出由后面的输出处理推进。
            this.handler.connected(conn, attachment);
        } catch (final Exception ex) {
            this.handler.exception(conn, ex);
        }
    }

    @Override
    protected void onClosed(final DefaultNHttpClientConnection conn) {
        this.handler.closed(conn);
    }

    @Override
    protected void onException(final DefaultNHttpClientConnection conn, final IOException ex) {
        this.handler.exception(conn, ex);
    }

    @Override
    protected void onInputReady(final DefaultNHttpClientConnection conn) {
        conn.consumeInput(this.handler);
    }

    @Override
    protected void onOutputReady(final DefaultNHttpClientConnection conn) {
        /*
         * requestOutput() 只是开启写事件关注；可写事件到来后，程序执行到这里。
         * conn.produceOutput 的实现在依赖 HttpCore 的 DefaultNHttpClientConnection 中：
         *
         * 1. 连接处于活动状态、没有待生产的请求体且输出缓冲区为空时，调用 requestReady(conn)，
         *    询问当前请求处理器是否有请求可以提交。若 onConnected 已提交请求，则先处理已有数据。
         * 2. 有请求体编码器时，调用 outputReady(conn, encoder)，最终进入用户 producer 的
         *    produceContent，让它分批提供 POST 等请求的正文；正文也可能由编码器直接写出。
         * 3. 输出缓冲区有数据时，执行 outbuf.flush(session.channel())，内部调用 channel.write。
         *    普通 HTTP 此时向 socket 通道写入字节；TLS 连接则经过加密层，握手就绪后传输。
         *
         * 非阻塞写入可能只写出一部分，余下数据由后续事件继续处理。
         * produceOutput 的参数 this.handler 是上面的公共事件处理器，不是某一次请求的 handler。
         */
        conn.produceOutput(this.handler);
    }

    @Override
    protected void onTimeout(final DefaultNHttpClientConnection conn) {
        try {
            this.handler.timeout(conn);
        } catch (final Exception ex) {
            this.handler.exception(conn, ex);
        }
    }

}
