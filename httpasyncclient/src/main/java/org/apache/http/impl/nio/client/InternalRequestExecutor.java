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
import org.apache.http.HttpException;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.NHttpClientEventHandler;

// reactor 与 HttpCore 协议状态机之间的日志适配层，不持有某个请求的 producer/Future。
// builder 默认注入的 handler 是依赖 httpcore-nio 的 HttpAsyncRequestExecutor；该类不在
// 本仓库源码中。连接事件由 IOEventDispatch 送到这里，再委托给同一个协议处理器。
// 普通请求主线：requestReady -> HttpCore 从 conn.context[HTTP_HANDLER] 取得 exchange
// -> exchange.generateRequest -> conn.submitRequest。输入/输出内容可在之后多轮事件中推进。
class InternalRequestExecutor implements NHttpClientEventHandler {

    private final Log log;
    private final NHttpClientEventHandler handler;

    public InternalRequestExecutor(final Log log, final NHttpClientEventHandler handler) {
        this.log = log;
        this.handler = handler;
    }

    @Override
    public void connected(
            final NHttpClientConnection conn,
            final Object attachment) throws IOException, HttpException {
        // HttpCore 的 connected 会初始化连接状态并直接调用其 requestReady；不是所有
        // 首次发送都必须经过本适配器的 requestReady 方法，因此观察协议入口要区分这两层。
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + ": Connected");
        }
        this.handler.connected(conn, attachment);
    }

    @Override
    public void closed(final NHttpClientConnection conn) {
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + ": Disconnected");
        }
        this.handler.closed(conn);
    }

    @Override
    public void requestReady(
            final NHttpClientConnection conn) throws IOException, HttpException {
        // conn 是此事件的实际连接，比应用共享 HttpContext 中可变的 HTTP_CONNECTION 更直接。
        // HttpCore 在本次调用栈内执行 exchange.generateRequest；动态线程状态只能覆盖此栈，
        // 不能自动延伸到之后的 outputReady、响应回调或另一个线程的 cancel。
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " Request ready");
        }
        this.handler.requestReady(conn);
    }

    @Override
    public void inputReady(
            final NHttpClientConnection conn,
            final ContentDecoder decoder) throws IOException, HttpException {
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " Input ready");
        }
        this.handler.inputReady(conn, decoder);
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " " + decoder);
        }
    }

    @Override
    public void outputReady(
            final NHttpClientConnection conn,
            final ContentEncoder encoder) throws IOException, HttpException {
        // 内容生产可被重复回调；requestReady 返回之后仍可能有大量 body 尚未发送。
        // 在该线程阻塞等待请求结果会阻止 reactor 处理本线程负责的连接。
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " Output ready");
        }
        this.handler.outputReady(conn, encoder);
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " " + encoder);
        }
    }

    @Override
    public void responseReceived(
            final NHttpClientConnection conn) throws HttpException, IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " Response received");
        }
        this.handler.responseReceived(conn);
    }

    @Override
    public void timeout(final NHttpClientConnection conn) throws HttpException, IOException {
        // 是否失败由 HttpCore 当前协议状态决定：等待 100 Continue 超时可继续发送实体，
        // 普通 socket timeout 则可能使 exchange.failed；不能在日志适配层一概判成请求失败。
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " Timeout");
        }
        this.handler.timeout(conn);
    }

    @Override
    public void exception(final NHttpClientConnection conn, final Exception ex) {
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " Exception", ex);
        }
        this.handler.exception(conn, ex);
    }

    @Override
    public void endOfInput(final NHttpClientConnection conn) throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug(conn + " End of input");
        }
        this.handler.endOfInput(conn);
    }

}
