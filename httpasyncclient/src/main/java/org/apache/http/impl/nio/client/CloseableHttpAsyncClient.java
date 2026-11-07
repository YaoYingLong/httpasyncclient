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

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.Future;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.concurrent.BasicFuture;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Args;

/**
 * Base implementation of {@link HttpAsyncClient} that also implements {@link Closeable}.
 *
 * @since 4.0
 */
@Contract(threading = ThreadingBehavior.SAFE)
public abstract class CloseableHttpAsyncClient implements HttpAsyncClient, Closeable {

    /*
     * 这里的 execute 重载负责入口适配，最终汇合到子类的
     * execute(requestProducer, responseConsumer, context, callback)。producer 提供目标、
     * 请求头与异步请求体，consumer 接收响应并生成 T，callback 观察最终结果。
     * 未传 context 的重载为每次调用创建新上下文；显式传入的对象会沿调用链继续使用。
     */

    public abstract boolean isRunning();

    public abstract void start();

    @Override
    public <T> Future<T> execute(
            final HttpAsyncRequestProducer requestProducer,
            final HttpAsyncResponseConsumer<T> responseConsumer,
            final FutureCallback<T> callback) {
        return execute(requestProducer, responseConsumer, HttpClientContext.create(), callback);
    }

    @Override
    public Future<HttpResponse> execute(
            final HttpHost target, final HttpRequest request, final HttpContext context,
            final FutureCallback<HttpResponse> callback) {
        // 将普通 HttpRequest 适配为 producer；默认 consumer 缓冲响应体并返回 HttpResponse。
        // 此时只有逻辑 target，请求还未完成路由选择、租借连接或发送。
        return execute(
                HttpAsyncMethods.create(target, request),
                HttpAsyncMethods.createConsumer(),
                context, callback);
    }

    @Override
    public Future<HttpResponse> execute(
            final HttpHost target, final HttpRequest request,
            final FutureCallback<HttpResponse> callback) {
        return execute(target, request, HttpClientContext.create(), callback);
    }

    @Override
    public Future<HttpResponse> execute(
            final HttpUriRequest request,
            final FutureCallback<HttpResponse> callback) {
        return execute(request, HttpClientContext.create(), callback);
    }

    @Override
    public Future<HttpResponse> execute(
            final HttpUriRequest request,
            final HttpContext context,
            final FutureCallback<HttpResponse> callback) {
        final HttpHost target;
        try {
            target = determineTarget(request);
        } catch (final ClientProtocolException ex) {
            // URI 校验失败直接在当前调用栈完成 Future 并调用 failed 回调；
            // 并非所有异步 API 的回调都会等到 execute 返回或切换到 I/O 线程。
            final BasicFuture<HttpResponse> future = new BasicFuture<HttpResponse>(callback);
            future.failed(ex);
            return future;
        }
        return execute(target, request, context, callback);
    }

    private HttpHost determineTarget(final HttpUriRequest request) throws ClientProtocolException {
        Args.notNull(request, "HTTP request");
        // A null target may be acceptable if there is a default target.
        // Otherwise, the null target is detected in the director.
        HttpHost target = null;

        final URI requestURI = request.getURI();
        // 绝对 URI 的 authority 提供逻辑主机；相对 URI 留下 null 交给后续默认目标处理。
        // 这里不做 DNS 解析，也不观察代理或连接远端，因此不能作为实际 peer 的来源。
        if (requestURI.isAbsolute()) {
            target = URIUtils.extractHost(requestURI);
            if (target == null) {
                throw new ClientProtocolException(
                        "URI does not specify a valid host name: " + requestURI);
            }
        }
        return target;
    }

}
