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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.ProtocolException;
import org.apache.http.auth.AUTH;
import org.apache.http.auth.AuthProtocolState;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthenticationStrategy;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.NonRepeatableRequestException;
import org.apache.http.client.RedirectException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.UserTokenHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.Configurable;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.protocol.RequestClientConnControl;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.conn.routing.BasicRouteDirector;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRouteDirector;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.impl.auth.HttpAuthenticator;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.RequestTargetHost;

// 普通客户端的 HTTP 策略状态机，实例由 builder 创建并供多个 exchange 共用。
// 本类字段只保存策略；每次请求可变状态放在 InternalState 和 handler，不能放进共享 exec。
// prepare 在 execute 调用栈中规划初始请求；其余方法由 exchange 转发协议层事件。
// 一次逻辑请求可经历 CONNECT、认证挑战和重定向；只有 finalResponse 才交给用户 consumer。
class MainClientExec implements InternalClientExec {

    private final Log log = LogFactory.getLog(getClass());

    private final HttpProcessor httpProcessor;
    private final HttpProcessor proxyHttpProcessor;
    private final HttpRoutePlanner routePlanner;
    private final AuthenticationStrategy targetAuthStrategy;
    private final AuthenticationStrategy proxyAuthStrategy;
    private final UserTokenHandler userTokenHandler;
    private final RedirectStrategy redirectStrategy;
    private final HttpRouteDirector routeDirector;
    private final HttpAuthenticator authenticator;

    public MainClientExec(
            final HttpProcessor httpProcessor,
            final HttpRoutePlanner routePlanner,
            final RedirectStrategy redirectStrategy,
            final AuthenticationStrategy targetAuthStrategy,
            final AuthenticationStrategy proxyAuthStrategy,
            final UserTokenHandler userTokenHandler) {
        super();
        this.httpProcessor = httpProcessor;
        this.proxyHttpProcessor = new ImmutableHttpProcessor(
                new RequestTargetHost(), new RequestClientConnControl());
        this.routePlanner = routePlanner;
        this.redirectStrategy = redirectStrategy;
        this.targetAuthStrategy = targetAuthStrategy;
        this.proxyAuthStrategy = proxyAuthStrategy;
        this.userTokenHandler = userTokenHandler;
        this.routeDirector = new BasicRouteDirector();
        this.authenticator = new HttpAuthenticator(log);
    }

    @Override
    public void prepare(
            final HttpHost target,
            final HttpRequest original,
            final InternalState state,
            final AbstractClientExchangeHandler handler) throws HttpException, IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + state.getId() + "] start execution");
        }

        final HttpClientContext localContext = state.getLocalContext();

        // 请求级 RequestConfig 若存在，覆盖先前由 context / client 默认值填充的配置。
        if (original instanceof Configurable) {
            final RequestConfig config = ((Configurable) original).getConfig();
            if (config != null) {
                localContext.setRequestConfig(config);
            }
        }

        final List<URI> redirectLocations = localContext.getRedirectLocations();
        if (redirectLocations != null) {
            redirectLocations.clear();
        }

        // wrapper 承载 URI/header 的执行期改写。target 只是 producer 提供的逻辑目标；
        // routePlanner 可以选代理、补端口或改路由，此时还没有实际连接。
        final HttpRequestWrapper request = HttpRequestWrapper.wrap(original);
        final HttpRoute route = this.routePlanner.determineRoute(target, request, localContext);

        // 关键代码，后续的逻辑中才能获取到
        // route 是“应该怎样到达目标”的计划，包含目标、代理、是否需要隧道/TLS等信息。
        // 这里只保存计划，没有创建连接；start() 接下来会拿这个 route 去连接池申请连接。
        handler.setRoute(route);

        // 初始时，两处都保存用户原本的 GET。mainRequest 留存业务请求，currentRequest 表示
        // “本轮要发的请求”；HTTPS 请求走 HTTP 代理时，它会暂时换成内部创建的 CONNECT。
        state.setMainRequest(request);
        handler.setCurrentRequest(request);

        // 初始请求拦截器也在申请连接之前运行；不能把此时 context 中可能残留的
        // HTTP_CONNECTION 当成本次连接。重定向时同一方法还会再次运行。
        prepareRequest(state, handler);
    }

    @Override
    public HttpRequest generateRequest(
            final InternalState state,
            final AbstractClientExchangeHandler handler) throws IOException, HttpException {

        /*
         * 调用者是 HttpCore 的 HttpAsyncRequestExecutor.requestReady(conn)。它已经拿到连接，
         * 现在调用本方法询问“本轮发什么”，然后执行 conn.submitRequest(本方法返回的请求)。
         *
         * 先看最简单的 GET http://example.com/users：池已建立 TCP -> 下面登记直连路由完成
         * -> 返回准备好的 GET。这里的 CONNECT_TARGET 只是路由步骤名称，不是 HTTP CONNECT。
         *
         * 再看 GET https://example.com/users 经过 HTTP 代理：池已连到代理 -> 第一轮返回
         * CONNECT example.com:443 -> 等代理响应 200 -> 下一轮启动 TLS，再返回原来的 GET。
         * 同一个 handler 因此可以多次来到这里；最初 producer.generateRequest() 提供的 GET
         * 一直保存在 state.mainRequest 中，不必重新创建一个用户 producer。
         */

        final HttpRoute route = handler.getRoute();

        // 已租到 TCP 连接，但“TCP 连通”和“HTTP 路由可用”是两件事。
        // 新 HTTPS 代理连接还需 CONNECT -> 等响应 -> TLS 升级 -> 发送业务请求；
        // 池中已有完整路由的连接可以直接跳过这套建立过程。
        handler.verifytRoute();

        if (!handler.isRouteEstablished()) {
            int step;
            loop:
            do {
                final HttpRoute fact = handler.getActualRoute();
                step = this.routeDirector.nextStep(route, fact);
                switch (step) {
                case HttpRouteDirector.CONNECT_TARGET:
                    handler.onRouteToTarget();
                    break;
                case HttpRouteDirector.CONNECT_PROXY:
                    handler.onRouteToProxy();
                    break;
                case HttpRouteDirector.TUNNEL_TARGET:
                    if (this.log.isDebugEnabled()) {
                        this.log.debug("[exchange: " + state.getId() + "] Tunnel required");
                    }
                    // CONNECT 是本 handler 内部的当前请求，mainRequest 仍保存用户请求。
                    // 本轮先返回 CONNECT，待其响应成功后下一轮 generateRequest 再继续。
                    final HttpRequest connect = createConnectRequest(route, state);
                    handler.setCurrentRequest(HttpRequestWrapper.wrap(connect));
                    // 跳出这个路由循环，让方法往下返回 CONNECT。此处不会阻塞等代理响应；
                    // 代理响应稍后进入 responseReceived，处理结束后再请求下一次输出。
                    break loop;
                case HttpRouteDirector.TUNNEL_PROXY:
                    throw new HttpException("Proxy chains are not supported");
                case HttpRouteDirector.LAYER_PROTOCOL:
                    // CONNECT 成功后才走到这里：给当前连接加上 TLS，不再申请另一条 TCP 连接。
                    handler.onRouteUpgrade();
                    break;
                case HttpRouteDirector.UNREACHABLE:
                    throw new HttpException("Unable to establish route: " +
                            "planned = " + route + "; current = " + fact);
                case HttpRouteDirector.COMPLETE:
                    handler.onRouteComplete();
                    this.log.debug("[exchange: " + state.getId() + "] Connection route established");
                    break;
                default:
                    throw new IllegalStateException("Unknown step indicator "
                            + step + " from RouteDirector.");
                }
            } while (step > HttpRouteDirector.COMPLETE);
        }

        final HttpClientContext localContext = state.getLocalContext();
        HttpRequestWrapper currentRequest = handler.getCurrentRequest();
        if (currentRequest == null) {
            // CONNECT 的 200 响应会把 currentRequest 清空；现在恢复用户原本的 GET /users。
            currentRequest = state.getMainRequest();
            handler.setCurrentRequest(currentRequest);
        }

        if (handler.isRouteEstablished()) {
            // execCount 统计业务请求发送尝试，CONNECT 不在这里计数。
            // 认证/重定向等后续发送复用原 producer；已经开始写过不可重复实体时不能重放。
            state.incrementExecCount();
            if (state.getExecCount() > 1) {
                final HttpAsyncRequestProducer requestProducer = state.getRequestProducer();
                if (!requestProducer.isRepeatable() && state.isRequestContentProduced()) {
                    throw new NonRepeatableRequestException("Cannot retry request " +
                            "with a non-repeatable request entity.");
                }
                requestProducer.resetRequest();
            }
            if (this.log.isDebugEnabled()) {
                this.log.debug("[exchange: " + state.getId() + "] Attempt " + state.getExecCount() +
                    " to execute request");
            }

            if (!currentRequest.containsHeader(AUTH.WWW_AUTH_RESP)) {
                final AuthState targetAuthState = localContext.getTargetAuthState();
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + state.getId() + "] Target auth state: " + targetAuthState.getState());
                }
                this.authenticator.generateAuthResponse(currentRequest, targetAuthState, localContext);
            }
            if (!currentRequest.containsHeader(AUTH.PROXY_AUTH_RESP) && !route.isTunnelled()) {
                final AuthState proxyAuthState = localContext.getProxyAuthState();
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + state.getId() + "] Proxy auth state: " + proxyAuthState.getState());
                }
                this.authenticator.generateAuthResponse(currentRequest, proxyAuthState, localContext);
            }
        } else {
            if (!currentRequest.containsHeader(AUTH.PROXY_AUTH_RESP)) {
                final AuthState proxyAuthState = localContext.getProxyAuthState();
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + state.getId() + "] Proxy auth state: " + proxyAuthState.getState());
                }
                this.authenticator.generateAuthResponse(currentRequest, proxyAuthState, localContext);
            }
        }

        final NHttpClientConnection managedConn = handler.getConnection();
        // 发布的是可变 context 中的连接代理，供本次处理使用，并非地址快照。
        // 用户 producer / interceptor 可重入 execute，甚至复用此 context；完成时连接
        // 又可能已归还，因此“从 context 取到连接”本身不足以证明其归属或可读寿命。
        localContext.setAttribute(HttpCoreContext.HTTP_CONNECTION, managedConn);
        final RequestConfig config = localContext.getRequestConfig();
        if (config.getSocketTimeout() > 0) {
            managedConn.setSocketTimeout(config.getSocketTimeout());
        }
        // 把本轮请求交还 HttpCore；随后 submitRequest 编码请求行和请求头，可写事件负责写出。
        return currentRequest;
    }

    @Override
    public void produceContent(
            final InternalState state,
            final ContentEncoder encoder,
            final IOControl ioControl) throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + state.getId() + "] produce content");
        }
        final HttpAsyncRequestProducer requestProducer = state.getRequestProducer();
        // 标记在调用用户代码前置位；代表已进入内容生产，不保证已写出完整实体。
        // ContentEncoder / IOControl 属于非阻塞增量生产，不能在这里等待同 reactor 的其他请求。
        state.setRequestContentProduced();
        requestProducer.produceContent(encoder, ioControl);
        if (encoder.isCompleted()) {
            requestProducer.resetRequest();
        }
    }

    @Override
    public void requestCompleted(
            final InternalState state,
            final AbstractClientExchangeHandler handler) {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + state.getId() + "] Request completed");
        }
        final HttpClientContext localContext = state.getLocalContext();
        final HttpAsyncRequestProducer requestProducer = state.getRequestProducer();
        // 这是 HttpCore 对当前请求提交/内容生产完成的通知；输出缓冲区仍可能有未刷出的字节。
        // 例如无请求体的 GET，submitRequest 后即可通知这里，实际 flush 可以发生在后面。
        // 因此不能据此认定服务器已收到请求、处理完毕或逻辑 execute 已完成。
        // HttpCore 可在 Expect: 100-continue 阶段收到最终拒绝响应而不调用此方法，
        // 因此它既不是每次响应必经点，也不是可靠的首次连接观测点。
        requestProducer.requestCompleted(localContext);
    }

    @Override
    public void responseReceived(
            final HttpResponse response,
            final InternalState state,
            final AbstractClientExchangeHandler handler) throws IOException, HttpException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + state.getId() + "] Response received " + response.getStatusLine());
        }
        final HttpClientContext context = state.getLocalContext();
        context.setAttribute(HttpCoreContext.HTTP_RESPONSE, response);
        // 先运行响应拦截器，再判断 CONNECT/认证/重定向及是否交付用户 consumer。
        // 这里会进入用户扩展点，context 可被访问；读取连接信息要考虑扩展点之前的归属。
        this.httpProcessor.process(response, context);

        handler.setCurrentResponse(response);

        if (!handler.isRouteEstablished()) {
            final int status = response.getStatusLine().getStatusCode();
            if (status < 200) {
                throw new HttpException("Unexpected response to CONNECT request: " +
                        response.getStatusLine());
            }
            if (status == HttpStatus.SC_OK) {
                // 仅推进到隧道已建立；TLS/业务请求在下一次发送事件完成，不在此递归发送。
                handler.onRouteTunnelToTarget();
                // 隧道已建立，本轮 CONNECT 用完了。下次 generateRequest 先推进 TLS，
                // 再从 mainRequest 取回原始业务请求。下一轮由 handler.responseCompleted
                // 中的 requestOutput() 继续触发，而不是在这里直接递归发送。
                handler.setCurrentRequest(null);
            } else {
                if (!handleConnectResponse(state, handler)) {
                    state.setFinalResponse(response);
                }
            }
        } else {
            if (!handleResponse(state, handler)) {
                state.setFinalResponse(response);
            }
        }
        if (state.getFinalResponse() != null) {
            final HttpAsyncResponseConsumer<?> responseConsumer = state.getResponseConsumer();
            responseConsumer.responseReceived(response);
        }
    }

    @Override
    public void consumeContent(
            final InternalState state,
            final ContentDecoder decoder,
            final IOControl ioControl) throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug("[exchange: " + state.getId() + "] Consume content");
        }
        if (state.getFinalResponse() != null) {
            final HttpAsyncResponseConsumer<?> responseConsumer = state.getResponseConsumer();
            responseConsumer.consumeContent(decoder, ioControl);
        } else {
            // 中间响应（例如 401/302）的实体也必须读完，才能在同一连接上解析下个响应。
            // 这里只增量读入复用的 4 KiB 缓冲并丢弃，不缓存整个中间响应，也不交给用户。
            final ByteBuffer tmpbuf = state.getTmpbuf();
            tmpbuf.clear();
            decoder.read(tmpbuf);
        }
    }

    @Override
    public void responseCompleted(
            final InternalState state,
            final AbstractClientExchangeHandler handler) throws IOException, HttpException {
        final HttpClientContext localContext = state.getLocalContext();
        final HttpResponse currentResponse = handler.getCurrentResponse();

        if (!handler.isRouteEstablished()) {
            final int status = currentResponse.getStatusLine().getStatusCode();
            if (status == HttpStatus.SC_OK) {
                // CONNECT 的成功响应结束不意味着业务响应结束；保留连接推进 TLS/目标请求。
                handler.setCurrentResponse(null);
                return;
            }
        }

        // 决定当前连接是否可继续用，再决定是否继续逻辑请求。
        // 非 keep-alive 不自动意味着 execute 失败：认证/重定向仍可换连接继续。
        final boolean keepAlive = handler.manageConnectionPersistence();
        if (!keepAlive) {
            handler.releaseConnection();
            final AuthState proxyAuthState = localContext.getProxyAuthState();
            if (proxyAuthState.getState() == AuthProtocolState.SUCCESS
                    && proxyAuthState.getAuthScheme() != null
                    && proxyAuthState.getAuthScheme().isConnectionBased()) {
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + state.getId() + "] Resetting proxy auth state");
                }
                proxyAuthState.reset();
            }
            final AuthState targetAuthState = localContext.getTargetAuthState();
            if (targetAuthState.getState() == AuthProtocolState.SUCCESS
                    && targetAuthState.getAuthScheme() != null
                    && targetAuthState.getAuthScheme().isConnectionBased()) {
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + state.getId() + "] Resetting target auth state");
                }
                targetAuthState.reset();
            }
        }

        Object userToken = localContext.getUserToken();
        if (userToken == null) {
            userToken = this.userTokenHandler.getUserToken(localContext);
            localContext.setAttribute(HttpClientContext.USER_TOKEN, userToken);
        }

        if (state.getFinalResponse() != null) {
            final HttpAsyncResponseConsumer<?> responseConsumer = state.getResponseConsumer();
            // consumer 在此将累计实体转换成最终结果，Default handler 随后才完成 Future。
            // 非 keep-alive 分支可能已先归还连接；即使本分支仍持有连接，也会在 callback 前释放。
            responseConsumer.responseCompleted(localContext);
            if (this.log.isDebugEnabled()) {
                this.log.debug("[exchange: " + state.getId() + "] Response processed");
            }
            handler.releaseConnection();
        } else {
            if (state.getRedirect() != null) {
                // 此时上个响应体已读完，才切换请求与路由。一个 execute 的 producer/Future
                // 保持不变，所以仅在最初 producer.generateRequest 观察不到后续目的地。
                final HttpUriRequest redirect = state.getRedirect();
                final URI uri = redirect.getURI();
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + state.getId() + "] Redirecting to '" + uri + "'");
                }
                state.setRedirect(null);

                final HttpHost newTarget = URIUtils.extractHost(uri);
                if (newTarget == null) {
                    throw new ProtocolException("Redirect URI does not specify a valid host name: " + uri);
                }

                // Reset auth states if redirecting to another host
                final HttpRoute route = handler.getRoute();
                if (!route.getTargetHost().equals(newTarget)) {
                    final AuthState targetAuthState = localContext.getTargetAuthState();
                    if (this.log.isDebugEnabled()) {
                        this.log.debug("[exchange: " + state.getId() + "] Resetting target auth state");
                    }
                    targetAuthState.reset();
                    final AuthState proxyAuthState = localContext.getProxyAuthState();
                    final AuthScheme authScheme = proxyAuthState.getAuthScheme();
                    if (authScheme != null && authScheme.isConnectionBased()) {
                        if (this.log.isDebugEnabled()) {
                            this.log.debug("[exchange: " + state.getId() + "] Resetting proxy auth state");
                        }
                        proxyAuthState.reset();
                    }
                }

                if (!redirect.headerIterator().hasNext()) {
                    final HttpRequest original = state.getMainRequest().getOriginal();
                    redirect.setHeaders(original.getAllHeaders());
                }

                final HttpRequestWrapper newRequest = HttpRequestWrapper.wrap(redirect);
                final HttpRoute newRoute = this.routePlanner.determineRoute(
                    newTarget, newRequest, localContext);
                // route 包含代理/隧道/本地绑定等信息，比较完整 route，而不是只比较目标域名。
                // 不同 route 释放旧连接；同 route 留给 handler 决定是否可继续发送。
                if (!route.equals(newRoute)) {
                    handler.releaseConnection();
                }
                handler.setRoute(newRoute);
                handler.setCurrentRequest(newRequest);
                state.setMainRequest(newRequest);
                prepareRequest(state, handler);
            }
        }
        handler.setCurrentResponse(null);
    }

    private void rewriteRequestURI(
            final HttpRequestWrapper request,
            final HttpRoute route) throws ProtocolException {
        try {
            URI uri = request.getURI();
            if (uri != null) {
                // 普通 HTTP 代理发送 absolute-form；直连或已建立隧道发送 origin-form。
                // 请求行的 URI 形态由传输路线决定，不能据它反推 TCP 对端是谁。
                if (route.getProxyHost() != null && !route.isTunnelled()) {
                    // Make sure the request URI is absolute
                    if (!uri.isAbsolute()) {
                        final HttpHost target = route.getTargetHost();
                        uri = URIUtils.rewriteURI(uri, target, true);
                    } else {
                        uri = URIUtils.rewriteURI(uri);
                    }
                } else {
                    // Make sure the request URI is relative
                    if (uri.isAbsolute()) {
                        uri = URIUtils.rewriteURI(uri, null, true);
                    } else {
                        uri = URIUtils.rewriteURI(uri);
                    }
                }
                request.setURI(uri);
            }
        } catch (final URISyntaxException ex) {
            throw new ProtocolException("Invalid URI: " +
                    request.getRequestLine().getUri(), ex);
        }
    }

    private void prepareRequest(
            final InternalState state,
            final AbstractClientExchangeHandler handler) throws IOException, HttpException {
        final HttpClientContext localContext = state.getLocalContext();
        final HttpRequestWrapper currentRequest = handler.getCurrentRequest();
        final HttpRoute route = handler.getRoute();

        final HttpRequest original = currentRequest.getOriginal();
        URI uri = null;
        if (original instanceof HttpUriRequest) {
            uri = ((HttpUriRequest) original).getURI();
        } else {
            final String uriString = original.getRequestLine().getUri();
            try {
                uri = URI.create(uriString);
            } catch (final IllegalArgumentException ex) {
                if (this.log.isDebugEnabled()) {
                    this.log.debug("[exchange: " + state.getId() + "] Unable to parse '" + uriString +
                            "' as a valid URI; request URI and Host header may be inconsistent", ex);
                }
            }

        }
        currentRequest.setURI(uri);

        // Re-write request URI if needed
        rewriteRequestURI(currentRequest, route);

        // 拦截器中的 HTTP_TARGET_HOST 优先表示原始绝对 URI 的逻辑目标，可能不同于
        // routePlanner 决定的连接目的地。Host/URL、route、socket peer 是不同层次的信息。
        HttpHost target = null;
        if (uri != null && uri.isAbsolute() && uri.getHost() != null) {
            target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
        }
        if (target == null) {
            target = route.getTargetHost();
        }

        // Get user info from the URI
        if (uri != null) {
            final String userinfo = uri.getUserInfo();
            if (userinfo != null) {
                final CredentialsProvider credsProvider = localContext.getCredentialsProvider();
                credsProvider.setCredentials(
                        new AuthScope(target),
                        new UsernamePasswordCredentials(userinfo));
            }
        }

        localContext.setAttribute(HttpCoreContext.HTTP_REQUEST, currentRequest);
        localContext.setAttribute(HttpCoreContext.HTTP_TARGET_HOST, target);
        localContext.setAttribute(HttpClientContext.HTTP_ROUTE, route);
        this.httpProcessor.process(currentRequest, localContext);
    }

    private HttpRequest createConnectRequest(
            final HttpRoute route, final InternalState state) throws IOException, HttpException {
        // 发给代理的请求行是 CONNECT targetHost:targetPort；这里的端口描述隧道目标，
        // 不描述已经连上的代理端口。只走 proxyHttpProcessor，不生成第二个用户 producer。
        // see RFC 2817, section 5.2 and
        // INTERNET-DRAFT: Tunneling TCP based protocols through
        // Web proxy servers
        final HttpHost target = route.getTargetHost();
        final String host = target.getHostName();
        final int port = target.getPort();
        final StringBuilder buffer = new StringBuilder(host.length() + 6);
        buffer.append(host);
        buffer.append(':');
        buffer.append(Integer.toString(port));
        final HttpRequest request = new BasicHttpRequest("CONNECT", buffer.toString(), HttpVersion.HTTP_1_1);
        final HttpClientContext localContext = state.getLocalContext();
        this.proxyHttpProcessor.process(request, localContext);
        return request;
    }

    private boolean handleConnectResponse(
            final InternalState state,
            final AbstractClientExchangeHandler handler) {
        final HttpClientContext localContext = state.getLocalContext();
        final RequestConfig config = localContext.getRequestConfig();
        if (config.isAuthenticationEnabled()) {
            final CredentialsProvider credsProvider = localContext.getCredentialsProvider();
            if (credsProvider != null) {
                final HttpRoute route = handler.getRoute();
                final HttpHost proxy = route.getProxyHost();
                final HttpResponse currentResponse = handler.getCurrentResponse();
                final AuthState proxyAuthState = localContext.getProxyAuthState();
                if (this.authenticator.isAuthenticationRequested(proxy, currentResponse,
                        this.proxyAuthStrategy, proxyAuthState, localContext)) {
                    return this.authenticator.handleAuthChallenge(proxy, currentResponse,
                            this.proxyAuthStrategy, proxyAuthState, localContext);
                }
            }
        }
        return false;
    }

    private boolean handleResponse(
            final InternalState state,
            final AbstractClientExchangeHandler handler) throws HttpException {
        final HttpClientContext localContext = state.getLocalContext();
        final RequestConfig config = localContext.getRequestConfig();
        // true 表示已安排后续交换，当前响应不是 finalResponse；认证优先于重定向。
        // false 才将响应交给用户 consumer。这不是依据 HTTP 状态码简单判定成功/失败。
        if (config.isAuthenticationEnabled()) {
            if (needAuthentication(state, handler)) {
                // discard previous auth headers
                final HttpRequestWrapper currentRequest = handler.getCurrentRequest();
                final HttpRequest original = currentRequest.getOriginal();
                if (!original.containsHeader(AUTH.WWW_AUTH_RESP)) {
                    currentRequest.removeHeaders(AUTH.WWW_AUTH_RESP);
                }
                if (!original.containsHeader(AUTH.PROXY_AUTH_RESP)) {
                    currentRequest.removeHeaders(AUTH.PROXY_AUTH_RESP);
                }
                return true;
            }
        }
        if (config.isRedirectsEnabled()) {
            final HttpRequestWrapper currentRequest = handler.getCurrentRequest();
            final HttpResponse currentResponse = handler.getCurrentResponse();
            if (this.redirectStrategy.isRedirected(currentRequest, currentResponse, localContext)) {
                final int maxRedirects = config.getMaxRedirects() >= 0 ? config.getMaxRedirects() : 100;
                if (state.getRedirectCount() >= maxRedirects) {
                    throw new RedirectException("Maximum redirects (" + maxRedirects + ") exceeded");
                }
                state.incrementRedirectCount();
                final HttpUriRequest redirect = this.redirectStrategy.getRedirect(currentRequest.getOriginal(), currentResponse,
                    localContext);
                state.setRedirect(redirect);
                return true;
            }
        }
        return false;
    }

    private boolean needAuthentication(
            final InternalState state,
            final AbstractClientExchangeHandler handler) {
        final HttpClientContext localContext = state.getLocalContext();
        final CredentialsProvider credsProvider = localContext.getCredentialsProvider();
        if (credsProvider != null) {
            final HttpRoute route = handler.getRoute();
            final HttpResponse currentResponse = handler.getCurrentResponse();
            HttpHost target = localContext.getTargetHost();
            if (target == null) {
                target = route.getTargetHost();
            }
            if (target.getPort() < 0) {
                target = new HttpHost(
                        target.getHostName(),
                        route.getTargetHost().getPort(),
                        target.getSchemeName());
            }
            final AuthState targetAuthState = localContext.getTargetAuthState();
            final AuthState proxyAuthState = localContext.getProxyAuthState();

            final boolean targetAuthRequested = this.authenticator.isAuthenticationRequested(
                    target, currentResponse, this.targetAuthStrategy, targetAuthState, localContext);

            HttpHost proxy = route.getProxyHost();
            // if proxy is not set use target host instead
            if (proxy == null) {
                proxy = route.getTargetHost();
            }
            final boolean proxyAuthRequested = this.authenticator.isAuthenticationRequested(
                    proxy, currentResponse, this.proxyAuthStrategy, proxyAuthState, localContext);

            if (targetAuthRequested) {
                return this.authenticator.handleAuthChallenge(target, currentResponse,
                        this.targetAuthStrategy, targetAuthState, localContext);
            }
            if (proxyAuthRequested) {
                return this.authenticator.handleAuthChallenge(proxy, currentResponse,
                        this.proxyAuthStrategy, proxyAuthState, localContext);
            }
        }
        return false;
    }

}
