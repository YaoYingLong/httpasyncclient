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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.concurrent.Cancellable;

final class FutureWrapper<T> implements Future<T> {

    // future（客户端传入 BasicFuture）保存结果；cancellable 是处理连接与资源的 handler。
    // 这个包装层把取消资源与发布结果连起来，本身不调度线程，也不持有正在执行的任务线程。

    private final Future<T> future;
    private final Cancellable cancellable;

    public FutureWrapper(final Future<T> future, final Cancellable cancellable) {
        super();
        this.future = future;
        this.cancellable = cancellable;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        // 先让 handler 取消 consumer、释放/丢弃连接和关闭资源；它也可能已将结果 Future
        // 设为取消、失败，甚至用 consumer 已有的结果完成。因此最终返回值不是 handler 的
        // cancel 返回值：finally 中第二次 future.cancel 可能返回 false，而 isCancelled 为 true。
        // 当前 BasicFuture 不用 mayInterruptIfRunning 中断 reactor；取消依赖 handler 的资源操作。
        try {
            if (cancellable != null) {
                cancellable.cancel();
            }
        } finally {
            // return 位于 finally：即使 handler.cancel 抛异常也会尝试取消结果 Future；
            // 若这次调用正常返回，它的返回值还会覆盖前面取消资源时抛出的异常。
            return future.cancel(mayInterruptIfRunning);
        }
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        // 等待超时只结束这一次 get 等待，不会自动调用 handler.cancel 或终止网络请求。
        return future.get(timeout, unit);
    }

    @Override
    public String toString() {
        return future.toString();
    }

}
