package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ResponseFuture;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerMethodModel;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.rpc.support.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 *
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestClientFilter.class);
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

       // LOGGER.info("filter start resultstr:{},result:{},invoker:{},invocation:{}",result.toString(),result.getClass(),invoker.getClass(),invocation.getClass());
        String key = invoker.getUrl().getHost().split("-")[1];
        AtomicInteger target = UserLoadBalance.taskMap.get(key);
        if (target != null && target.get() <= 1) {
            CompletableFuture<Object> rcompletableFuture = CompletableFuture.supplyAsync(() -> {
//                try {
//                    while (true) {
//                        int taskCount = target.get();
//                        if (taskCount < max) {
//                            target.compareAndSet(taskCount, taskCount + 1);
//                            break;
//                        }
//                        Thread.sleep(100);
//                    }
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                while (target.get() <= 1) {

                }
                target.decrementAndGet();
                UserLoadBalance.totalWeight.decrementAndGet();
                SimpleAsyncRpcResult simpleAsyncRpcResult = (SimpleAsyncRpcResult) invoker.invoke(invocation);
                Integer value = null;

                try {
                    value = (Integer) simpleAsyncRpcResult.getValueFuture().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                int t = target.incrementAndGet();
                UserLoadBalance.totalWeight.incrementAndGet();
             //   LOGGER.info("asyncResult value:{},taskCount", value, t);
                return value;
            });
            RpcContext.getContext().setFuture(rcompletableFuture);
            SimpleAsyncRpcResult asyncRpcResult = new SimpleAsyncRpcResult(rcompletableFuture, false);
            //LOGGER.info("asyncResult return");
            return asyncRpcResult;
        }
        int nowTaskCount = 0;
        if (target != null) {
            nowTaskCount = target.decrementAndGet();
        }
        UserLoadBalance.totalWeight.decrementAndGet();
     // LOGGER.info("syn return,task count:{},key:{}",nowTaskCount,key);
        return invoker.invoke(invocation);

    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String key = invoker.getUrl().getHost().split("-")[1];
        AtomicInteger target = UserLoadBalance.taskMap.get(key);
        int nowTaskCount = target.incrementAndGet();
        UserLoadBalance.totalWeight.incrementAndGet();
        //LOGGER.info("onResponse key:{},taskCount:{},result:{}",key,nowTaskCount,result.getValue());
        return result;
    }

}
