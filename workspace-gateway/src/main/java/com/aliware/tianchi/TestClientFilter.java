package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerMethodModel;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
        try{
            //Result result = invoker.invoke(invocation);
            return postProcessResult(null,invoker,invocation);
        }catch (RpcException e){
            throw e;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
            return invoker.invoke(invocation);
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {return result;
    }

    private static Result postProcessResult(Result result, Invoker<?> invoker, Invocation invocation) throws ExecutionException, InterruptedException {
        CompletableFuture<Result> completableFuture = CompletableFuture.supplyAsync(()->{
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("run end ...");
            return invoker.invoke(invocation);
        });
//        AsyncRpcResult asyncRpcResult = new AsyncRpcResult(completableFuture);
//        return  asyncRpcResult;
        Result r = completableFuture.get();
        System.out.println(new Date().toString()+r.getValue());
        return invoker.invoke(invocation);
    }
//    public static void main(String[] args) {
//        Result result = new RpcResult();
//        result = postProcessResult(result,null,null);
//        Object value = result.getValue();
//        return;
//    }
    private void fireInvokeCallback(final Invoker<?> invoker, final Invocation invocation) {
        final ConsumerMethodModel.AsyncMethodInfo asyncMethodInfo = getAsyncMethodInfo(invoker, invocation);
        if (asyncMethodInfo == null) {
            return;
        }

        // 获取事件配置信息
        final Method onInvokeMethod = asyncMethodInfo.getOninvokeMethod();
        final Object onInvokeInst = asyncMethodInfo.getOninvokeInstance();

        if (onInvokeMethod == null && onInvokeInst == null) {
            return;
        }
        if (onInvokeMethod == null || onInvokeInst == null) {
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() + " has a oninvoke callback config , but no such " + (onInvokeMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        if (!onInvokeMethod.isAccessible()) {
            onInvokeMethod.setAccessible(true);
        }

        // 获取方法参数
        Object[] params = invocation.getArguments();
        try {
            // 触发oninvoke事件
            onInvokeMethod.invoke(onInvokeInst, params);
        } catch (InvocationTargetException e) {
            // 触发onthrow事件
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }

    private ConsumerMethodModel.AsyncMethodInfo getAsyncMethodInfo(Invoker<?> invoker, Invocation invocation) {
        // 首先获取消费者信息
        final ConsumerModel consumerModel = ApplicationModel.getConsumerModel(invoker.getUrl().getServiceKey());
        if (consumerModel == null) {
            return null;
        }

        // 获取消费者对应的方法信息
        ConsumerMethodModel methodModel = consumerModel.getMethodModel(invocation.getMethodName());
        if (methodModel == null) {
            return null;
        }

        // 获取消费者对应方法的事件信息，即是否有配置事件通知
        final ConsumerMethodModel.AsyncMethodInfo asyncMethodInfo = methodModel.getAsyncInfo();
        if (asyncMethodInfo == null) {
            return null;
        }
        return asyncMethodInfo;
    }



    private void fireThrowCallback(Invoker<?> invoker, Invocation invocation, Object e){
        LOGGER.info("error!");
    }

    private void f1(Invoker<?> invoker, Invocation invocation){
        LOGGER.info("AsyResult!");
    }
}
