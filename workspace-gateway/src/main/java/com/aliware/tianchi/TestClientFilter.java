package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
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
    //正式提交成绩时关闭日志打印能大幅降低响应时间，需要获取一些关键信息时可以开启
    private static final Logger LOGGER = LoggerFactory.getLogger(TestClientFilter.class);
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
       // LOGGER.info("filter start resultstr:{},result:{},invoker:{},invocation:{}",result.toString(),result.getClass(),invoker.getClass(),invocation.getClass());
        String key = invoker.getUrl().getHost().split("-")[1];
        AtomicInteger target = UserLoadBalance.weightMap.get(key);
        //当前统计的权重小于等于1时异步发送请求
        if (target != null && target.get() <= 1) {
            //java8的新特性之一，异步处理类CompletableFuture，匿名函数里是真正的异步执行逻辑
            CompletableFuture<Object> rcompletableFuture = CompletableFuture.supplyAsync(() -> {
                //目前策略是让对象阻塞到有可用线程（权重）
                while (target.get() <= 1) {

                }
                /**
                 * 阻塞结束进行发送，总权重统计值减一，这样做还没有真正解决并发冲突，正规的做法是使用
                 *   AtomicInteger的compareAndSet()方法，将判断和设置两个操作变成一个原子操作，失败则
                 *   重新获取当前AtomicInteger值后再compareAndSet()一直到成功，也就是CAS机制
                 */
                target.decrementAndGet();
                /**
                 * 自身权重也减一，正规做法也要使用cas而且总权重和自身权重有一个cas失败就都要进行重试，
                 * 没有想出太好的解决办法，或许只能上锁，但负载算法优化后发送异步请求的情况比较少，上锁
                 * 也可以接受
                 */
                UserLoadBalance.totalWeight.decrementAndGet();
                /**
                 * dubbo的异步结果对象，内部封装了发送请求的方法，事实上直接进行invoker.invoke
                 * 请求也是异步的，只不过他会直接发送请求，不能定制，要进行我们前面的阻塞逻辑得
                 * 自己创建CompletableFuture，然后在里面进行invoker.invoke
                 */
                SimpleAsyncRpcResult simpleAsyncRpcResult = (SimpleAsyncRpcResult) invoker.invoke(invocation);
                Integer value = null;

                try {
                    /**
                     * 测试程序里通过RpcContext.getContext().getFuture()获取到保存异步结果的future，
                     * 见HttpProcessHandler 64行，而invoker.invoke中将valueFuture暂存在RpcContext，
                     * 见DubboInvoker 96行，故我们的CompletableFuture要伪装成valueFuture，也就是阻
                     * 塞获取valueFuture的返回结果，并把它当作自己的返回
                     */
                    value = (Integer) simpleAsyncRpcResult.getValueFuture().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                /**
                 *  获取返回后执行统计，同上面的这里也是没有解决并发冲突,不确定onResponse是
                 *  否也会进行异步请求的返回处理（理论上会），如果是则这一步没有必要
                 */
                int t = target.incrementAndGet();
                UserLoadBalance.totalWeight.incrementAndGet();
                LOGGER.info("asyncResult value:{},taskCount", value, t);
                return value;
            });
            /**
             * 测试程序里通过RpcContext.getContext().getFuture()获取到保存异步结果的future，
             * RpcContext是每个线程独有的，不能在异步逻辑里设定，必须在主线程里设定，否则获
             * 取不到，这里rcompletableFuture已经伪装成了invoker.invoke返回的simpleAsyncRpcResult
             * 对象里的valueFuture,正是测试程序需要的
             */
            RpcContext.getContext().setFuture(rcompletableFuture);
            /**
             *   再伪装一个SimpleAsyncRpcResult交回给主线程，必须是SimpleAsyncRpcResult
             *   不能是父类AsyncRpcResult，因为AsyncRpcResult.recreate方法会被调用，产生
             *   强制类型转换错误
             */
            SimpleAsyncRpcResult asyncRpcResult = new SimpleAsyncRpcResult(rcompletableFuture, false);
            //LOGGER.info("asyncResult return");
            return asyncRpcResult;
        }
        //否则进行同步请求逻辑（实际上也是异步的，只是这个我们没办法定制，他会直接发送出去），和相关统计。
        int nowTaskCount = 0;
        //考虑到请求实际上都是异步发送，并且异步执行onResponse，这里的统计方式依然会线程不安全
        if (target != null) {
            nowTaskCount = target.decrementAndGet();
        }
        UserLoadBalance.totalWeight.decrementAndGet();
     // LOGGER.info("syn return,task count:{},key:{}",nowTaskCount,key);
        return invoker.invoke(invocation);

    }

    /**
     * 请求完成时执行的回调方法，主要进行统计，他实际上是通过
     * CompletableFuture的thenApply调用的，见ProtocolFilterWrapper
     * 76行，不管有没有执行延时逻辑他与发送的主线程都是异步的，
     * 因为发送请求都是通过异步future实现的，所以后续需要增加线程
     * 安全处理手段
     * @param result
     * @param invoker
     * @param invocation
     * @return
     */
    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String key = invoker.getUrl().getHost().split("-")[1];
        //目前的统计都是线程不安全的
        AtomicInteger target = UserLoadBalance.weightMap.get(key);
        int nowTaskCount = target.incrementAndGet();
        UserLoadBalance.totalWeight.incrementAndGet();
        //LOGGER.info("onResponse key:{},taskCount:{},result:{}",key,nowTaskCount,result.getValue());
        return result;
    }

}
