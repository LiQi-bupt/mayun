package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author daofeng.xjf
 *
 * 服务端限流
 * 可选接口
 * 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {

    /**
     * @param request 服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return  false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException
     *          true 不限流
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TestRequestLimiter.class);

    private static ThreadPoolExecutor tp = null;

    private static int maxPoolSize;

    private static int threshold;

    public static int fullThreshold;

    static {
        DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
        Map<String, Object> executors = dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY);
        for (Map.Entry<String, Object> entry2 : executors.entrySet()) {
            ExecutorService executor = (ExecutorService) entry2.getValue();
            if (executor instanceof ThreadPoolExecutor) {
                tp = (ThreadPoolExecutor) executor;
            }
        }
        maxPoolSize = tp.getMaximumPoolSize();
        threshold = maxPoolSize / 10;
        fullThreshold = threshold * 8;
        LOGGER.info("core:{},max:{},pool:{},active:{},queue:{}", tp.getCorePoolSize(), tp.getMaximumPoolSize(),
                tp.getPoolSize(), tp.getActiveCount(), tp.getQueue().size());
    }

    private int lastActiveTaskCount = 0;



    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        if (activeTaskCount  >= maxPoolSize){
            LOGGER.info(new Date().toString()+" refuse: maxPoolSize:{}, activeTaskCount:{},queue:{}",maxPoolSize,
                    tp.getActiveCount(),tp.getQueue().size());
            return false;
        }
        return true;
    }

}
