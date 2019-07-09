package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    //正式提交成绩时取消日志打印能大幅降低响应时间，需要获取一些关键信息时可以开启
    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);
    //随机数生成器，也可以尝试更好的随机数生成方案
    private Random  random = new Random();
    //最大权重记录Map,目前的策略里没有使用
    static HashMap<String,Integer> MaxWeightMap = new HashMap<>(16);
    //当前总权重记录Map,接受和发送时都会修改，所以使用了原子类
    static volatile AtomicInteger totalWeight = new AtomicInteger(0);
    //当前权重记录Map
    static volatile ConcurrentHashMap<String, AtomicInteger> weightMap = new ConcurrentHashMap<>(8);
    //默认权重，在第一个请求发送的时候可能会用上
    private static final int defaultWeight = 100;

    /**
     * 加权随机算法，依据自己的统计数据进行
     * 加权随机，也可以进行优化
     * @param invokers
     * @param url
     * @param invocation
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int tw = totalWeight.get();
        //LOGGER.info("totalWeight:{}",tw);
        if (tw > 0) {
            // If totalWeight>0 select randomly based on totalWeight.
            int offset = random.nextInt(tw);
            // Return a invoker based on the random value.
            for (Invoker<T> tmpInvoker:invokers) {
                offset -= getWeight(tmpInvoker);
                if (offset < 0) {
                    return tmpInvoker;
                }
            }
        }
        // Number of invokers
        int length = invokers.size();
        // If totalWeight=0, return evenly.
        return invokers.get(random.nextInt(length));
    }

    private int getWeight(Invoker<?> invoker) {
        String key = invoker.getUrl().getHost().split("-")[1];
        Integer weight;
        AtomicInteger atomicInteger = weightMap.get(key);
        if(atomicInteger == null){
            weight = defaultWeight;
        } else {
            weight = atomicInteger.get();
        }
        return weight;
    }

}
