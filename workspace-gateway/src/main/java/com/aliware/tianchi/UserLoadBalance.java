package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    Random  random = new Random();

    public static ConcurrentHashMap<String,Integer> weightMap = new ConcurrentHashMap<>(16);

    private static long startTime = System.currentTimeMillis();

    private static int warmUpTime = 35*1000;

    static {
        weightMap.put("small",200);
        weightMap.put("medium",450);
        weightMap.put("large",600);
    }
    private static int defaultWeight = 100;
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        if (invokers == null || invokers.isEmpty())
//            return null;
//        //如果只有一个提供者直接返回，预热失效
//        if (invokers.size() == 1)
//            return invokers.get(0);
        return doSelect(invokers, url, invocation);
    }


    //让子类实现doSelect
    private  <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation){
        int length = invokers.size(); // Number of invokers
        int totalWeight = 0; // The sum of weights
        boolean sameWeight = true; // Every invoker has the same weight?
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            totalWeight += weight; // Sum
            if (sameWeight && i > 0
                    && weight != getWeight(invokers.get(i - 1), invocation)) {
                sameWeight = false;
            }
        }
        //如果提供者权重不一样，加权随机
        if (totalWeight > 0 && !sameWeight) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            int offset = random.nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < length; i++) {
                offset -= getWeight(invokers.get(i), invocation);
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }
        //如果提供者权重都一样，普通随机
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(random.nextInt(length));}

    //计算预热权重
    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        String key = invoker.getUrl().getHost().split("-")[1];
        Integer weight = weightMap.get(key);
        if(weight == null){
            weight = defaultWeight;
        }
        //预热时间30秒
//        int uptime = (int) (System.currentTimeMillis() - startTime);
//        if (uptime > 0 && uptime < warmUpTime) {
//            weight = calculateWarmupWeight(uptime, warmUpTime, weight);
//        } else {
//            CallbackListenerImpl.needWarmUP = false;
//        }
        LOGGER.info("weight :"+key+":"+weight);

        return weight;
    }

    //用于计算预热权重
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        int ww = (int) ((float) uptime / ((float) warmup / (float) weight));
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }

}
