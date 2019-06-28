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
    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    Random  random = new Random();

    public static HashMap<String,Integer> weightMap = new HashMap<>(16);

    private static long startTime = System.currentTimeMillis();

    private static int warmUpTime = 35*1000;

    public  static volatile int totalWeight = 300;

    public static volatile ConcurrentHashMap<String, AtomicInteger> taskMap = new ConcurrentHashMap<>(8);

//    static {
//        weightMap.put("small",200);
//        weightMap.put("medium",450);
//        weightMap.put("large",650);
//    }
    public static final int defaultWeight = 100;
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
        //如果提供者权重不一样，加权随机
        if (totalWeight > 0 ) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            int offset = random.nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (Invoker<T> tmpInvoker:invokers) {
                offset -= getWeight(tmpInvoker);
                if (offset < 0) {
                    return tmpInvoker;
                }
            }
        }
        //如果提供者权重都一样，普通随机
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(random.nextInt(length));
    }

    //计算预热权重
    private int getWeight(Invoker<?> invoker) {
        String key = invoker.getUrl().getHost().split("-")[1];
        Integer weight = weightMap.get(key);
        if(weight == null){
            weight = defaultWeight;
        }
        //预热时间30秒
//        int uptime = (int) (System.currentTimeMillis() - startTime);
//        if (uptime > 0 && uptime < warmUpTime) {
//            weight = calculateWarmUpWeight(uptime, warmUpTime, weight);
//        } else {
//            CallbackListenerImpl.needWarmUP = false;
//        }
//        LOGGER.info("weight :"+key+":"+weight);

        return weight;
    }

    //用于计算预热权重
    static int calculateWarmUpWeight(int uptime, int warmup, int weight) {
        int ww = (int) ((float) uptime / ((float) warmup / (float) weight));
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }

}
