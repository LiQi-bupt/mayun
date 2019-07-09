package com.aliware.tianchi;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 *
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 *
 */
public class CallbackListenerImpl implements CallbackListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CallbackListenerImpl.class);

    private static Gson gson = new Gson();

    /**
     * 目前回调只在刚刚建立与服务端的链接时发生一次，其他时间不建议
     * 使用回调，回调有延迟，consumer自己统计数据会更准确,如果要使
     * 用需要考虑与TestClientFilter中异步发送逻辑里进行的统计造成
     * 的并发冲突问题，目前使用原子类和ConcurrentHashMap解决并发写
     * @param msg
     */
    @Override
    public void receiveServerMsg(String msg) {
        try {
            Map<String, String> status = gson.fromJson(msg, HashMap.class);
            //最大线程数
            Integer maxmumPoolSize = Integer.parseInt(status.get("maxmumPoolSize"));
            //当前线程数，目前未使用
            Integer poolSize = Integer.parseInt(status.get("poolSize"));
            //当前活跃线程数，目前未使用
            Integer activeCount = Integer.parseInt(status.get("activeCount"));
            //在map中定位服务的key
            String key = status.get("quota");
            Integer newWeight = maxmumPoolSize;
            //将新权重增加到总权重统计，目前只在刚连接时计算一次，后续都由consumer端自己统计，不由服务端提供
            UserLoadBalance.totalWeight.addAndGet(newWeight);
            //将新权重设置为服务自身的最大权重，目前只在刚连接时设定一次，后续这个值不发生变化，目前策略不使用这个值
            UserLoadBalance.MaxWeightMap.put(key, newWeight);
            //将新权重设置为服务自身的当前权重，目前只在刚连接时计算一次，后续都由consumer端自己统计，不由服务端提供
            UserLoadBalance.weightMap.put(key, new AtomicInteger(newWeight));
            LOGGER.info(new Date() + " new weight:{}:{},total:{}", key, newWeight, UserLoadBalance.totalWeight);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info("receive msg from server :" + msg);
    }

}
