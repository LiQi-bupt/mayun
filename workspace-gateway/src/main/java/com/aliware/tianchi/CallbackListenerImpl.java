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
    public static boolean needWarmUP = true;
    @Override
    public void receiveServerMsg(String msg) {
        try {
            Map<String, String> status = gson.fromJson(msg, HashMap.class);
            Integer maxmumPoolSize = Integer.parseInt(status.get("maxmumPoolSize"));
            Integer poolSize = Integer.parseInt(status.get("poolSize"));
            Integer activeCount = Integer.parseInt(status.get("activeCount"));
            String key = status.get("quota");
            Integer newWeight = maxmumPoolSize;
            synchronized (CallbackListenerImpl.class) {
                Integer oldWeight = UserLoadBalance.weightMap.get(key);
                int oldTotalWeight = UserLoadBalance.totalWeight;
                int newTotalWeight = (oldWeight == null ?
                        oldTotalWeight - UserLoadBalance.defaultWeight + newWeight :
                        oldTotalWeight - oldWeight + newWeight);
                UserLoadBalance.totalWeight = newTotalWeight;
                UserLoadBalance.weightMap.put(key, newWeight);
                UserLoadBalance.taskMap.put(key,new AtomicInteger(0));
                LOGGER.info(new Date()+" new weight:{}:{},total:{}", key, newWeight, UserLoadBalance.totalWeight);
            }

        } catch (Exception e) {
            LOGGER.error(e.toString());
        }
        LOGGER.info("receive msg from server :" + msg);
    }

}
