package com.aliware.tianchi;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;

import java.util.HashMap;
import java.util.Map;

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
            Integer newWeight = maxmumPoolSize - activeCount;
            Integer oldWeight = UserLoadBalance.weightMap.get(key);
            UserLoadBalance.totalWeight = (oldWeight == null ?
                    UserLoadBalance.totalWeight - UserLoadBalance.defaultWeight + newWeight :
                    UserLoadBalance.totalWeight - oldWeight + newWeight);
            UserLoadBalance.weightMap.put(key, newWeight);
            LOGGER.info("new weight:{}:{},total:{}",key,newWeight,UserLoadBalance.totalWeight);
        } catch (Exception e) {
            LOGGER.error(e.toString());
        }
        LOGGER.info("receive msg from server :" + msg);
    }

}
