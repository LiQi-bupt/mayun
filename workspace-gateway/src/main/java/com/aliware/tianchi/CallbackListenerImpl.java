package com.aliware.tianchi;

import com.alibaba.fastjson.JSONObject;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.listener.CallbackListener;

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


    @Override
    public void receiveServerMsg(String msg) {
        try {
            Map<String,String> status  = (Map)JSONObject.parse(msg);
            Integer maxmumPoolSize = Integer.parseInt(status.get("maxmumPoolSize"));
            Integer poolSize = Integer.parseInt(status.get("poolSize"));
            Integer activeCount = Integer.parseInt(status.get("activeCount"));
            String key = status.get("quota");
            UserLoadBalance.weightMap.put(key,maxmumPoolSize-activeCount);
        } catch (Exception e){
            LOGGER.error(e);
        }

        LOGGER.info("receive msg from server :" + msg);
    }

}
