package com.aliware.tianchi;

import com.google.gson.Gson;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CallbackServiceImpl.class);

    private static Gson gson = new Gson();

    private static long startTime = System.currentTimeMillis();

    private static int warmUpTime = 35*1000;

    public static volatile boolean needReLoadBalance = false;

    public static volatile boolean full = false;

    private static final int TYPE_RELOADBALANCE = 1;

    private static final int TYPE_FULL = 2;

    private static ThreadPoolExecutor tp ;

    private static int maxPoolSize;

    private static int threshold;

    public static int fullThreshold;

    private static int lastActiveTaskCount = 0;

    private int act;

    private static volatile AtomicInteger inTimer = new AtomicInteger();

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
        threshold = maxPoolSize / 5;
        fullThreshold = threshold * 8;
        inTimer.set(0);
    }

    //不建议使用回调
    public CallbackServiceImpl() {
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                act = tp.getActiveCount();
//                int tmp = Math.abs(act - lastActiveTaskCount);
//                if (tmp > threshold) {
//                    if (!listeners.isEmpty()&&inTimer.compareAndSet(0,1)) {
//                        sendMessage(TYPE_RELOADBALANCE);
//                        LOGGER.info("act:{},lastAct:{},{}",act,lastActiveTaskCount,listeners.isEmpty());
//                        inTimer.compareAndSet(1,0);
//                    }
//                }
//            }
//        }, 0, 1000);

    }

    private Timer timer = new Timer();

    private void sendMessage(int type) {
        for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
            try {
                Map<String, String> statusMap = new HashMap<String, String>(16);
                statusMap.put("maxmumPoolSize", String.valueOf(tp.getMaximumPoolSize()));
                statusMap.put("poolSize", String.valueOf(tp.getPoolSize()));
                act = tp.getActiveCount();
                statusMap.put("activeCount", String.valueOf(act));
                statusMap.put("quota", System.getProperty("quota"));
                entry.getValue().receiveServerMsg(gson.toJson(statusMap));
                switch (type) {
                    case TYPE_RELOADBALANCE:
                        needReLoadBalance = false;
                        break;
                    case TYPE_FULL:
                        if (act < TestRequestLimiter.fullThreshold) {
                            full = false;
                        }
                        LOGGER.info("full:tmp:{},threshold:{},act:{}",act,fullThreshold,tp.getActiveCount());
                        break;
                }
                lastActiveTaskCount = act;
            } catch (Throwable t1) {
               // listeners.remove(entry.getKey());
                LOGGER.error("error! remove listener",t1);
            }
        }
    }

    /**
     * key: listener type
     * value: callback listener
     */
    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
        Map<String, String> statusMap = new HashMap<String, String>(16);
        statusMap.put("maxmumPoolSize", String.valueOf(tp.getMaximumPoolSize()));
        statusMap.put("poolSize", String.valueOf(tp.getPoolSize()));
        statusMap.put("activeCount", String.valueOf(tp.getActiveCount()));
        statusMap.put("quota", System.getProperty("quota"));
        listener.receiveServerMsg(gson.toJson(statusMap)); // send notification for change
    }
}
