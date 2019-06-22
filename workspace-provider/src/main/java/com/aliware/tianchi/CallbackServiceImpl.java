package com.aliware.tianchi;

import com.google.gson.Gson;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.bytecode.Proxy;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    private static Gson gson = new Gson();

    private static long startTime = System.currentTimeMillis();

    private static int warmUpTime = 35*1000;

    public static boolean full = false;

    private static ThreadPoolExecutor tp ;

    static {
        DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
        Map<String, Object> executors = dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY);
        for (Map.Entry<String, Object> entry2 : executors.entrySet()) {
            ExecutorService executor = (ExecutorService) entry2.getValue();
            if (executor instanceof ThreadPoolExecutor) {
                tp = (ThreadPoolExecutor) executor;
            }
        }
    }

    public CallbackServiceImpl() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (full&&!listeners.isEmpty()) {
                    for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                        try {
                            Map<String, String> statusMap = new HashMap<String, String>(16);
                            statusMap.put("maxmumPoolSize", String.valueOf(tp.getMaximumPoolSize()));
                            statusMap.put("poolSize", String.valueOf(tp.getPoolSize()));
                            statusMap.put("activeCount", String.valueOf(tp.getActiveCount()));
                            statusMap.put("quota", System.getProperty("quota"));
                            entry.getValue().receiveServerMsg(gson.toJson(statusMap));
                            full = false;
                        } catch (Throwable t1) {
                            listeners.remove(entry.getKey());
                        }
                    }
                }
            }
        }, 0, 1000);
    }

    private Timer timer = new Timer();

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
