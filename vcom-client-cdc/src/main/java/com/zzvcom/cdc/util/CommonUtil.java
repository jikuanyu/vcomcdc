package com.zzvcom.cdc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yujikuan
 * @Classname CommonUtil
 * @Description 工具类
 * @Date 2023/8/17 9:55
 */
public class CommonUtil {
    private static final Logger log = LoggerFactory.getLogger(CommonUtil.class);

    private CommonUtil() {
    }

    public static ThreadPoolExecutor newCacheThreadPool(int nThreads, long keepAliveTime) {
        return new ThreadPoolExecutor(nThreads,
                nThreads,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                (r, exe) -> {
                    if (!exe.isShutdown()) {
                        try {
                            exe.getQueue().put(r);
                        } catch (InterruptedException e) {
                            log.error("线程退出{}", e.getMessage());
                            Thread.currentThread().interrupt();
                        }
                    }
                });
    }
}
