package com.xderi.client.alarm.dataprocessing;


import com.xderi.client.AlarmEventModel;
import com.xderi.client.AlarmEventListener;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

import redis.clients.jedis.Pipeline;

/**
 * @author : yangtingting
 * @date : 2018/6/19 16:54
 * @modified by :
 * @description : 该类用于构建报警事件缓存，以及多线程处理缓存数据
 **/
public class AlarmEventCacheHandler {
    private static Logger logger = Logger.getLogger(AlarmEventCacheHandler.class);

    private static AlarmEventCacheHandler alarmEventCacheHandler = null;

    private AlarmEventListener alarmEventListener = null;

    private LinkedList<AlarmEventModel> alarmEventCache = new LinkedList<>();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * 是否执行线程中的while模块
     */
    private boolean threadFlag = false;
    private Thread alarmEventDBThread = null;
    private String host;
    private int port;
    private int timeout;

    private AlarmEventCacheHandler() {
    }

    public static AlarmEventCacheHandler getInstance() {
        if (null == alarmEventCacheHandler) {
            alarmEventCacheHandler = new AlarmEventCacheHandler();
        }
        return alarmEventCacheHandler;
    }

    public void addAlarmEventListener(AlarmEventListener alarmEventListener) {
        this.alarmEventListener = alarmEventListener;
    }

    /**
     * 该方法用于将AlarmEventModel对象加入缓存队首
     *
     * @param alarmEventModel
     */
    public void addAlarmEventModel(AlarmEventModel alarmEventModel) {
        if (null != alarmEventModel) {
            try {
                readWriteLock.writeLock().lock();
                alarmEventCache.addLast(alarmEventModel);
            } catch (Exception e) {
                logger.info(e.getMessage(), e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
            //logger.info("向缓存alarmEventCache队首添加成功");
        }
    }

    public AlarmEventModel removeAlarmEventModel() {
        AlarmEventModel alarmEventModel = null;
        if (alarmEventCache.size() > 0) {
            try {
                readWriteLock.writeLock().lock();
                alarmEventModel = alarmEventCache.removeFirst();
            } catch (Exception e) {
                logger.info(e.getMessage(), e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
            //logger.info("缓存alarmEventCache最后一个元素 删除成功");
        }
        return alarmEventModel;
    }

    private class AlarmEventDBThread implements Runnable {
        @Override
        public void run() {
            LinkedList<AlarmEventModel> alarmEventModelLinkedList = new LinkedList<>();
            Jedis jedis = null;
            Pipeline pipeline = null;
            try {
                jedis = JedisPoolUtil.getJedis(host, port, timeout);
                jedis.select(0);
                pipeline = jedis.pipelined();
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(AlarmEventCacheHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
            while (!threadFlag) {
                //System.out.println("AlarmEventDBThread");
                if (alarmEventCache.size() > 0) {
                    int alarmEventCacheSize = alarmEventCache.size();
                    //logger.info("此时缓存alarmEventCache大小为 " + alarmEventCacheSize);
                    //将前一次循环中alarmEventModelLinkedList中的数据清除
                    alarmEventModelLinkedList.clear();
                    //取出alarmEventModels中最前面的数据放入alarmEventModelLinkedList
                    try {
                        for (int i = 0; i < alarmEventCacheSize; i++) {
                            alarmEventModelLinkedList.addLast(removeAlarmEventModel());
                        }
                        //将alarmEventModelLinkedList中的数据进行redis数据库操作（更新或删除）

                        LinkedList<AlarmEventModel> newAlarmEventModelLinkedList = JedisPoolUtil.jedisPipelineOperate(pipeline, alarmEventModelLinkedList, alarmEventListener);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        logger.info(e.getMessage(), e);
                    }
                }
            }
            JedisPoolUtil.returnResource(jedis);
        }
    }

    public void startAlarmEventDBThread(String host, int port, int timeout) {
        if (null == alarmEventDBThread) {
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            alarmEventDBThread = new Thread(new AlarmEventDBThread());
            logger.info("New AlarmEventDBThread");
        }
        if (!alarmEventDBThread.isAlive()) {
            alarmEventDBThread.start();
            logger.info("AlarmEventDBThread is start");
        }
    }

    public void stopAlarmEventDBThread() {
        if (null != alarmEventDBThread) {
            while (alarmEventDBThread.isAlive()) {
                threadFlag = true;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.info(e.getMessage());
                }
            }
            logger.info("AlarmEventDBThread is stop");
        }

    }
}