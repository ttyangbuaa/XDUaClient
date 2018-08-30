package com.xderi.client.alarm.dataprocessing;

import com.xderi.client.AlarmEventListener;
import com.xderi.client.DPTpyeEnum;
import com.xderi.client.AlarmEventModel;
import redis.clients.jedis.*;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;


public class JedisPoolUtil {

    private static JedisPool jedisPool = null;
    private static Logger logger = Logger.getLogger(AlarmEventCacheHandler.class);
    /**
     * 读取Redis配置文件
     *
     * @return
     * @throws IOException
     */
/*    private static Properties getJedisProperties() throws IOException {

        Properties properties = new Properties();

        //打印当前文件夹所在绝对路径
        //System.out.println("当前文件夹所在绝对路径:" + new File("").getAbsolutePath());
        //方法2
        //FileInputStream fileInputStream = new FileInputStream("./src/xdscadaserver/jedis.properties");
        BufferedReader bufferedReader = new BufferedReader(new FileReader("./src/XDUaClient/jedis.properties"));
        properties.load(bufferedReader);

        return properties;
    }*/

    /**
     * 创建连接池
     */
    private static void createJedisPool(String host, int port, int timeout) throws IOException {
        // 建立连接池配置参数
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //Properties properties = getJedisProperties();
        // 设置最大连接数
        jedisPoolConfig.setMaxTotal(1024);
        // 设置最大阻塞时间，记住是毫秒数milliseconds
        jedisPoolConfig.setMaxWaitMillis(100000);
        // 设置空间连接
        jedisPoolConfig.setMaxIdle(5);
        // jedis实例是否可用
        boolean borrow = true;
        jedisPoolConfig.setTestOnBorrow(borrow);

        //String password = properties.getProperty("PASSWORD");

        // 创建连接池
        jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout);

    }

    /**
     * 获取Jedis实例
     *
     * @return
     */
    public static Jedis getJedis(String host, int port, int timeout) throws IOException {
        if (null == jedisPool) {
            createJedisPool(host, port, timeout);
        }
        return jedisPool.getResource();
    }

    public static void returnResource(Jedis jedis) {
        if (null != jedis && null != jedisPool) {
            jedis.close();
            //jedisPool.returnResource(jedis); returnResource方法遭弃用，应该为close
        }
    }

    /**
     * 该方法用于批量向Redis中更新时间报警状态（更新方式为记录更新或删除）
     *
     * @param pipeline
     * @param alarmEventModels
     * @return 
     */
    public static LinkedList<AlarmEventModel> jedisPipelineOperate(Pipeline pipeline, LinkedList<AlarmEventModel> alarmEventModels, AlarmEventListener alarmEventListener) {
        //alarmEventModelLinkedList用于存放处理后的新数据
        LinkedList<AlarmEventModel> alarmEventModelLinkedList = new LinkedList<>();
    

        //pipelineResponses用于存储提交管道请求后redis返回的Response类型数据
        Map<String, Response<Map<String, String>>> pipelineResponses = new HashMap<>(alarmEventModels.size());

        //数据处理总思路：isDeleted为true时，将数据从redis中删除；isDeleted为false时，判断Redis中是否存在该key的数据：不存在则直接覆盖更新，存在则比较新旧数据得出newAlarmEventModel
        //1.isDeleted为false时：1.1根据key从redis中进行查询，同时将查询结果放入pipelineResponses中
        //2.isDeleted为true时，DPTpyeEnum打标签为DELETE
        for (AlarmEventModel alarmEventModel : alarmEventModels) {
            if (!alarmEventModel.isDeleted()) {
                pipelineResponses.put(alarmEventModel.getKey(), pipeline.hgetAll(alarmEventModel.getKey()));
            } else {
                alarmEventModel.setDpTpyeEnum(DPTpyeEnum.DELETE);
                alarmEventModelLinkedList.add(alarmEventModel);

                alarmEventListener.onAlarm(alarmEventModel);
            }

        }

        pipeline.sync();

        //1.2解析pipelineResponses，2种情况：redis中存在该key，进行新旧数据的对比，DPTpyeEnum打标签为UPDATE；redis中不存在该key,将DPTpyeEnum打标签为ADD
        for (AlarmEventModel alarmEventModel : alarmEventModels) {
            if (alarmEventModel.isDeleted()) {
                continue;
            }
            String key = alarmEventModel.getKey();
            //redisMap用于存放根据key从redis中取出的value（这里需要注意redis可能不存在该key，因此还要判断redisMap.size）
            Map<String, String> redisMap = pipelineResponses.get(key).get();

            alarmEventModel = dataProcessFunc(alarmEventModel,redisMap);

            alarmEventModelLinkedList.add(alarmEventModel);

            alarmEventListener.onAlarm(alarmEventModel);
        }

        //3.将新数据进行redis处理
        for (AlarmEventModel alarmEventModel : alarmEventModelLinkedList) {
            if (alarmEventModel.isDeleted()) {
                pipeline.hdel(alarmEventModel.getKey());
                //System.out.println("------------------hdel--------------");
            } else {
                pipeline.hmset(alarmEventModel.getKey(), alarmEventModel.getValue());
                //System.out.println("------------------hmset--------------");
            }
        }
        pipeline.sync();
        //logger.info("AlarmData Redis Operate Complete!");
        return alarmEventModelLinkedList;
    }

    private static AlarmEventModel dataProcessFunc(AlarmEventModel alarmEventModel, Map<String, String> redisMap) {
        //subscriptionMap用于存放订阅到的新数据value
        Map<String, String> subscriptionMap = alarmEventModel.getValue();

        String ActiveTime = "", InActiveTime = "", AckedTime = "", ConfirmedTime = "";
        String subTime = subscriptionMap.get("Time");
        String redisTime = redisMap.get("Time");

        if (redisMap.size() == 0) {
            //向subscriptionMap中添加4个新字段
            if ("true".equals(subscriptionMap.get("ActiveState"))) {
                ActiveTime = subTime;
            } else {
                InActiveTime = subTime;
            }
            if ("true".equals(subscriptionMap.get("AckedState"))) {
                AckedTime = subTime;
            }
            if ("true".equals(subscriptionMap.get("ConfirmedState"))) {
                ConfirmedTime = subTime;
            }

            subscriptionMap.put("ActiveTime", ActiveTime);
            subscriptionMap.put("InActiveTime", InActiveTime);
            subscriptionMap.put("AckedTime", AckedTime);
            subscriptionMap.put("ConfirmedTime", ConfirmedTime);

            //System.out.println("Time = " + subTime +"\tActiveTime = " + ActiveTime + "\tInActiveTime = " + InActiveTime + "\tAckedTime = " + AckedTime + "\tConfirmedTime = " + ConfirmedTime);

            alarmEventModel.setDpTpyeEnum(DPTpyeEnum.ADD);
            alarmEventModel.setValue(subscriptionMap);
        } else {
            /*遍历redisMap的所有key，逐一对比redisMap和subscriptionMap中的value：
                subscriptionMap中的value为空，则用redisMap中的value覆盖，反之则保留subscriptionMap中的value*/
            Map<String, String> newRedisValueMap = new HashMap<>();
            for (Map.Entry<String, String> RedisEntry : redisMap.entrySet()) {
                String key = RedisEntry.getKey();
                if ("".equals(subscriptionMap.get(key))) {
                    newRedisValueMap.put(key, RedisEntry.getValue());
                } else {
                    newRedisValueMap.put(key, subscriptionMap.get(key));
                }
            }

            String newRedisTime = newRedisValueMap.get("Time");
            //redisMap中没有4个新字段
            if (!redisMap.containsKey("ActiveTime")) {
                if ("true".equals(redisMap.get("ActiveState"))) {
                    ActiveTime = redisTime;
                    if ("false".equals(subscriptionMap.get("ActiveState"))) {
                        InActiveTime = newRedisTime;
                    }
                } else {
                    if ("true".equals(subscriptionMap.get("ActiveState"))) {
                        ActiveTime = newRedisTime;
                    } else {
                        ActiveTime = redisTime;
                    }
                }

                if ("true".equals(redisMap.get("AckedState"))) {
                    AckedTime = redisTime;
                } else {
                    if ("true".equals(subscriptionMap.get("AckedState"))) {
                        AckedTime = newRedisTime;
                    }
                }

                if ("true".equals(redisMap.get("ConfirmedState"))) {
                    ConfirmedTime = redisTime;
                } else {
                    if ("true".equals(subscriptionMap.get("ConfirmedState"))) {
                        ConfirmedTime = newRedisTime;
                    }
                }
            } else {//redisMap中有4个新字段
                if ("true".equals(redisMap.get("ActiveState")) && "false".equals(subscriptionMap.get("ActiveState"))) {
                    InActiveTime = newRedisTime;
                    ActiveTime = redisTime;
                }
                if ("true".equals(subscriptionMap.get("ActiveState")) && "false".equals(redisMap.get("ActiveState"))) {
                    ActiveTime = newRedisTime;
                }

                if ("false".equals(redisMap.get("AckedState")) && "true".equals(subscriptionMap.get("AckedState"))) {
                    AckedTime = newRedisTime;
                }

                if ("false".equals(redisMap.get("ConfirmedState")) && "true".equals(subscriptionMap.get("ConfirmedState"))) {
                    ConfirmedTime = newRedisTime;
                }

            }

            newRedisValueMap.put("ActiveTime", ActiveTime);
            newRedisValueMap.put("InActiveTime", InActiveTime);
            newRedisValueMap.put("AckedTime", AckedTime);
            newRedisValueMap.put("ConfirmedTime", ConfirmedTime);

            /*System.out.println(newRedisValueMap.get("ConditionId"));
            System.out.println("newRedisTime = " + newRedisTime +"\tActiveTime = " + ActiveTime + "\tInActiveTime = " + InActiveTime + "\tAckedTime = " + AckedTime + "\tConfirmedTime = " + ConfirmedTime);
*/

            alarmEventModel.setDpTpyeEnum(DPTpyeEnum.UPDATE);
            alarmEventModel.setValue(newRedisValueMap);
        }

        return alarmEventModel;
    }


}