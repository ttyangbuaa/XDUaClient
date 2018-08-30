package com.xderi.client;


import java.util.Map;

/**
 * @author : yangtingting
 * @date : 2018/6/19 16:54
 * @modified by :
 * @description : 该类用于构建报警事件模型
 **/
public class AlarmEventModel {
    private String key;//表示redis数据库中map类型数据的key
    private Map<String, String> value;//缓存数据，表示redis数据库中map类型数据的value
    private boolean isDeleted = false; //在Redis中是否删除该记录
    private DPTpyeEnum dpTpyeEnum;

    public AlarmEventModel() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Map<String, String> getValue() {
        return value;
    }

    public void setValue(Map<String, String> value) {
        this.value = value;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    public DPTpyeEnum getDpTpyeEnum() {
        return dpTpyeEnum;
    }

    public void setDpTpyeEnum(DPTpyeEnum dpTpyeEnum) {
        this.dpTpyeEnum = dpTpyeEnum;
    }
}