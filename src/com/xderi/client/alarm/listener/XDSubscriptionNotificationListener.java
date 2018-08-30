package com.xderi.client.alarm.listener;


import com.prosysopc.ua.client.MonitoredDataItem;
import com.prosysopc.ua.client.MonitoredEventItem;
import com.prosysopc.ua.client.Subscription;
import com.prosysopc.ua.client.SubscriptionNotificationListener;
import com.xderi.client.AlarmEventListener;
import com.xderi.client.AlarmEventModel;
import com.xderi.client.alarm.dataprocessing.AlarmEventCacheHandler;
import org.apache.log4j.Logger;
import org.opcfoundation.ua.builtintypes.*;
import org.opcfoundation.ua.core.NotificationData;
import org.opcfoundation.ua.core.TimeZoneDataType;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;

/**
 * @author : yangtingting
 * @date : 2018/6/25 16:24
 * @modified by :
 * @description :
 **/
public class XDSubscriptionNotificationListener implements SubscriptionNotificationListener {
    private static Logger logger = Logger.getLogger(XDSubscriptionNotificationListener.class);
    private static QualifiedName[] eventFieldNames;

    public XDSubscriptionNotificationListener(QualifiedName[] eventFieldNames,AlarmEventListener alarmEventListener) {
        super();
        XDSubscriptionNotificationListener.eventFieldNames = eventFieldNames;
        AlarmEventCacheHandler.getInstance().addAlarmEventListener(alarmEventListener);
    }
    @Override
    public void onBufferOverflow(Subscription subscription, UnsignedInteger unsignedInteger, ExtensionObject[] extensionObjects) {
        logger.warn("SubscriptionNotificationListener-onBufferOverflow" + "*** SUBCRIPTION BUFFER OVERFLOW ***");
    }

    @Override
    public void onDataChange(Subscription subscription, MonitoredDataItem monitoredDataItem, DataValue dataValue) {
        // Called for each data change notification
    }

    @Override
    public void onError(Subscription subscription, Object o, Exception e) {
        // Called if the parsing of the notification data fails,notification is either a MonitoredItemNotification or an EventList
        logger.warn("SubscriptionNotificationListener-onError" + e);
    }

    @Override
    public void onEvent(Subscription subscription, MonitoredEventItem monitoredEventItem, Variant[] variants) {
        // Called for each event notification
        //logger.info("XDSubscriptionNotificationListener" + monitoredEventItem.getNodeId());
        AlarmEventModel alarmEventModel = variantsToCacheModel(eventFieldNames, variants);
        AlarmEventCacheHandler.getInstance().addAlarmEventModel(alarmEventModel);
    }

    @Override
    public long onMissingData(UnsignedInteger lastSequenceNumber, long sequenceNumber, long newSequenceNumber, StatusCode statusCode) {
        logger.warn("Data missed: lastSequenceNumber=" + lastSequenceNumber + " newSequenceNumber=" + newSequenceNumber);

        return newSequenceNumber;
    }

    @Override
    public void onNotificationData(Subscription subscription, NotificationData notificationData) {
        // Called after a complete notification data package is handled
    }

    @Override
    public void onStatusChange(Subscription subscription, StatusCode statusCode, StatusCode statusCode1, DiagnosticInfo diagnosticInfo) {
        // Called when the subscription status has changed in the server
    }

    protected AlarmEventModel variantsToCacheModel(QualifiedName[] eventFieldNames, Variant[] variants) {
        AlarmEventModel alarmEventModel = new AlarmEventModel();
        HashMap<String, String> map = new HashMap<>();
        Boolean isDeleted = false;

        String eventId = "", localTimeZone = "", message = "", comment = "", quality = "";

        //eventId为byte数组，将其转换成16进制字符串
        if (variants[0].getValue() != null) {
            eventId = "0x" + DatatypeConverter.printHexBinary((byte[]) variants[0].getValue());
        }
        map.put(eventFieldNames[0].toString(), eventId);

        String eventType = variants[1].getValue() == null ? "" : variants[1].getValue().toString();
        //不需要加ns=0，如果没有就是ns=0
//        if (!eventType.contains("ns")) {
//            eventType = "ns=0;" + eventType;
//        }
        map.put(eventFieldNames[1].toString(), eventType);

        if (variants[2].getValue() != null) {
            TimeZoneDataType localTimeStr = (TimeZoneDataType) variants[2].getValue();
            localTimeZone = String.valueOf(localTimeStr.getOffset());
        }
        map.put(eventFieldNames[2].toString(), localTimeZone);

        LocalizedText txt = variants[3].getValue() == null ? new LocalizedText("") : (LocalizedText) variants[3].getValue();
        if (null != txt) {
            message = txt.getText();
        }
        map.put(eventFieldNames[3].toString(), message);

        DateTime receiveTime = variants[4].getValue() == null ? null : (DateTime) variants[4].getValue();
        map.put(eventFieldNames[4].toString(), receiveTime.toString());

        int severity = variants[5].getValue() == null ? 0 : variants[5].intValue();
        map.put(eventFieldNames[5].toString(), String.valueOf(severity));

        String sourceName = variants[6].getValue() == null ? "" : variants[6].getValue().toString();
        map.put(eventFieldNames[6].toString(), sourceName);

        String sourceNode = variants[7].getValue() == null ? "" : variants[7].getValue().toString();
        map.put(eventFieldNames[7].toString(), sourceNode);

        String branchId = variants[8].getValue() == null ? "" : variants[8].getValue().toString();
        map.put(eventFieldNames[8].toString(), branchId);

        String addComment = variants[9].getValue() == null ? "" : variants[9].getValue().toString();
        map.put(eventFieldNames[9].toString(), addComment);

        String clientUserId = variants[10].getValue() == null ? "" : variants[10].getValue().toString();
        map.put(eventFieldNames[10].toString(), clientUserId);

        LocalizedText txtComment = variants[11].getValue() == null ? new LocalizedText("") : (LocalizedText) variants[11].getValue();
        if (null != txtComment) {
            comment = txtComment.getText();
        }
        map.put(eventFieldNames[11].toString(), comment);

        String conditionId = variants[30].getValue() == null ? "" : variants[30].toString();
        map.put(eventFieldNames[12].toString(), conditionId);

        String conditionClassId = variants[13].getValue() == null ? "" : variants[13].getValue().toString();
        map.put(eventFieldNames[13].toString(), conditionClassId);

        String conditionClassName = variants[14].getValue() == null ? "" : variants[14].getValue().toString();
        map.put(eventFieldNames[14].toString(), conditionClassName);

        String conditionName = variants[15].getValue() == null ? "" : variants[15].getValue().toString();
        map.put(eventFieldNames[15].toString(), conditionName);

        boolean conditionRefresh = variants[16].getValue() == null ? false : variants[16].booleanValue();
        map.put(eventFieldNames[16].toString(), String.valueOf(conditionRefresh));

        boolean disable = variants[17].getValue() == null ? false : variants[17].booleanValue();
        map.put(eventFieldNames[17].toString(), String.valueOf(disable));

        boolean enable = variants[18].getValue() == null ? false : variants[18].booleanValue();
        map.put(eventFieldNames[18].toString(), String.valueOf(enable));

        boolean enabledState = variants[19].getValue() == null ? false : variants[19].booleanValue();
        map.put(eventFieldNames[19].toString(), String.valueOf(enabledState));

        int lastSeverity = variants[20].getValue() == null ? 0 : variants[20].intValue();
        map.put(eventFieldNames[20].toString(), String.valueOf(lastSeverity));

        StatusCode statusCode = ((StatusCode) variants[21].getValue());
        if (null != statusCode) {
            quality = String.format("0x%08x", statusCode.getValue().longValue());
        }
        map.put(eventFieldNames[21].toString(), quality);

        boolean retain = variants[22].getValue() == null ? false : variants[22].booleanValue();
        map.put(eventFieldNames[22].toString(), String.valueOf(retain));

        DateTime time = variants[23].getValue() == null ? null : (DateTime) variants[23].getValue();
        map.put(eventFieldNames[23].toString(), String.valueOf(time));

        boolean activeState = variants[24].getValue() == null ? false : variants[24].booleanValue();
        map.put("ActiveState", String.valueOf(activeState));

        boolean ackedstate = variants[25].getValue() == null ? false : variants[25].booleanValue();
        map.put(eventFieldNames[25].toString(), String.valueOf(ackedstate));

        boolean confirmedState = variants[26].getValue() == null ? false : variants[26].booleanValue();
        map.put(eventFieldNames[26].toString(), String.valueOf(confirmedState));

        boolean limitState = variants[27].getValue() == null ? false : variants[27].booleanValue();
        map.put(eventFieldNames[27].toString(), String.valueOf(limitState));


        String eventIdetify = variants[28].getValue() == null ? "" : variants[28].toString();
        map.put(eventFieldNames[28].toString(), eventIdetify);

        //System.out.println("map: " + map.toString());

        //Retain=false ActiveState=true, AckedState=true, ConfirmedState=true时，isDeleted=true
        //Retain=false ActiveState=false时，isDeleted=true
        if (!retain && (!activeState || (!ackedstate && !confirmedState))) {
            isDeleted = true;
            //System.out.println(conditionId + "应该被删除");
        }

        alarmEventModel.setDeleted(isDeleted);
        alarmEventModel.setValue(map);
        alarmEventModel.setKey(conditionId);

        return alarmEventModel;
    }

}
