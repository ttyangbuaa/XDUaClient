package com.xderi.client.alarm.listener;
import com.prosysopc.ua.ContentFilterBuilder;
import com.prosysopc.ua.client.MonitoredEventItem;
import com.xderi.client.XDUaClient;
import com.xderi.client.AlarmEventListener;
import org.opcfoundation.ua.builtintypes.NodeId;
import org.opcfoundation.ua.builtintypes.QualifiedName;
import org.opcfoundation.ua.builtintypes.UnsignedInteger;
import org.opcfoundation.ua.builtintypes.Variant;
import org.opcfoundation.ua.core.*;

/**
 * @author : yangtingting
 * @date : 2018/6/25 16:33
 * @modified by :
 * @description :
 **/
public class MonitorServiceHandler {
    private static XDUaClient client;
    private static QualifiedName[] eventFieldNames;



    public MonitorServiceHandler(XDUaClient client, QualifiedName[] eventFieldNames) {
        MonitorServiceHandler.client = client;
        MonitorServiceHandler.eventFieldNames = eventFieldNames;
    }

    /**
     * 该类用于生成Event类型的Monitor
     *
     * @param nodeId
     * @return
     */
    public MonitoredEventItem createMonitoredEventItem(NodeId nodeId) {

        EventFilter eventFilter = createEventFilter(eventFieldNames);

        MonitoredEventItem eventItem = new MonitoredEventItem(nodeId, eventFilter);

        return eventItem;
    }

    private EventFilter createEventFilter(QualifiedName[] eventFieldNames) {
        EventFilter eventFilter = new EventFilter();

        // This defines the event type of the fields. It should be defined per browsePath, but for example the Java SDK servers ignore the value at the moment
        NodeId conditionType = Identifiers.ConditionType;
        UnsignedInteger eventAttributeId = Attributes.Value;
        String indexRange = null;
        SimpleAttributeOperand[] selectClauses = new SimpleAttributeOperand[eventFieldNames.length + 1];
        for (int i = 0; i < eventFieldNames.length; i++) {
            QualifiedName[] browsePath = createBrowsePath(eventFieldNames[i], 0);
            selectClauses[i] = new SimpleAttributeOperand(conditionType, browsePath, eventAttributeId, indexRange);
        }
        // Add a field to get the NodeId of the event source
        selectClauses[eventFieldNames.length] = new SimpleAttributeOperand(conditionType, null, Attributes.NodeId, null);
        // Event field selection
        eventFilter.setSelectClauses(selectClauses);

        // Event filtering: the following sample creates a "Not OfType GeneralModelChangeEventType" eventFilter
        ContentFilterBuilder fb = new ContentFilterBuilder(client.getEncoderContext());
        QualifiedName[] eventTypePath = {new QualifiedName("EventType")};
        fb.add(FilterOperator.Or, new ElementOperand(UnsignedInteger.valueOf(1)), new ElementOperand(UnsignedInteger.valueOf(2))); //0
        fb.add(FilterOperator.Equals,
                new SimpleAttributeOperand(
                        Identifiers.ConditionType,
                        eventTypePath, Attributes.Value, null),
                new LiteralOperand(new Variant(Identifiers.DiscreteAlarmType)));                                                   //1
        fb.add(FilterOperator.Or, new ElementOperand(UnsignedInteger.valueOf(3)), new ElementOperand(UnsignedInteger.valueOf(4)));//2
        fb.add(FilterOperator.Or, new ElementOperand(UnsignedInteger.valueOf(5)), new ElementOperand(UnsignedInteger.valueOf(6)));//4
        fb.add(FilterOperator.Equals,
                new SimpleAttributeOperand(
                        Identifiers.ConditionType,
                        eventTypePath, Attributes.Value, null),
                new LiteralOperand(new Variant(Identifiers.ExclusiveDeviationAlarmType)));                                                   //3
        fb.add(FilterOperator.Equals,
                new SimpleAttributeOperand(
                        Identifiers.ConditionType,
                        eventTypePath, Attributes.Value, null),
                new LiteralOperand(new Variant(Identifiers.ExclusiveLevelAlarmType)));                                                   //5
        fb.add(FilterOperator.Equals,
                new SimpleAttributeOperand(
                        Identifiers.ConditionType,
                        eventTypePath, Attributes.Value, null),
                new LiteralOperand(new Variant(Identifiers.ExclusiveRateOfChangeAlarmType)));                                                   //6

        //设置where条件
        eventFilter.setWhereClause(fb.getContentFilter());
        return eventFilter;
    }

    private QualifiedName[] createBrowsePath(QualifiedName qualifiedName, int namespaceIndex) {
        if (!qualifiedName.getName().contains("/")) {
            return new QualifiedName[]
                    {
                            new QualifiedName(namespaceIndex, qualifiedName.toString())
                    };
        }

        String[] names = qualifiedName.getName().split("/");
        QualifiedName[] result = new QualifiedName[names.length];
        for (int i = 0; i < names.length; i++) {
            result[i] = new QualifiedName(namespaceIndex, names[i]);
        }
        return result;
    }
}
