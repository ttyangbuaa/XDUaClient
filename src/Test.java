import com.xderi.client.AlarmEventListener;
import com.xderi.client.AlarmEventModel;
import com.xderi.client.XDUaClient;
import com.prosysopc.ua.SecureIdentityException;
import com.prosysopc.ua.ServiceException;
import com.prosysopc.ua.StatusException;
import org.apache.log4j.PropertyConfigurator;
import org.opcfoundation.ua.builtintypes.NodeId;
import org.opcfoundation.ua.transport.security.SecurityMode;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author : yangtingting
 * @date : 2018/6/25 15:48
 * @modified by :
 * @description :
 **/
public class Test {
    public static void main(String[] args) throws IOException, SecureIdentityException, URISyntaxException, ServiceException, StatusException {
        //PropertyConfigurator.configureAndWatch(Test.class.getResource("log.properties").getFile(), 5000);

        String serverUrl = "opc.tcp://localhost:48010/OPCUA/XD_ScadaServer";
        SecurityMode securityMode = SecurityMode.NONE;

        XDUaClient xdUaClient = new XDUaClient(serverUrl);
        xdUaClient.connect(xdUaClient, securityMode);

        NodeId nodeId = new NodeId(2, "1101");
        List<NodeId> nodeIds = new ArrayList<>();

        nodeIds.add(nodeId);

        AlarmEventListener alarmEventListener = new AlarmEventListener() {
            @Override
            public void onAlarm(AlarmEventModel alarmEventModel) {
                System.out.println("111111111111111111111" + new Date());
                System.out.println(alarmEventModel.getDpTpyeEnum() + "\t" + alarmEventModel.getKey());
                System.out.println(alarmEventModel.getValue().toString());
                System.out.println();
                System.out.println();
                System.out.println();
            }


        };

        xdUaClient.startAlarmEventHandler(nodeIds, alarmEventListener, "localhost", 6379, 1000);
    }
}
