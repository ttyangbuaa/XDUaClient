package com.xderi.client;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.*;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

import com.xderi.client.alarm.dataprocessing.AlarmEventCacheHandler;
import org.opcfoundation.ua.builtintypes.*;
import org.opcfoundation.ua.core.*;
import org.opcfoundation.ua.encoding.DecodingException;
import org.opcfoundation.ua.transport.security.HttpsSecurityPolicy;
import org.opcfoundation.ua.transport.security.SecurityMode;
import org.opcfoundation.ua.utils.NumericRange;
import org.slf4j.LoggerFactory;
import com.xderi.client.alarm.listener.XDSubscriptionAliveListener;
import com.xderi.client.alarm.listener.MonitorServiceHandler;
import com.xderi.client.alarm.listener.XDSubscriptionNotificationListener;
import com.prosysopc.ua.*;
import com.prosysopc.ua.client.*;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author dudong
 */
public class XDUaClient extends UaClient
{

    public enum EnumConfigFileType
    {
        EnumConfigFileType_None,
        EnumConfigFileType_ServerModleCfg,
        EnumConfigFileType_AcquisitionIOCfg;
    }

    public class ProgressInfo
    {

        private long m_Pregress = 0;
        private long m_Total = 0;
        //private ReadWriteLock m_lock = new ReentrantReadWriteLock();

        /**
         * @return the m_Pregress
         */
        public long getPregress()
        {
            return m_Pregress;
        }

        /**
         * @param m_Pregress the m_Pregress to set
         */
        public void setPregress(long m_Pregress)
        {

            this.m_Pregress = m_Pregress;

        }

        /**
         * @return the m_Total
         */
        public long getTotal()
        {
            return m_Total;
        }

        /**
         * @param m_Total the m_Total to set
         */
        public void setTotal(long m_Total)
        {
            this.m_Total = m_Total;
        }

    }

    public class ContinuationPointForHistoryReading
    {

        /**
         * 要开始读的索引号
         */
        private byte[] m_continueReadInfo = null;
        /**
         * 是否已经读取完成
         */
        private boolean m_bFinished = false;
        /**
         * 读了多少条
         */
        private long m_iHasReadCount = 0;

        private String m_ErrorMessage = "";
        private long m_iTotalReadCount = 0;

        /**
         * @return the m_iTotalReadCount
         */
        public long getTotalReadCount()
        {
            return m_iTotalReadCount;
        }

        /**
         * @return the m_continueReadInfo
         */
        public byte[] getContinueReadInfo()
        {
            return m_continueReadInfo;
        }

        /**
         * @param m_continueReadInfo the m_continueReadInfo to set
         */
        private void setContinueReadInfo(byte[] m_continueReadInfo)
        {
            this.m_continueReadInfo = m_continueReadInfo;
        }

        /**
         * @return the m_bFinished
         */
        public boolean isFinished()
        {
            return m_bFinished;
        }

        /**
         * @param m_bFinished the m_bFinished to set
         */
        private void setFinished(boolean m_bFinished)
        {
            this.m_bFinished = m_bFinished;
        }

        /**
         * @return the m_iPreReadCount
         */
        public long getHasReadCount()
        {
            return m_iHasReadCount;
        }

        /**
         *
         */
        private void setHasReadCount(long iHasReadCount)
        {
            this.m_iTotalReadCount += iHasReadCount;
            this.m_iHasReadCount = iHasReadCount;
        }

        /**
         * @return the m_ErrorMessage
         */
        public String getErrorMessage()
        {
            return m_ErrorMessage;
        }

        /**
         * @param m_ErrorMessage the m_ErrorMessage to set
         */
        private void setErrorMessage(String m_ErrorMessage)
        {
            this.m_ErrorMessage = m_ErrorMessage;
        }
    }
    private final static org.slf4j.Logger m_sLogger = LoggerFactory.getLogger(XDUaClient.class);
    private static final  String m_sLogConfigFileName = "log.properties";//System.getProperty("user.dir")+"\\
    public XDUaClient(String strURI) throws URISyntaxException
    {
        super(strURI);
        PropertyConfigurator.configureAndWatch(XDUaClient.class.getResource(m_sLogConfigFileName).getFile());        
        m_sLogger.info("new  XDUaClient! ");
        // <editor-fold defaultstate="collapsed" desc="Compiled Code">
        /* 0: aload_0
         * 1: invokespecial com/prosysopc/ua/client/UaClient."<init>":()V
         * 4: aload_0
         * 5: aload_1
         * 6: invokevirtual com/prosysopc/ua/client/UaClient.setUri:(Ljava/lang/String;)V
         * 9: return
         *  */
        // </editor-fold>
    }

    public XDUaClient(UaAddress uaAddress)
    {
        super(uaAddress);
        PropertyConfigurator.configureAndWatch(XDUaClient.class.getResource(m_sLogConfigFileName).getFile());
        m_sLogger.info("new  XDUaClient! ");
    }

    public ContinuationPointForHistoryReading CreateContinuationPointForHistoryReading()
    {
        return new ContinuationPointForHistoryReading();
    }

    public ProgressInfo CreateProgressInfo()
    {
        return new ProgressInfo();
    }

    /**
     * 报警Enbale 使能
     *
     * @param conditionId
     * @return
     * @throws ServiceException
     * @throws MethodCallStatusException
     */
    public boolean Enbale(NodeId conditionId) throws ServiceException, MethodCallStatusException
    {
        boolean bResult = true;
        //NodeId objectId = this.getAddressSpace().getServerTable().;
        NodeId methodId = Identifiers.ConditionType_Enable;
        try
        {
            //ConditionType_Enable方法无返回值，错误时则会抛出异常
            this.call(conditionId, methodId);
        } catch (ServiceException | MethodCallStatusException ex)
        {
            m_sLogger.error(ex.getMessage(), ex);
            bResult = false;
            throw ex;
        }
        return bResult;
    }

    /**
     * 报警Disabale失效
     *
     * @param conditionId 报警的ConditionId，可取报警中获取到
     * @return
     * @throws ServiceException
     * @throws MethodCallStatusException
     */
    public boolean Disabale(NodeId conditionId) throws ServiceException, MethodCallStatusException
    {
        boolean bResult = true;
        //NodeId objectId = this.getAddressSpace().getServerTable().;
        NodeId methodId = Identifiers.ConditionType_Disable;
        try
        {
            //ConditionType_Disable方法无返回值，错误时则会抛出异常
            this.call(conditionId, methodId);
        } catch (ServiceException | MethodCallStatusException ex)
        {
            m_sLogger.error(ex.getMessage(), ex);
            bResult = false;
            throw ex;
        }
        return bResult;
    }

    /**
     * 报警AddComment
     *
     * @param conditionId 报警的ConditionId，可取报警中获取到
     * @param eventId 要转换成字节流，具体方法可调用stringToBytes
     * @param comment 备注
     * @return
     * @throws ServiceException
     * @throws MethodCallStatusException
     */
    public boolean AddComment(NodeId conditionId, byte[] eventId, String comment) throws ServiceException, MethodCallStatusException
    {
        boolean bResult = true;
        //NodeId objectId = this.getAddressSpace().getServerTable().;
        NodeId methodId = Identifiers.ConditionType_AddComment;
        Variant[] inputs = new Variant[2];
        inputs[0] = new Variant(eventId);
        inputs[1] = new Variant(new LocalizedText(comment));
//
//        	byte[] eventId = (byte[]) inputArguments[0].getValue();
//                LocalizedText comment = (LocalizedText) inputArguments[1].getValue();
        try
        {
            //ConditionType_AddComment方法无返回值，错误时则会抛出异常
            this.call(conditionId, methodId, inputs);
        } catch (ServiceException | MethodCallStatusException ex)
        {
            m_sLogger.error(ex.getMessage(), ex);
            bResult = false;
            throw ex;
        }
        return bResult;
    }

    /**
     * Acknowledge报警认可
     * 报警承认
     * @param conditionId 报警的ConditionId，可取报警中获取到
     * @param eventId 要转换成字节流，具体方法可调用stringToBytes
     * @param comment 备注
     * @return
     * @throws ServiceException
     * @throws MethodCallStatusException
     */
    public boolean Acknowledge(NodeId conditionId, byte[] eventId, String comment) throws ServiceException, MethodCallStatusException
    {
        boolean bResult = true;
        //NodeId objectId = this.getAddressSpace().getServerTable().;
        NodeId methodId = Identifiers.AcknowledgeableConditionType_Acknowledge;
        Variant[] inputs = new Variant[2];
        inputs[0] = new Variant(eventId);
        inputs[1] = new Variant(new LocalizedText(comment));
        // byte[] eventId = (byte[]) inputArguments[0].getValue();
        //LocalizedText comment = (LocalizedText) inputArguments[1].getValue();
        try
        {
            //AcknowledgeableConditionType_Acknowledge方法无返回值，错误时则会抛出异常
            this.call(conditionId, methodId, inputs);
        } catch (ServiceException | MethodCallStatusException ex)
        {
            m_sLogger.error("XDClien::Acknowledge" + ex.getMessage(), ex);
            bResult = false;
            throw ex;
        }
        return bResult;
    }

    /**
     * Confirm报警确认
     * 使有效，报警证实
     * @param conditionId 报警的ConditionId，可取报警中获取到
     * @param eventId 要转换成字节流，具体方法可调用stringToBytes
     * @param comment 备注
     * @return
     * @throws ServiceException
     * @throws MethodCallStatusException
     */
    public boolean Confirm(NodeId conditionId, byte[] eventId, String comment) throws ServiceException, MethodCallStatusException
    {
        boolean bResult = true;
        //NodeId objectId = this.getAddressSpace().getServerTable().;
        NodeId methodId = Identifiers.AcknowledgeableConditionType_Confirm;
        Variant[] inputs = new Variant[2];
        inputs[0] = new Variant(eventId);
        inputs[1] = new Variant(new LocalizedText(comment));
        // byte[] eventId = (byte[]) inputArguments[0].getValue();
        //LocalizedText comment = (LocalizedText) inputArguments[1].getValue();
        try
        {
            //AcknowledgeableConditionType_Confirm方法无返回值，错误时则会抛出异常
            this.call(conditionId, methodId, inputs);
        } catch (ServiceException | MethodCallStatusException ex)
        {
            m_sLogger.error(ex.getMessage(), ex);
            bResult = false;
            throw ex;
        }
        return bResult;
    }

    /**
     *
     * @param strHex 16进制字符串，不含0x
     * @return
     */
    public byte[] stringToBytes(String strHex)
    {
        if (strHex == null || strHex.trim().equals(""))
        {
            return new byte[0];
        }

        byte[] bytes = new byte[strHex.length() / 2];
        for (int i = 0; i < strHex.length() / 2; i++)
        {
            String subStr = strHex.substring(i * 2, i * 2 + 2);
            bytes[i] = (byte) Integer.parseInt(subStr, 16);
        }

        return bytes;
    }

    public boolean UploadConfigFile(String strConfigFile, EnumConfigFileType enumConfigFileType, ProgressInfo progressInfo) throws FileNotFoundException, IOException
    {
        boolean bResult = false;

        NodeId methodId = new NodeId(2, "UploadConfigFile");
        NodeId objectId = new NodeId(2, "MethodFolder");
        FileInputStream fCfg = new FileInputStream(strConfigFile);
        //        FileOutputStream fCfg = new FileOutputStream(strCfgFileName,true); //追加
        InputStreamReader inputStreamReader = new InputStreamReader(fCfg, "UTF-8");
        int maxBufferLength = 10240;
        int iPostion = 0;
        //byte[] bBuffer = new byte[maxBufferLength];
        char[] bCharBuufer = new char[maxBufferLength];
        if (null != inputStreamReader)
        {
            int iRead = 0;
            if (null != progressInfo)
            {
                progressInfo.setTotal(fCfg.getChannel().size());
            }
            do
            {
                iRead = inputStreamReader.read(bCharBuufer);
                if (iRead > 0)
                {
                    Variant[] inputs = new Variant[5];
                    inputs[0] = new Variant(enumConfigFileType.ordinal());
                    if (iRead < maxBufferLength)
                    {
                        inputs[1] = new Variant(true);
                    } else
                    {
                        inputs[1] = new Variant(false);
                    }
                    inputs[2] = new Variant(new String(bCharBuufer, 0, iRead));
                    inputs[3] = new Variant(iRead);
                    inputs[4] = new Variant(iPostion);
                    Variant[] result = null;
                    try
                    {
                        result = this.call(objectId, methodId, inputs);
                        iPostion += iRead;
                        if (null != progressInfo)
                        {
                            progressInfo.setPregress(iPostion);
                        }
                    } catch (ServiceException | MethodCallStatusException ex)
                    {
                        m_sLogger.error(ex.getMessage(), ex);
                    }
                    if (null != result && result.length > 0 && result[0] != null)
                    {
                        bResult = result[0].booleanValue();
                        if (!bResult)
                        {
                            return bResult;
                        }
                    }

                } else
                {
                    //读取完成；
                }
            } while (iRead >= maxBufferLength);
            inputStreamReader.close();
        }
        return bResult;
    }

    public boolean DownloadConfigFile(String strConfigFile, EnumConfigFileType enumConfigFileType, ProgressInfo progressInfo) throws FileNotFoundException, IOException
    {
        boolean bResult = false;

        NodeId methodId = new NodeId(2, "DownloadConfigFile");
        NodeId objectId = new NodeId(2, "MethodFolder");
        FileOutputStream fCfg = new FileOutputStream(strConfigFile);
         //       FileOutputStream fCfg = new FileOutputStream(strCfgFileName,true); //追加
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fCfg, "UTF-8");
        int startPostion = 0;
        int maxBufferLength = 10240;
        String strBuffer = "";
        boolean isFinished = false;
        if (null != outputStreamWriter)
        {
            do
            {
                Variant[] inputs = new Variant[3];
                inputs[0] = new Variant(enumConfigFileType.ordinal());
                inputs[1] = new Variant(startPostion);
                inputs[2] = new Variant(maxBufferLength);
                Variant[] result = null;
                try
                {
                    result = this.call(objectId, methodId, inputs);
                } catch (ServiceException | MethodCallStatusException ex)
                {
                    m_sLogger.error(ex.getMessage(), ex);
                }
                if (null != result && result.length == 3)
                {
                    strBuffer = result[0].toString();
                    outputStreamWriter.write(strBuffer);
                    isFinished = result[1].booleanValue();
                    startPostion = result[2].intValue();
                    if (null != progressInfo)
                    {
                        progressInfo.setPregress(startPostion);
                    }
                }
                outputStreamWriter.flush();
            } while (!isFinished);
            outputStreamWriter.close();
        }
        return bResult;
    }

    public HistoryEventFieldList[] HistoryEventsReadByStep(NodeId nodeId, DateTime startTime, DateTime endTime,
            UnsignedInteger numValuesPerNode, EventFilter filter, TimestampsToReturn timestampsToReturn, ContinuationPointForHistoryReading continuationPointForHistoryReading) throws ServerConnectionException, ServiceException, DecodingException
    {
        if (null == continuationPointForHistoryReading)
        {
            return null;
        }
        HistoryReadDetails details = new ReadEventDetails(numValuesPerNode, startTime, endTime, filter);

        HistoryReadValueId id = new HistoryReadValueId();
        id.setNodeId(nodeId);
        HistoryEventFieldList[] returnEvents = null;

        //HistoryReadDetails details = new ReadRawModifiedDetails(false, startTime, endTime, UnsignedInteger.valueOf(numValuesPerNode), returnBounds);
        //HistoryReadValueId id = new HistoryReadValueId();
        //id.setNodeId(nodeId);
        //DataValue[] returnArrayDataValue = null;
        if (null != continuationPointForHistoryReading.getContinueReadInfo())
        {
            id.setContinuationPoint(continuationPointForHistoryReading.getContinueReadInfo());
        }

        HistoryReadResult[] results = historyRead(details, timestampsToReturn, false, id);
        if (results[0].getStatusCode().isBad())
        {
            continuationPointForHistoryReading.setErrorMessage("StatusCodeBad:" + results[0].getStatusCode().getDescription());
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setContinueReadInfo(null);
            continuationPointForHistoryReading.setHasReadCount(0);
            return null;
        }
        if (results[0].getStatusCode().getValue().equals(StatusCodes.Good_NoData))
        {
            continuationPointForHistoryReading.setErrorMessage("No Data!");
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setContinueReadInfo(null);
            continuationPointForHistoryReading.setHasReadCount(0);
            return null;
        }
        ExtensionObject historyDataObject = results[0].getHistoryData();
        if (historyDataObject == null)
        {
            continuationPointForHistoryReading.setErrorMessage("No Data!");
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setContinueReadInfo(null);
            continuationPointForHistoryReading.setHasReadCount(0);
            return null;
        }
        HistoryEvent data = historyDataObject.decode(this.getEncoderContext());
        byte[] continuationPoint = results[0].getContinuationPoint();

        if (null != data)
        {
            //dataList.addAll(Arrays.asList(data.getDataValues()));
            returnEvents = data.getEvents();
        }
        if (continuationPoint != null)
        {
            continuationPointForHistoryReading.setFinished(false);
            continuationPointForHistoryReading.setHasReadCount((null == returnEvents ? 0 : returnEvents.length));
            // continuationPointForHistoryReading.setErrorMessage("StatusCodeBad:" + results[0].getStatusCode().getDescription());
            //return data.getEvents();
            if (null == continuationPointForHistoryReading.getContinueReadInfo())
            {
                continuationPointForHistoryReading.setContinueReadInfo(new byte[continuationPoint.length]);
            }
            System.arraycopy(continuationPoint, 0, continuationPointForHistoryReading.getContinueReadInfo(), 0, continuationPoint.length);
        } else
        {
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setHasReadCount((null == returnEvents ? 0 : returnEvents.length));
            continuationPointForHistoryReading.setContinueReadInfo(null);
        }
        return returnEvents;
        //id.setContinuationPoint(continuationPoint);
        //} while (continuationPoint != null);
        //return dataList.toArray(new HistoryEventFieldList[dataList.size()]);
//        return dataList == null ? new HistoryEventFieldList[0]
//                : dataList.toArray(new HistoryEventFieldList[dataList.size()]);

    }

    public DataValue[] HistoryDatasReadByStep(NodeId nodeId, DateTime startTime, DateTime endTime, long numValuesPerNode,
            Boolean returnBounds, NumericRange indexRange, TimestampsToReturn timestampsToReturn, ContinuationPointForHistoryReading continuationPointForHistoryReading) throws ServerConnectionException, ServiceException, DecodingException
    {

        if (null == continuationPointForHistoryReading)
        {
            return null;
        }
        HistoryReadDetails details = new ReadRawModifiedDetails(false, startTime, endTime, UnsignedInteger.valueOf(numValuesPerNode), returnBounds);
        HistoryReadValueId id = new HistoryReadValueId();
        id.setNodeId(nodeId);
        if (indexRange != null)
        {
            id.setIndexRange(indexRange.toString());
        }
        DataValue[] returnArrayDataValue = null;
        if (null != continuationPointForHistoryReading.getContinueReadInfo())
        {
            id.setContinuationPoint(continuationPointForHistoryReading.getContinueReadInfo());
        }

        HistoryReadResult[] results = historyRead(details, timestampsToReturn, false, id);
        if (results[0].getStatusCode().isBad())
        {
            continuationPointForHistoryReading.setErrorMessage("StatusCodeBad:" + results[0].getStatusCode().getDescription());
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setContinueReadInfo(null);
            continuationPointForHistoryReading.setHasReadCount(0);
            return null;
        }
        if (results[0].getStatusCode().getValue().equals(StatusCodes.Good_NoData))
        {
            continuationPointForHistoryReading.setErrorMessage("No Data!");
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setContinueReadInfo(null);
            continuationPointForHistoryReading.setHasReadCount(0);
            return null;
        }
        ExtensionObject historyDataObject = results[0].getHistoryData();
        if (historyDataObject == null)
        {
            continuationPointForHistoryReading.setErrorMessage("No Data!");
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setContinueReadInfo(null);
            continuationPointForHistoryReading.setHasReadCount(0);
            return null;
        }
        HistoryData data = historyDataObject.decode(this.getEncoderContext());
        byte[] continuationPoint = results[0].getContinuationPoint();

        if (null != data)
        {
            //dataList.addAll(Arrays.asList(data.getDataValues()));
            returnArrayDataValue = data.getDataValues();
        }
        if (continuationPoint != null)
        {
            continuationPointForHistoryReading.setFinished(false);
            continuationPointForHistoryReading.setHasReadCount((null == returnArrayDataValue ? 0 : returnArrayDataValue.length));
            //continuationPointForHistoryReading.setTotalReadCount(continuationPointForHistoryReading.getTotalReadCount() + continuationPointForHistoryReading.getHasReadCount());

            if (null == continuationPointForHistoryReading.getContinueReadInfo())
            {
                continuationPointForHistoryReading.setContinueReadInfo(new byte[continuationPoint.length]);
            }
            System.arraycopy(continuationPoint, 0, continuationPointForHistoryReading.getContinueReadInfo(), 0, continuationPoint.length);
        } else
        {
            continuationPointForHistoryReading.setFinished(true);
            continuationPointForHistoryReading.setHasReadCount((null == returnArrayDataValue ? 0 : returnArrayDataValue.length));
            //continuationPointForHistoryReading.setTotalReadCount(continuationPointForHistoryReading.getTotalReadCount() + continuationPointForHistoryReading.getHasReadCount());
            continuationPointForHistoryReading.setContinueReadInfo(null);
        }
        return returnArrayDataValue;
    }

    /**
     * 对象转数组
     *
     * @param obj
     * @return
     */
    private byte[] toByteArray(Object obj)
    {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex)
        {
            m_sLogger.error(ex.getMessage(), ex);
            //ex.printStackTrace();
        }
        return bytes;
    }

    /**
     * 数组转对象
     *
     * @param bytes
     * @return
     */
    private Object toObject(byte[] bytes)
    {
        Object obj = null;
        try
        {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex)
        {
            m_sLogger.error(ex.getMessage(), ex);
        } catch (ClassNotFoundException ex)
        {
            m_sLogger.error(ex.getMessage(), ex);
        }
        return obj;
    }


    public synchronized void connect(XDUaClient client, SecurityMode securityMode) throws URISyntaxException, IOException, SecureIdentityException {
        String APP_NAME = "XDUaClient";


        final PkiFileBasedCertificateValidator validator = new PkiFileBasedCertificateValidator();
        client.setCertificateValidator(validator);
        // ...and react to validation results with a custom handler (to prompt the user what to do, if necessary)
        // *** Application Description is sent to the server
        ApplicationDescription appDescription = new ApplicationDescription();
        // 'localhost' (all lower case) in the ApplicationName and ApplicationURI is converted to the actual host name of the computer
        // in which the application is run
        appDescription.setApplicationName(new LocalizedText(APP_NAME + "@localhost"));
        appDescription.setApplicationUri("urn:localhost:OPCUA:" + APP_NAME);
        appDescription.setProductUri("urn:prosysopc.com:OPCUA:" + APP_NAME);
        appDescription.setApplicationType(ApplicationType.Client);
        File privatePath = new File(validator.getBaseDir(), "private");
        // Create self-signed certificates
        int[] keySizes = null;


        // Create the HTTPS certificate. The HTTPS certificate must be created, if you enable HTTPS.
        String hostName = InetAddress.getLocalHost().getHostName();

        //Define our user locale - the default is Locale.getDefault()
        client.setLocale(Locale.ENGLISH);
        //Define the call timeout in milliseconds. Default is null
        client.setTimeout(30000);
        //StatusCheckTimeout is used to detect communication problems and start automatic reconnection.
        client.setStatusCheckTimeout(10000);
        client.setAutoReconnect(true);
        //Listen to server status changes
        //Set security mode
        client.setSecurityMode(securityMode);
        // Define the security policies for HTTPS; ALL is the default
        client.getHttpsSettings().setHttpsSecurityPolicies(HttpsSecurityPolicy.ALL);
        // Set endpoint configuration parameters
        client.getEndpointConfiguration().setMaxByteStringLength(Integer.MAX_VALUE);
        client.getEndpointConfiguration().setMaxArrayLength(Integer.MAX_VALUE);
        System.out.println("---------------------------------------------INITIALIZED------------------------");

        if (!client.isConnected()) {
            try {
                client.connect();

                System.out.println("---------------------------------------------CONNECTED------------------------");
            } catch (ServiceException e) {
                e.printStackTrace();
                m_sLogger.error("There is an exception when client is connected to server. " + e.toString());
            }
        }
    }
    public void startAlarmEventHandler(List<NodeId> nodeIds, AlarmEventListener alarmEventListener, String host, int port, int timeout) throws ServiceException, StatusException
    {
        QualifiedName[] eventFieldNames =
        {
            new QualifiedName("EventId"),
            new QualifiedName("EventType"), new QualifiedName("LocalTime"), new QualifiedName("Message"),
            new QualifiedName("ReceiveTime"), new QualifiedName("Severity"), new QualifiedName("SourceName"),
            new QualifiedName("SourceNode"), new QualifiedName("BranchId"), new QualifiedName("AddComment"),
            new QualifiedName("ClientUserId"), new QualifiedName("Comment"), new QualifiedName("ConditionId"),
            new QualifiedName("ConditionClassId"), new QualifiedName("ConditionClassName"), new QualifiedName("ConditionName"),
            new QualifiedName("ConditionRefresh"), new QualifiedName("Disable"), new QualifiedName("Enable"),
            new QualifiedName("EnabledState"), new QualifiedName("LastSeverity"), new QualifiedName("Quality"),
            new QualifiedName("Retain"), new QualifiedName("Time"), new QualifiedName("ActiveState/Id"),
            new QualifiedName("AckedState"), new QualifiedName("ConfirmedState"), new QualifiedName("LimitState"),
            new QualifiedName("EventIdentify"), new QualifiedName("UserProperty")
        };
        AlarmEventCacheHandler.getInstance().startAlarmEventDBThread(host, port, timeout);
        //1.创建订阅
        Subscription subscription = new Subscription();

        SubscriptionAliveListener subscriptionAliveListener = new XDSubscriptionAliveListener();
        SubscriptionNotificationListener subscriptionNotificationListener = new XDSubscriptionNotificationListener(eventFieldNames, alarmEventListener);

        subscription.addAliveListener(subscriptionAliveListener);
        subscription.addNotificationListener(subscriptionNotificationListener);

        //2.创建监视项
        MonitorServiceHandler monitorServiceHandler = new MonitorServiceHandler(this, eventFieldNames);

        //3.向订阅添加事件监视项
        for (NodeId nodeId : nodeIds)
        {
            MonitoredEventItem monitoredEventItem = monitorServiceHandler.createMonitoredEventItem(nodeId);
            subscription.addItem(monitoredEventItem);
        }

        this.addSubscription(subscription);

    }

    @Override
    public void disconnect()
    {
        AlarmEventCacheHandler.getInstance().stopAlarmEventDBThread();
        super.disconnect();
    }

    public int getNamespaceIndexByNamespaceUri(String namespaceUri)
    {
        int iNamesapceIndex = -1;
        //iNamesapceIndex = this.getAddressSpace().getNamespaceTable().getIndex(namespaceUri);
        iNamesapceIndex = this.getNamespaceTable().getIndex(namespaceUri);
        return iNamesapceIndex;
    }
}
