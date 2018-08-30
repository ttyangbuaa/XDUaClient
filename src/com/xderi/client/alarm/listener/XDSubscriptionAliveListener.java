package com.xderi.client.alarm.listener;


import com.prosysopc.ua.client.Subscription;
import com.prosysopc.ua.client.SubscriptionAliveListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : yangtingting
 * @date : 2018/6/25 16:22
 * @modified by :
 * @description :
 **/
public class XDSubscriptionAliveListener implements SubscriptionAliveListener {
    private static Logger logger = LoggerFactory.getLogger(XDSubscriptionAliveListener.class);
    @Override
    public void onAfterCreate(Subscription subscription) {
        // the subscription was (re)created to the server
        // this happens if the subscription was timed out during a communication break and had to be recreated after reconnection
        logger.info("SubscriptionAliveListener-onAfterCreate : Subscription created. ID = " + subscription.getSubscriptionId().getValue());
    }

    @Override
    public void onAlive(Subscription subscription) {
        // the server acknowledged that the connection is alive,although there were no changes to send
    }

    @Override
    public void onTimeout(Subscription subscription) {
        // the server did not acknowledge that the connection is alive, and the maxKeepAliveCount has been exceeded
        logger.warn("SubscriptionAliveListener-onTimeout : Subscription timeout. ID = " + subscription.getSubscriptionId().getValue());
    }
}
