package com.proxyapp.temporal.activity;

import com.proxyapp.model.CanonicalMessage;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface DeliverToEdgeActivity {

    @ActivityMethod(name = "TransmitToDevice")
    void deliver(CanonicalMessage message);
}
