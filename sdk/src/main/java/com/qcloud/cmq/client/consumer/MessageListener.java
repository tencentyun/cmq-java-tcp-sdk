package com.qcloud.cmq.client.consumer;

import java.util.List;

public interface MessageListener {
    List<Long> consumeMessage(final String queue, final List<Message> msgs);
}
