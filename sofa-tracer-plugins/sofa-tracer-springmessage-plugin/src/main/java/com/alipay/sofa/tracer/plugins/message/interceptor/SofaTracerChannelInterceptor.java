/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.tracer.plugins.message.interceptor;

import com.alipay.common.tracer.core.constants.SofaTracerConstant;
import com.alipay.common.tracer.core.context.span.SofaTracerSpanContext;
import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.CommonSpanTags;
import com.alipay.common.tracer.core.span.SofaTracerSpan;
import com.alipay.sofa.tracer.plugins.message.tracers.MessagePubTracer;
import com.alipay.sofa.tracer.plugins.message.tracers.MessageSubTracer;

import org.apache.rocketmq.common.message.MessageClientExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.ExecutorChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Map;

import io.opentracing.tag.Tags;

/**
 * @author: guolei.sgl (guolei.sgl@antfin.com) 2019/12/5 3:20 PM
 * @since:
 **/
public class SofaTracerChannelInterceptor implements ChannelInterceptor, ExecutorChannelInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SofaTracerChannelInterceptor.class);

    private static final String REMOTE_SERVICE_NAME = "broker";

    public static final String STREAM_DIRECT_CHANNEL = "org.springframework.cloud.stream.messaging.DirectWithAttributesChannel";

    final         boolean  integrationObjectSupportPresent;
    private final boolean  hasDirectChannelClass;
    // special case of a Stream
    private final Class<?> directWithAttributesChannelClass;

    private static final String SPAN_CONTEXT_KEY = "STREAM_SPAM_CONTEXT_SOFA";

    private static final String ORIGINAL_ROCKETMQ_MESSAGE_KEY = "ORIGINAL_ROCKETMQ_MESSAGE";

    private final        MessageSubTracer messageSubTracer;
    private final        MessagePubTracer messagePubTracer;
    private static final Method           GET_APPLICATION_CONTEXT = ReflectionUtils
            .findMethod(
                    IntegrationObjectSupport.class,
                    "getApplicationContext");
    private static final boolean          BINDING_SERVICE_PRESENT = ClassUtils
            .isPresent(
                    "org.springframework.cloud.stream.binding.BindingService",
                    null);
    private static final Method           GET_BINDER;

    private static final Class<?> ROCKETMQ_BINDER_CLASS = ClassUtils
            .isPresent(
                    "org.springframework.cloud.stream.binder.rocketmq.RocketMQMessageChannelBinder",
                    null) ? ClassUtils
            .resolveClassName(
                    "org.springframework.cloud.stream.binder.rocketmq.RocketMQMessageChannelBinder",
                    null)
            : null;

    static {
        GET_APPLICATION_CONTEXT.setAccessible(true);
        if (BINDING_SERVICE_PRESENT) {
            GET_BINDER = ReflectionUtils.findMethod(BindingService.class, "getBinder",
                    String.class, Class.class);
            GET_BINDER.setAccessible(true);
        } else {
            GET_BINDER = null;
        }
    }

    private final String applicationName;

    private BindingService bindingService;

    SofaTracerChannelInterceptor(String applicationName) {
        this.integrationObjectSupportPresent = ClassUtils.isPresent(
                "org.springframework.integration.context.IntegrationObjectSupport", null);
        this.hasDirectChannelClass = ClassUtils.isPresent(
                "org.springframework.integration.channel.DirectChannel", null);
        this.directWithAttributesChannelClass = ClassUtils.isPresent(STREAM_DIRECT_CHANNEL, null) ? ClassUtils
                .resolveClassName(STREAM_DIRECT_CHANNEL, null) : null;
        messageSubTracer = MessageSubTracer.getMessageSubTracerSingleton();
        messagePubTracer = MessagePubTracer.getMessagePubTracerSingleton();
        this.applicationName = applicationName;
    }

    public static SofaTracerChannelInterceptor create(String applicationName) {
        return new SofaTracerChannelInterceptor(applicationName);
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        if (emptyMessage(message)) {
            return message;
        }
        Message<?> retrievedMessage = getMessage(message);
        MessageHeaderAccessor headers = mutableHeaderAccessor(retrievedMessage);
        Object spanContextSerialize = parseSpanContext(headers);
        SofaTracerSpan sofaTracerSpan;
        if (spanContextSerialize instanceof String) {
            SofaTracerSpanContext spanContext = SofaTracerSpanContext
                    .deserializeFromString(spanContextSerialize.toString());
            sofaTracerSpan = messageSubTracer.serverReceive(spanContext);
            sofaTracerSpan.setOperationName("mq-message-receive");
        } else {
            sofaTracerSpan = messagePubTracer.clientSend("mq-message-send");
        }
        String binderName = getBinder(channel);
        sofaTracerSpan.setTag("binder.type", binderName);
        // 塞回到 headers 中去
        headers.setHeader(SPAN_CONTEXT_KEY, sofaTracerSpan.getSofaTracerSpanContext()
                .serializeSpanContext());
        Message<?> outputMessage = outputMessage(message, retrievedMessage, headers);
        if (isDirectChannel(channel)) {
            beforeHandle(outputMessage, channel, null);
        }
        return outputMessage;
    }

    private String getBinder(MessageChannel channel) {
        if (BINDING_SERVICE_PRESENT && channel instanceof AbstractMessageChannel) {
            AbstractMessageChannel amc = (AbstractMessageChannel) channel;
            try {
                if (bindingService == null) {
                    if (channel instanceof AbstractMessageChannel) {
                        ApplicationContext applicationContext = (ApplicationContext) GET_APPLICATION_CONTEXT
                                .invoke(channel);
                        bindingService = applicationContext.getBean(BindingService.class);
                    }
                }
                if (bindingService != null) {
                    Object binder = GET_BINDER.invoke(bindingService, amc.getFullChannelName(),
                            amc.getClass());
                    if (ROCKETMQ_BINDER_CLASS.isAssignableFrom(binder.getClass())) {
                        return "ROCKETMQ";
                    }
                }
            } catch (Throwable e) {
                LOGGER.error("Fail to get binder from channel {}", channel, e);
            }
        }
        return "UNKNOWN";
    }

    private Object parseSpanContext(MessageHeaderAccessor headers) {
        Object spanContext = null;
        // adapter for rocketmq
        if (headers.getHeader(ORIGINAL_ROCKETMQ_MESSAGE_KEY) instanceof MessageClientExt) {
            MessageClientExt msg = (MessageClientExt) headers
                    .getHeader(ORIGINAL_ROCKETMQ_MESSAGE_KEY);
            Map<String, String> properties = msg.getProperties();
            if (properties.containsKey(SPAN_CONTEXT_KEY)) {
                spanContext = properties.get(SPAN_CONTEXT_KEY);
            }
        }
        return spanContext;
    }

    @Override
    public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent,
                                    Exception ex) {
        if (emptyMessage(message)) {
            return;
        }
        if (isDirectChannel(channel)) {
            afterMessageHandled(message, channel, null, ex);
        } else {
            finishSpan(ex, message, channel);
        }
    }

    @Override
    public Message<?> beforeHandle(Message<?> message, MessageChannel channel,
                                   MessageHandler handler) {
        if (emptyMessage(message)) {
            return message;
        }
        MessageHeaderAccessor headers = mutableHeaderAccessor(message);
        if (message instanceof ErrorMessage) {
            return new ErrorMessage((Throwable) message.getPayload(), headers.getMessageHeaders());
        }
        headers.setImmutable();
        return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
    }

    @Override
    public void afterMessageHandled(Message<?> message, MessageChannel channel,
                                    MessageHandler handler, Exception ex) {
        if (emptyMessage(message)) {
            return;
        }
        finishSpan(ex, message, channel);
    }

    private void appendTags(Message<?> message, MessageChannel channel,
                            SofaTracerSpan sofaTracerSpan) {

        Message<?> retrievedMessage = getMessage(message);
        MessageHeaderAccessor headers = mutableHeaderAccessor(retrievedMessage);
        String messageId = message.getHeaders().getId().toString();
        if (headers.getHeader(ORIGINAL_ROCKETMQ_MESSAGE_KEY) instanceof MessageClientExt) {
            MessageClientExt msg = (MessageClientExt) headers
                    .getHeader(ORIGINAL_ROCKETMQ_MESSAGE_KEY);
            messageId = msg.getMsgId();
            String topic = msg.getTopic();
            sofaTracerSpan.setTag(CommonSpanTags.MSG_TOPIC, topic);
        }
        String channelName = channelName(channel);
        sofaTracerSpan.setTag(CommonSpanTags.MSG_ID, messageId);
        sofaTracerSpan.setTag(CommonSpanTags.MSG_CHANNEL, channelName);
        sofaTracerSpan.setTag(CommonSpanTags.REMOTE_APP, REMOTE_SERVICE_NAME);
        sofaTracerSpan.setTag(CommonSpanTags.LOCAL_APP, applicationName);
    }

    private String channelName(MessageChannel channel) {
        String name = null;
        if (this.integrationObjectSupportPresent) {
            if (channel instanceof IntegrationObjectSupport) {
                name = ((IntegrationObjectSupport) channel).getComponentName();
            }
            if (name == null && channel instanceof AbstractMessageChannel) {
                name = ((AbstractMessageChannel) channel).getFullChannelName();
            }
        }
        if (name == null) {
            name = channel.toString();
        }
        return name;
    }

    private void finishSpan(Exception error, Message<?> message, MessageChannel channel) {
        SofaTraceContext sofaTraceContext = SofaTraceContextHolder.getSofaTraceContext();
        SofaTracerSpan currentSpan = sofaTraceContext.pop();
        if (currentSpan == null) {
            return;
        }
        appendTags(message, channel, currentSpan);
        String resultCode = SofaTracerConstant.RESULT_CODE_SUCCESS;
        if (error != null) {
            String exMessage = error.getMessage();
            if (exMessage == null) {
                exMessage = error.getClass().getSimpleName();
            }
            resultCode = SofaTracerConstant.RESULT_CODE_ERROR;
            currentSpan.setTag(Tags.ERROR.getKey(), exMessage);
        }
        currentSpan.setTag(CommonSpanTags.RESULT_CODE, resultCode);
        currentSpan.finish();
        // 恢复上下文
        if (currentSpan.getParentSofaTracerSpan() != null) {
            sofaTraceContext.push(currentSpan.getParentSofaTracerSpan());
        }
    }

    private boolean emptyMessage(Message<?> message) {
        return message == null;
    }

    private Message<?> getMessage(Message<?> message) {
        Object payload = message.getPayload();
        if (payload instanceof MessagingException) {
            MessagingException e = (MessagingException) payload;
            return e.getFailedMessage();
        }
        return message;
    }

    private MessageHeaderAccessor mutableHeaderAccessor(Message<?> message) {
        MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
        headers.setLeaveMutable(true);
        return headers;
    }

    private boolean isWebSockets(MessageHeaderAccessor headerAccessor) {
        return headerAccessor.getMessageHeaders().containsKey("stompCommand")
                || headerAccessor.getMessageHeaders().containsKey("simpMessageType");
    }

    private Message<?> outputMessage(Message<?> originalMessage, Message<?> retrievedMessage,
                                     MessageHeaderAccessor additionalHeaders) {
        MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(originalMessage);
        if (originalMessage.getPayload() instanceof MessagingException) {
            return new ErrorMessage((MessagingException) originalMessage.getPayload(),
                    isWebSockets(headers) ? headers.getMessageHeaders() : new MessageHeaders(
                            headers.getMessageHeaders()));
        }
        headers.copyHeaders(additionalHeaders.getMessageHeaders());
        return new GenericMessage<>(retrievedMessage.getPayload(),
                isWebSockets(headers) ? headers.getMessageHeaders() : new MessageHeaders(
                        headers.getMessageHeaders()));
    }

    private boolean isDirectChannel(MessageChannel channel) {
        Class<?> targetClass = AopUtils.getTargetClass(channel);
        boolean directChannel = this.hasDirectChannelClass
                && DirectChannel.class.isAssignableFrom(targetClass);
        if (!directChannel) {
            return false;
        }
        if (this.directWithAttributesChannelClass == null) {
            return true;
        }
        return !isStreamSpecialDirectChannel(targetClass);
    }

    private boolean isStreamSpecialDirectChannel(Class<?> targetClass) {
        return this.directWithAttributesChannelClass.isAssignableFrom(targetClass);
    }
}
