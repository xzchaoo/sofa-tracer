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
package com.alipay.sofa.tracer.boot.redis.configuration;

import com.alipay.sofa.tracer.boot.configuration.SofaTracerAutoConfiguration;
import com.sofa.alipay.tracer.plugins.spring.redis.SofaTracerRCFBeanPostProcessor;
import com.sofa.alipay.tracer.plugins.spring.redis.common.RedisActionWrapperHelper;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * @author: guolei.sgl (guolei.sgl@antfin.com) 2019/11/19 8:03 PM
 * @since:
 **/
@Configuration
@ConditionalOnClass({ RedisConnectionFactory.class, RedisActionWrapperHelper.class })
@ConditionalOnProperty(name = "com.alipay.sofa.tracer.redis.enabled", havingValue = "true", matchIfMissing = true)
@AutoConfigureAfter(SofaTracerAutoConfiguration.class)
public class SofaTracerRedisConfiguration {

    @Bean
    @ConditionalOnMissingBean
    RedisActionWrapperHelper redisActionWrapperHelper() {
        return new RedisActionWrapperHelper();
    }

    @Bean
    @ConditionalOnMissingBean
    SofaTracerRCFBeanPostProcessor sofaTracerRCFBeanPostProcessor(RedisActionWrapperHelper redisActionWrapperHelper) {
        return new SofaTracerRCFBeanPostProcessor(redisActionWrapperHelper);
    }
}
