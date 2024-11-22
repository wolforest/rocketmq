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
package org.apache.rocketmq.broker.server.bootstrap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.migration.AuthMigrator;
import org.apache.rocketmq.broker.api.auth.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.broker.api.auth.pipeline.AuthorizationPipeline;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;

public class BrokerAuthService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final Broker broker;
    private final AuthConfig authConfig;

    private AuthenticationMetadataManager authenticationMetadataManager;
    private AuthorizationMetadataManager authorizationMetadataManager;

    public BrokerAuthService(final Broker broker) {
        this.broker = broker;
        this.authConfig = broker.getAuthConfig();

        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(authConfig);
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(authConfig);

        if (this.authConfig != null && this.authConfig.isMigrateAuthFromV1Enabled()) {
            new AuthMigrator(this.authConfig).migrate();
        }
    }

    public void initialize() {
        this.initialRequestPipeline();
    }

    public void start() {
    }

    public void initialRequestPipeline() {
        if (this.authConfig == null) {
            return;
        }
        RequestPipeline pipeline = (ctx, request) -> {
        };
        // add pipeline
        // the last pipe add will execute at the first
        try {
            pipeline = pipeline.pipe(new AuthorizationPipeline(authConfig))
                .pipe(new AuthenticationPipeline(authConfig));
            this.setRequestPipeline(pipeline);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setRequestPipeline(RequestPipeline pipeline) {
        this.broker.getBrokerNettyServer().getRemotingServer().setRequestPipeline(pipeline);
        this.broker.getBrokerNettyServer().getFastRemotingServer().setRequestPipeline(pipeline);
    }

    public void shutdown() {
        if (this.authenticationMetadataManager != null) {
            this.authenticationMetadataManager.shutdown();
        }

        if (this.authorizationMetadataManager != null) {
            this.authorizationMetadataManager.shutdown();
        }
    }

    public AuthenticationMetadataManager getAuthenticationMetadataManager() {
        return authenticationMetadataManager;
    }

    @VisibleForTesting
    public void setAuthenticationMetadataManager(
        AuthenticationMetadataManager authenticationMetadataManager) {
        this.authenticationMetadataManager = authenticationMetadataManager;
    }

    public AuthorizationMetadataManager getAuthorizationMetadataManager() {
        return authorizationMetadataManager;
    }

    @VisibleForTesting
    public void setAuthorizationMetadataManager(
        AuthorizationMetadataManager authorizationMetadataManager) {
        this.authorizationMetadataManager = authorizationMetadataManager;
    }


}
