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
package org.apache.rocketmq.auth.authorization.model;

import org.apache.rocketmq.common.domain.resource.ResourcePattern;
import org.apache.rocketmq.common.domain.resource.ResourceType;
import org.junit.Assert;
import org.junit.Test;

public class ResourceTest {

    @Test
    public void parseResource() {
        Resource resource = Resource.of("*");
        Assert.assertEquals(resource.getResourceType(), ResourceType.ANY);
        Assert.assertNull(resource.getResourceName());
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.ANY);

        resource = Resource.of("Topic:*");
        Assert.assertEquals(resource.getResourceType(), ResourceType.TOPIC);
        Assert.assertNull(resource.getResourceName());
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.ANY);

        resource = Resource.of("Topic:test-*");
        Assert.assertEquals(resource.getResourceType(), ResourceType.TOPIC);
        Assert.assertEquals(resource.getResourceName(), "test-");
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.PREFIXED);

        resource = Resource.of("Topic:test-1");
        Assert.assertEquals(resource.getResourceType(), ResourceType.TOPIC);
        Assert.assertEquals(resource.getResourceName(), "test-1");
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.LITERAL);
    }

    @Test
    public void isMatch() {

    }
}
