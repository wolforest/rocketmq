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

package org.apache.rocketmq.common.utils;

import java.util.Properties;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class PropertyUtilsTest {

    @Test
    public void testProperties2Object() {
        DemoConfig demoConfig = new DemoConfig();
        Properties properties = new Properties();
        properties.setProperty("demoWidth", "123");
        properties.setProperty("demoLength", "456");
        properties.setProperty("demoOK", "true");
        properties.setProperty("demoName", "TestDemo");
        PropertyUtils.properties2Object(properties, demoConfig);
        assertThat(demoConfig.getDemoLength()).isEqualTo(456);
        assertThat(demoConfig.getDemoWidth()).isEqualTo(123);
        assertThat(demoConfig.isDemoOK()).isTrue();
        assertThat(demoConfig.getDemoName()).isEqualTo("TestDemo");
    }

    @Test
    public void testProperties2String() {
        DemoSubConfig demoConfig = new DemoSubConfig();
        demoConfig.setDemoLength(123);
        demoConfig.setDemoWidth(456);
        demoConfig.setDemoName("TestDemo");
        demoConfig.setDemoOK(true);

        demoConfig.setSubField0("1");
        demoConfig.setSubField1(false);

        Properties properties = PropertyUtils.object2Properties(demoConfig);
        assertThat(properties.getProperty("demoLength")).isEqualTo("123");
        assertThat(properties.getProperty("demoWidth")).isEqualTo("456");
        assertThat(properties.getProperty("demoOK")).isEqualTo("true");
        assertThat(properties.getProperty("demoName")).isEqualTo("TestDemo");

        assertThat(properties.getProperty("subField0")).isEqualTo("1");
        assertThat(properties.getProperty("subField1")).isEqualTo("false");

        properties = PropertyUtils.object2Properties(new Object());
        assertEquals(0, properties.size());
    }

    @Test
    public void testIsPropertiesEqual() {
        final Properties p1 = new Properties();
        final Properties p2 = new Properties();

        p1.setProperty("a", "1");
        p1.setProperty("b", "2");
        p2.setProperty("a", "1");
        p2.setProperty("b", "2");

        assertThat(PropertyUtils.isPropertiesEqual(p1, p2)).isTrue();
    }

    static class DemoConfig {
        private int demoWidth = 0;
        private int demoLength = 0;
        private boolean demoOK = false;
        private String demoName = "haha";

        int getDemoWidth() {
            return demoWidth;
        }

        public void setDemoWidth(int demoWidth) {
            this.demoWidth = demoWidth;
        }

        public int getDemoLength() {
            return demoLength;
        }

        public void setDemoLength(int demoLength) {
            this.demoLength = demoLength;
        }

        public boolean isDemoOK() {
            return demoOK;
        }

        public void setDemoOK(boolean demoOK) {
            this.demoOK = demoOK;
        }

        public String getDemoName() {
            return demoName;
        }

        public void setDemoName(String demoName) {
            this.demoName = demoName;
        }

        @Override
        public String toString() {
            return "DemoConfig{" +
                "demoWidth=" + demoWidth +
                ", demoLength=" + demoLength +
                ", demoOK=" + demoOK +
                ", demoName='" + demoName + '\'' +
                '}';
        }
    }

    static class DemoSubConfig extends DemoConfig {
        private String subField0 = "0";
        public boolean subField1 = true;

        public String getSubField0() {
            return subField0;
        }

        public void setSubField0(String subField0) {
            this.subField0 = subField0;
        }

        public boolean isSubField1() {
            return subField1;
        }

        public void setSubField1(boolean subField1) {
            this.subField1 = subField1;
        }
    }


}
