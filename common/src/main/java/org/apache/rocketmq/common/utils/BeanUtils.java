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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class BeanUtils {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static void printObjectProperties(final Logger logger, final Object object) {
        printObjectProperties(logger, object, false);
    }

    public static void printObjectProperties(final Logger logger, final Object object,
        final boolean onlyImportantField) {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            String name = field.getName();
            if (name.startsWith("this")) {
                continue;
            }

            if (onlyImportantField) {
                Annotation annotation = field.getAnnotation(ImportantField.class);
                if (null == annotation) {
                    continue;
                }
            }

            Object value = null;
            try {
                field.setAccessible(true);
                value = field.get(object);
                if (null == value) {
                    value = "";
                }
            } catch (IllegalAccessException e) {
                log.error("Failed to obtain object properties", e);
            }

            if (logger != null) {
                logger.info(name + "=" + value);
            }
        }
    }

    public static String properties2String(final Properties properties) {
        return properties2String(properties, false);
    }

    public static String properties2String(final Properties properties, final boolean isSort) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<Object, Object>> entrySet = isSort ? new TreeMap<>(properties).entrySet() : properties.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            if (entry.getValue() == null) {
                continue;
            }

            sb.append(entry.getKey().toString())
                .append("=")
                .append(entry.getValue().toString())
                .append("\n");
        }
        return sb.toString();
    }

    public static Properties string2Properties(final String str) {
        Properties properties = new Properties();
        try {
            InputStream in = new ByteArrayInputStream(str.getBytes(MQConstants.DEFAULT_CHARSET));
            properties.load(in);
        } catch (Exception e) {
            log.error("Failed to handle properties", e);
            return null;
        }

        return properties;
    }

    public static Properties object2Properties(final Object object) {
        Properties properties = new Properties();

        Class<?> objectClass = object.getClass();
        while (true) {
            object2Properties(object, objectClass, properties);

            if (objectClass == Object.class || objectClass.getSuperclass() == Object.class) {
                break;
            }
            objectClass = objectClass.getSuperclass();
        }

        return properties;
    }

    private static void object2Properties(final Object object, final Class<?> objectClass, Properties properties) {
        Field[] fields = objectClass.getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            String name = field.getName();
            if (name.startsWith("this")) {
                continue;
            }

            Object value = null;
            try {
                field.setAccessible(true);
                value = field.get(object);
            } catch (IllegalAccessException e) {
                log.error("Failed to handle properties", e);
            }

            if (value != null) {
                properties.setProperty(name, value.toString());
            }
        }
    }

    public static void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (!mn.startsWith("set")) {
                continue;
            }

            try {
                String tmp = mn.substring(4);
                String first = mn.substring(3, 4);

                String key = first.toLowerCase() + tmp;
                String property = p.getProperty(key);
                if (property == null) {
                    continue;
                }

                Class<?>[] pt = method.getParameterTypes();
                if (pt.length <= 0) {
                    continue;
                }

                String cn = pt[0].getSimpleName();
                Object arg = getArg(cn, property);
                if (arg == null) {
                    continue;
                }

                method.invoke(object, arg);
            } catch (Throwable ignored) {
            }
        }
    }

    private static Object getArg(String cn, String property) {
        Object arg;
        if (cn.equals("int") || cn.equals("Integer")) {
            arg = Integer.parseInt(property);
        } else if (cn.equals("long") || cn.equals("Long")) {
            arg = Long.parseLong(property);
        } else if (cn.equals("double") || cn.equals("Double")) {
            arg = Double.parseDouble(property);
        } else if (cn.equals("boolean") || cn.equals("Boolean")) {
            arg = Boolean.parseBoolean(property);
        } else if (cn.equals("float") || cn.equals("Float")) {
            arg = Float.parseFloat(property);
        } else if (cn.equals("String")) {
            property = property.trim();
            arg = property;
        } else {
            return null;
        }

        return arg;
    }

    public static boolean isPropertiesEqual(final Properties p1, final Properties p2) {
        return p1.equals(p2);
    }

    public static boolean isPropertyValid(Properties props, String key, Predicate<String> validator) {
        return validator.test(props.getProperty(key));
    }
}
