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

import java.util.Collection;
import org.apache.commons.lang3.builder.ToStringStyle;

public class BuilderUtils {
    public static final ToStringStyle DEFAULT_STYLE = ToStringStyle.DEFAULT_STYLE;
    public static final ToStringStyle MULTI_LINE_STYLE = ToStringStyle.MULTI_LINE_STYLE;
    public static final ToStringStyle NO_FIELD_NAMES_STYLE = ToStringStyle.NO_FIELD_NAMES_STYLE;
    public static final ToStringStyle SHORT_PREFIX_STYLE = ToStringStyle.SHORT_PREFIX_STYLE;
    public static final ToStringStyle SIMPLE_STYLE = ToStringStyle.SIMPLE_STYLE;
    public static final ToStringStyle NO_CLASS_NAME_STYLE = ToStringStyle.NO_CLASS_NAME_STYLE;
    public static final ToStringStyle JSON_STYLE = ToStringStyle.JSON_STYLE;

    public static boolean reflectionEquals(final Object lhs, final Object rhs, final Collection<String> excludeFields) {
        return org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals(lhs, rhs, excludeFields);
    }

    public static boolean reflectionEquals(final Object lhs, final Object rhs, final String... excludeFields) {
        return org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals(lhs, rhs, excludeFields);
    }

    public static int reflectionHashCode(final Object object, final Collection<String> excludeFields) {
        return org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode(object, excludeFields);
    }

    public static int reflectionHashCode(final Object object, final String... excludeFields) {
        return org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode(object, excludeFields);
    }

    public static String toString(final Object object) {
        return org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(object);
    }

    public static String toString(final Object object, final ToStringStyle style) {
        return org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(object, style);
    }

    public static String toString(final Object object, final ToStringStyle style, final boolean outputTransients) {
        return org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(object, style, outputTransients);
    }


}
