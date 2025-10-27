/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;
import com.clickhouse.data.value.array.ClickHouseByteArrayValue;
import com.clickhouse.data.value.array.ClickHouseIntArrayValue;
import com.clickhouse.data.value.array.ClickHouseLongArrayValue;
import com.clickhouse.data.value.array.ClickHouseShortArrayValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TypeConvertUtil {

    public static SeaTunnelDataType<?> convert(ClickHouseColumn column) {
        if (column.isArray()) {
            ClickHouseColumn subArrayDataType = column.getNestedColumns().get(0);
            SeaTunnelDataType<?> dataType = convert(subArrayDataType);
            if (BasicType.INT_TYPE.equals(dataType)) {
                return ArrayType.INT_ARRAY_TYPE;
            } else if (BasicType.STRING_TYPE.equals(dataType)) {
                return ArrayType.STRING_ARRAY_TYPE;
            } else if (BasicType.FLOAT_TYPE.equals(dataType)) {
                return ArrayType.FLOAT_ARRAY_TYPE;
            } else if (BasicType.DOUBLE_TYPE.equals(dataType)) {
                return ArrayType.DOUBLE_ARRAY_TYPE;
            } else if (BasicType.LONG_TYPE.equals(dataType)) {
                return ArrayType.LONG_ARRAY_TYPE;
            } else if (BasicType.SHORT_TYPE.equals(dataType)) {
                return ArrayType.SHORT_ARRAY_TYPE;
            } else if (BasicType.BOOLEAN_TYPE.equals(dataType)) {
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            } else if (BasicType.BYTE_TYPE.equals(dataType)) {
                return ArrayType.BYTE_ARRAY_TYPE;
            } else {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "data type in array is not supported: " + subArrayDataType.getDataType());
            }
        }
        Class<?> type = column.getDataType().getObjectClass();
        if (Integer.class.equals(type) || UnsignedShort.class.equals(type)) {
            return BasicType.INT_TYPE;
        } else if (Long.class.equals(type) || UnsignedInteger.class.equals(type)) {
            return BasicType.LONG_TYPE;
        } else if (Short.class.equals(type) || UnsignedByte.class.equals(type)) {
            return BasicType.SHORT_TYPE;
        } else if (Byte.class.equals(type)) {
            return BasicType.BYTE_TYPE;
        } else if (Boolean.class.equals(type)) {
            return BasicType.BOOLEAN_TYPE;
        } else if (LocalDate.class.equals(type)) {
            return LocalTimeType.LOCAL_DATE_TYPE;
        } else if (LocalDateTime.class.equals(type)) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        } else if (BigDecimal.class.equals(type)) {
            return new DecimalType(column.getPrecision(), column.getScale());
        } else if (String.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Float.class.equals(type)) {
            return BasicType.FLOAT_TYPE;
        } else if (Double.class.equals(type)) {
            return BasicType.DOUBLE_TYPE;
        } else if (Map.class.equals(type)) {
            return new MapType<>(
                    convert(column.getNestedColumns().get(0)),
                    convert(column.getNestedColumns().get(1)));
        } else if (UUID.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Inet4Address.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Inet6Address.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (Object.class.equals(type) || List.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else if (BigInteger.class.equals(type) || UnsignedLong.class.equals(type)) {
            return BasicType.STRING_TYPE;
        } else {
            // TODO support pojo
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    "unsupported data type: " + column.getDataType());
        }
    }

    public static Object valueUnwrap(SeaTunnelDataType<?> dataType, ClickHouseValue record) {
        if (dataType instanceof DecimalType) {
            return record.asBigDecimal();
        } else if (dataType.equals(BasicType.BOOLEAN_TYPE)) {
            return record.asBoolean();
        } else if (dataType.equals(BasicType.INT_TYPE)) {
            return record.asInteger();
        } else if (dataType.equals(BasicType.LONG_TYPE)) {
            return record.asLong();
        } else if (dataType.equals(BasicType.SHORT_TYPE)) {
            return record.asShort();
        } else if (dataType.equals(BasicType.BYTE_TYPE)) {
            return record.asByte();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TYPE)) {
            return record.asDate();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
            return record.asDateTime();
        } else if (dataType.equals(BasicType.STRING_TYPE)) {
            return record.asString();
        } else if (dataType.equals(BasicType.FLOAT_TYPE)) {
            return record.asFloat();
        } else if (dataType.equals(BasicType.DOUBLE_TYPE)) {
            return record.asDouble();
        } else if (dataType instanceof MapType) {
            return record.asMap();
        } else if (dataType instanceof ArrayType) {
            Class<?> typeClass = dataType.getTypeClass();
            if (String[].class.equals(typeClass)) {
                if (record instanceof ClickHouseLongArrayValue) {
                    Object[] objects = record.asArray();
                    if (objects instanceof UnsignedLong[]) {
                        return Arrays.stream(objects).map(Object::toString).toArray(String[]::new);
                    }
                }
                return record.asArray(String.class);
            } else if (Boolean[].class.equals(typeClass)) {
                return record.asArray(Boolean.class);
            } else if (Byte[].class.equals(typeClass)) {
                return record.asArray(Byte.class);
            } else if (Short[].class.equals(typeClass)) {
                if (record instanceof ClickHouseByteArrayValue) {
                    Object[] objects = record.asArray();
                    if (objects instanceof UnsignedByte[]) {
                        return Arrays.stream(objects)
                                .map(v -> ((UnsignedByte) v).shortValue())
                                .toArray(Short[]::new);
                    }
                }
                return record.asArray(Short.class);
            } else if (Integer[].class.equals(typeClass)) {
                if (record instanceof ClickHouseShortArrayValue) {
                    Object[] objects = record.asArray();
                    if (objects instanceof UnsignedShort[]) {
                        return Arrays.stream(objects)
                                .map(v -> ((UnsignedShort) v).intValue())
                                .toArray(Integer[]::new);
                    }
                }
                return record.asArray(Integer.class);
            } else if (Long[].class.equals(typeClass)) {
                if (record instanceof ClickHouseIntArrayValue) {
                    Object[] objects = record.asArray();
                    if (objects instanceof UnsignedInteger[]) {
                        return Arrays.stream(objects)
                                .map(v -> ((UnsignedInteger) v).longValue())
                                .toArray(Long[]::new);
                    }
                }
                return record.asArray(Long.class);
            } else if (Float[].class.equals(typeClass)) {
                return record.asArray(Float.class);
            } else if (Double[].class.equals(typeClass)) {
                return record.asArray(Double.class);
            } else {
                return record.asArray();
            }
        } else {
            // TODO support pojo
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    "unsupported data type: " + dataType);
        }
    }
}
