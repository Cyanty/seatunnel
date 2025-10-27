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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.internal.JdbcUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;

public class ArrayInjectFunction implements ClickhouseFieldInjectFunction {

    private static final Pattern PATTERN = Pattern.compile("(Array.*)");
    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        Object[] elements = (Object[]) value;
        String type = fieldType.substring(fieldType.indexOf("(") + 1, fieldType.indexOf(")"));

        ClickHouseDataType clickHouseDataType;
        try {
            clickHouseDataType = ClickHouseDataType.valueOf(type);
        } catch (IllegalArgumentException e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    "array inject error, unsupported data type: " + type);
        }
        String sqlTypeName = clickHouseDataType.name();

        statement.setArray(
                index,
                new com.clickhouse.jdbc.types.Array(
                        Collections.unmodifiableList(Arrays.asList(elements.clone())),
                        sqlTypeName,
                        JdbcUtils.convertToSqlType(clickHouseDataType).getVendorTypeNumber()));
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        if (PATTERN.matcher(fieldType).matches()) {
            this.fieldType = fieldType;
            return true;
        }
        return false;
    }
}
