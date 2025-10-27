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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.exception.CommonError;

import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StringInjectFunction implements ClickhouseFieldInjectFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        try {
            if ("Point".equals(fieldType)) {
                double[] point = MAPPER.readValue(replace(value.toString()), double[].class);
                statement.setObject(index, ClickHouseGeoPointValue.of(point).toSqlExpression());
            } else if ("Ring".equals(fieldType)) {
                double[][] ring = MAPPER.readValue(replace(value.toString()), double[][].class);
                statement.setObject(index, ClickHouseGeoRingValue.of(ring).toSqlExpression());
            } else if ("Polygon".equals(fieldType)) {
                double[][][] polygon =
                        MAPPER.readValue(replace(value.toString()), double[][][].class);
                statement.setObject(index, ClickHouseGeoPolygonValue.of(polygon).toSqlExpression());
            } else if ("MultiPolygon".equals(fieldType)) {
                double[][][][] multiPolygon =
                        MAPPER.readValue(replace(value.toString()), double[][][][].class);
                statement.setObject(
                        index, ClickHouseGeoMultiPolygonValue.of(multiPolygon).toSqlExpression());
            } else if ("JSON".equals(fieldType)) {
                statement.setString(
                        index,
                        value instanceof String
                                ? value.toString()
                                : MAPPER.writeValueAsString(value));
            } else {
                statement.setString(index, value.toString());
            }
        } catch (JsonProcessingException e) {
            throw CommonError.jsonOperationError("Clickhouse", value.toString(), e);
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        if ("String".equals(fieldType)
                || "UInt64".equals(fieldType)
                || "Int128".equals(fieldType)
                || "UInt128".equals(fieldType)
                || "Int256".equals(fieldType)
                || "UInt256".equals(fieldType)
                || "Point".equals(fieldType)
                || "Ring".equals(fieldType)
                || "Polygon".equals(fieldType)
                || "MultiPolygon".equals(fieldType)
                || "JSON".equals(fieldType)) {
            this.fieldType = fieldType;
            return true;
        }
        return false;
    }

    private static String replace(String str) {
        return str.replaceAll("\\(", "[").replaceAll("\\)", "]");
    }
}
