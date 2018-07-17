/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test correct support of nested struct in table when converting from RelNode to SqlNode
 */
public class RelToSqlNestedStruct {

  private static final Schema MY_SCHEMA = new AbstractSchema() {

    @Override protected Map<String, Table> getTableMap() {
      return ImmutableMap.of("myTable", MY_TABLE);
    }

  };

  private static final Table MY_TABLE = new Table() {

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataType bType =
          typeFactory.createSqlType(SqlTypeName.INTEGER);
      RelDataType aType =
          typeFactory.createStructType(ImmutableList.of(bType), ImmutableList.of("b"));
      return typeFactory.createStructType(ImmutableList.of(aType), ImmutableList.of("a"));
    }

    @Override public Statistic getStatistic() {
      return ZERO_STATISTICS;
    }

    @Override public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    @Override public boolean isRolledUp(String column) {
      return false;
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                          SqlCall call,
                                                          SqlNode parent,
                                                          CalciteConnectionConfig config) {
      return false;
    }

  };

  private static final Statistic ZERO_STATISTICS = new Statistic() {

    @Override public Double getRowCount() {
      return 0D;
    }

    @Override public boolean isKey(ImmutableBitSet columns) {
      return false;
    }

    @Override public List<RelReferentialConstraint> getReferentialConstraints() {
      return ImmutableList.of();
    }

    @Override public List<RelCollation> getCollations() {
      return ImmutableList.of();
    }

    @Override public RelDistribution getDistribution() {
      return null;
    }

  };

  private static final SchemaPlus ROOT_SCHEMA =
      CalciteSchema.createRootSchema(false).add("mySchema", MY_SCHEMA).plus();

  @Test public void testTableStruct()
      throws RelConversionException, ValidationException, SqlParseException {

    FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(ROOT_SCHEMA)
            .build();
    Planner planner = Frameworks.getPlanner(config);

    // "myTable" schema is nested:
    // {
    //     a: {
    //         b: INTEGER
    //         }
    //     }
    // }
    SqlNode sqlNode = planner.parse("SELECT * FROM \"myTable\"");
    SqlNode validate = planner.validate(sqlNode);
    RelRoot relRoot = planner.rel(validate);
    RelNode node = relRoot.rel;

    RelToSqlConverter converter = new RelToSqlConverter(HiveSqlDialect.DEFAULT);
    SqlNode resSql = converter.visitChild(0, node).asStatement();
  }
}
// End RelToSqlNestedStruct.java
