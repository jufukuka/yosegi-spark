/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.utils;

import jp.co.yahoo.yosegi.message.formatter.json.JacksonMessageWriter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ProjectionPushdownUtil{
  private final List<Object> pushdownColumnNameList = new ArrayList<>();

  public static String createProjectionPushdownJson( final StructType requiredSchema ){
    StructField[] fields = requiredSchema.fields();
    StringBuffer buffer = new StringBuffer();
    buffer.append( "[" );
    for( int i = 0 ; i < fields.length ; i++ ){
      if( i != 0){
        buffer.append( "," );
      }
      buffer.append( String.format( "[\"%s\"]" , fields[i].name() ) );
    }
    buffer.append( "]" );
    return buffer.toString();
  }

  public String toYosegiPushdownJson(final StructType schema) {
    try {
      addPushdownNode(schema);
      return new String(new JacksonMessageWriter().create(pushdownColumnNameList));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void addPushdownNode(final StructType schema) {
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      PushdownNode currentNode = new PushdownNode(fields[i].name());
      addChildPushdownNode(currentNode, fields[i]);
      currentNode.toColumnNameArray(pushdownColumnNameList, new ArrayList<>());
    }
  }

  private void addChildPushdownNode(final PushdownNode currentNode, final StructField field) {
    switch (field.dataType().typeName()) {
      case "array":
        DataType elementType = ((ArrayType) field.dataType()).elementType();
        if (elementType.typeName().equals("struct")) {
          StructField[] fields = ((StructType) elementType).fields();
          for ( int i = 0 ; i < fields.length ; i++ ) {
            PushdownNode childNode = new PushdownNode(fields[i].name());
            currentNode.addChild(childNode);
            addChildPushdownNode(childNode, fields[i]);
          }
        }
        break;
      case "struct":
        StructField[] fields = ((StructType) field.dataType()).fields();
        for (int i = 0 ; i < fields.length ; i++) {
          PushdownNode childNode = new PushdownNode(fields[i].name());
          currentNode.addChild(childNode);
          addChildPushdownNode(childNode, fields[i]);
        }
        break;
      default:
        break;
    }
  }

  private class PushdownNode {
    final String name;
    final List<PushdownNode> childList;

    public PushdownNode(final String name) {
      this.name = name;
      childList = new ArrayList<>();
    }

    public void addChild(final PushdownNode node) {
      childList.add(node);
    }

    public void toColumnNameArray(List<Object> pushdownColumnNameList, List<String> parentColumnNameList) {
      parentColumnNameList.add(name);
      if (childList.isEmpty()) {
        pushdownColumnNameList.add(new ArrayList<>(parentColumnNameList));
      } else {
        for (PushdownNode child : childList) {
          child.toColumnNameArray(pushdownColumnNameList, parentColumnNameList);
        }
      }
      parentColumnNameList.remove(parentColumnNameList.size() - 1);
    }
  }
}
