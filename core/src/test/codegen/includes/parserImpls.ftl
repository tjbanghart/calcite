<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->


SqlAlter SqlUploadJarNode(Span s, String scope) :
{
    SqlNode jarPath;
    final List<SqlNode> jarPathsList = new ArrayList<SqlNode>();
}
{
    <UPLOAD> <JAR>
    jarPath = StringLiteral() {
        jarPathsList.add(jarPath);
    }
    (
        <COMMA> jarPath = StringLiteral() {
            jarPathsList.add(jarPath);
        }
    )*
    {
        return new SqlUploadJarNode(s.end(this), scope, jarPathsList);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlIdentifier id;
    final SqlNodeList columnList;
    final SqlNode query;
}
{
    <TABLE> id = CompoundIdentifier()
    (
        columnList = ExtendList()
    |   { columnList = null; }
    )
    (
        <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    |   { query = null; }
    )
    {
        return new SqlCreateTable(s.end(this), id, columnList, query);
    }
}

SqlNode SqlDescribeSpacePower() :
{
}
{
    <DESCRIBE> <SPACE> <POWER> {
        return null;
    }
}

/**
* A sql type name extended basic data type, it has a counterpart basic
* sql type name but always represents as a special alias compared with the standard name.
* We'll want to replace this with a more generic solution at some point but this
* satisfies our immediate need. This fix-up should not be merged upstream.
*
* For tracking https://issues.apache.org/jira/browse/CALCITE-5346
*/
SqlTypeNameSpec BigQuerySqlTypeNames() :
{
    final SqlTypeName typeName;
    final String typeAlias;
    int precision = -1;
}
{
    (
        <STRING> {
            typeName = SqlTypeName.VARCHAR;
            typeAlias = token.image;
            precision = Integer.MAX_VALUE;
        }
    |
        <BYTES> {
            typeName = SqlTypeName.VARBINARY;
            typeAlias = token.image;
            precision = Integer.MAX_VALUE;
        }
    |
       <FLOAT64> {
            typeName = SqlTypeName.DOUBLE;
            typeAlias = token.image;
        }
    |
       <BOOL> {
            typeName = SqlTypeName.BOOLEAN;
            typeAlias = token.image;
       }
    |
       <INT64> {
            typeName = SqlTypeName.BIGINT;
            typeAlias = token.image;
       }
    )
    {
        return new SqlAlienSystemTypeNameSpec(typeAlias, typeName, precision, getPos());
    }
}
