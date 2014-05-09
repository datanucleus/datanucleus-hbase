/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
   ...
**********************************************************************/
package org.datanucleus.store.hbase.query.expression;

/**
 * Expression for a field in a HBase datastore.
 */
public class HBaseFieldExpression extends HBaseExpression
{
    final Class type;
    final String familyName;
    final String columnName;

    public HBaseFieldExpression(Class type, String familyName, String columnName)
    {
        this.type = type;
        this.familyName = familyName;
        this.columnName = columnName;
    }

    public Class getType()
    {
        return type;
    }

    public String getFamilyName()
    {
        return familyName;
    }

    public String getColumnName()
    {
        return columnName;
    }
}