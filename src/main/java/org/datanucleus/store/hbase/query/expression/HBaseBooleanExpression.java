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

import java.util.Arrays;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.query.expression.Expression;

/**
 * Representation of a boolean expression in HBase queries.
 */
public class HBaseBooleanExpression extends HBaseExpression
{
    Filter filter;

    /**
     * Constructor when the expression represents a comparison, between the field and a value.
     * @param familyName Family name
     * @param columnName Column name
     * @param value The value
     * @param op The operator (eq, noteq, lt, gt, etc)
     */
    public HBaseBooleanExpression(String familyName, String columnName, Object value, Expression.Operator op)
    {
        boolean filterIfMissing = true;
        if (op == Expression.OP_NOTEQ)
        {
            // Allow rows if not set since they are "!="
            filterIfMissing = false;
        }

        CompareOp comp = getComparisonOperatorForExpressionOperator(op);
        if (value instanceof String)
        {
            filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(columnName), 
                comp, Bytes.toBytes((String)value));
            ((SingleColumnValueFilter)filter).setFilterIfMissing(filterIfMissing);
        }
        else if (value instanceof Number)
        {
            if (value instanceof Double)
            {
                filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(columnName), 
                    comp, Bytes.toBytes((Double)value));
                ((SingleColumnValueFilter)filter).setFilterIfMissing(filterIfMissing);
            }
            else if (value instanceof Float)
            {
                filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(columnName), 
                    comp, Bytes.toBytes((Float)value));
                ((SingleColumnValueFilter)filter).setFilterIfMissing(filterIfMissing);
            }
            else if (value instanceof Long)
            {
                filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(columnName), 
                    comp, Bytes.toBytes((Long)value));
                ((SingleColumnValueFilter)filter).setFilterIfMissing(filterIfMissing);
            }
            else if (value instanceof Integer)
            {
                filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(columnName), 
                    comp, Bytes.toBytes((Integer)value));
                ((SingleColumnValueFilter)filter).setFilterIfMissing(filterIfMissing);
            }
            else if (value instanceof Short)
            {
                filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(columnName), 
                    comp, Bytes.toBytes((Short)value));
                ((SingleColumnValueFilter)filter).setFilterIfMissing(filterIfMissing);
            }
            else
            {
                throw new NucleusException("Dont currently support use of Literal of type "+value.getClass().getName() + " in expressions");
            }
        }
        else
        {
            throw new NucleusException("Dont currently support use of Literal of type "+value.getClass().getName() + " in expressions");
        }
    }

    public HBaseBooleanExpression(HBaseBooleanExpression expr1, HBaseBooleanExpression expr2, Expression.Operator op)
    {
        if (op == Expression.OP_AND)
        {
            filter = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(expr1.getFilter(), expr2.getFilter()));
        }
        else if (op == Expression.OP_OR)
        {
            filter = new FilterList(Operator.MUST_PASS_ONE, Arrays.asList(expr1.getFilter(), expr2.getFilter()));
        }
        else
        {
            throw new NucleusException("Dont currently support operator "+op + " in boolean expression for HBase");
        }
    }

    public Filter getFilter()
    {
        return filter;
    }

    protected static CompareOp getComparisonOperatorForExpressionOperator(Expression.Operator op)
    {
        if (op == Expression.OP_EQ)
        {
            return CompareOp.EQUAL;
        }
        else if (op == Expression.OP_NOTEQ)
        {
            return CompareOp.NOT_EQUAL;
        }
        else if (op == Expression.OP_GT)
        {
            return CompareOp.GREATER;
        }
        else if (op == Expression.OP_GTEQ)
        {
            return CompareOp.GREATER_OR_EQUAL;
        }
        else if (op == Expression.OP_LT)
        {
            return CompareOp.LESS;
        }
        else if (op == Expression.OP_LTEQ)
        {
            return CompareOp.LESS_OR_EQUAL;
        }
        return  null;
    }
}