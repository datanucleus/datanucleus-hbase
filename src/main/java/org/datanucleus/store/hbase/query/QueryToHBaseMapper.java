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
package org.datanucleus.store.hbase.query;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.hbase.query.expression.HBaseBooleanExpression;
import org.datanucleus.store.hbase.query.expression.HBaseExpression;
import org.datanucleus.store.hbase.query.expression.HBaseFieldExpression;
import org.datanucleus.store.hbase.query.expression.HBaseLiteral;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Class which maps a compiled (generic) query to a HBase query.
 */
public class QueryToHBaseMapper extends AbstractExpressionEvaluator
{
    final ExecutionContext ec;

    final String candidateAlias;

    final AbstractClassMetaData candidateCmd;

    final Query query;

    final QueryCompilation compilation;

    /** Input parameter values, keyed by the parameter name. Will be null if compiled pre-execution. */
    final Map parameters;

    /** Work Map for keying parameter value for the name, for case where parameters input as positional. */
    Map<String, Object> parameterValueByName = null;

    Map<Integer, String> paramNameByPosition = null;

    /** Positional parameter that we are up to (-1 implies not being used). */
    int positionalParamNumber = -1;

    /** State variable for the component being compiled. */
    CompilationComponent compileComponent;

    /** Whether the filter clause is completely evaluatable in the datastore. */
    boolean filterComplete = true;

    /** The filter expression. If no filter is required then this will be null. */
    HBaseBooleanExpression filterExpr = null;

    /**
     * State variable for whether this query is precompilable (hence whether it is cacheable).
     * Or in other words, whether we can compile it without knowing parameter values.
     */
    boolean precompilable = true;

    Deque<HBaseExpression> stack = new ArrayDeque<HBaseExpression>();

    public QueryToHBaseMapper(QueryCompilation compilation, Map parameters, AbstractClassMetaData cmd,
            ExecutionContext ec, Query q)
    {
        this.ec = ec;
        this.query = q;
        this.compilation = compilation;
        this.parameters = parameters;
        this.candidateCmd = cmd;
        this.candidateAlias = compilation.getCandidateAlias();
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public HBaseBooleanExpression getFilterExpression()
    {
        return filterExpr;
    }

    public void compile()
    {
        compileFilter();
    }

    /**
     * Accessor for whether the query is precompilable (doesn't need to know parameter values
     * to be able to compile it).
     * @return Whether the query is precompilable and hence cacheable
     */
    public boolean isPrecompilable()
    {
        return precompilable;
    }

    /**
     * Method to compile the WHERE clause of the query
     */
    protected void compileFilter()
    {
        if (compilation.getExprFilter() != null)
        {
            compileComponent = CompilationComponent.FILTER;

            try
            {
                compilation.getExprFilter().evaluate(this);
                HBaseExpression filterExpr = stack.pop();
                if (filterExpr instanceof HBaseBooleanExpression)
                {
                    this.filterExpr = (HBaseBooleanExpression)filterExpr;
                }
                else
                {
                    NucleusLogger.QUERY.error(">> invalid compilation : filter compiled to " + filterExpr);
                    filterComplete = false;
                }
            }
            catch (Exception e)
            {
                // Impossible to compile all to run in the datastore, so just exit
                filterComplete = false;
                NucleusLogger.QUERY.debug(">> compileFilter caught exception ", e);
            }

            compileComponent = null;
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processOrExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processOrExpression(Expression expr)
    {
        HBaseBooleanExpression right = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression left = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression orExpr = new HBaseBooleanExpression(left, right, Expression.OP_OR);
        stack.push(orExpr);
        return orExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processAndExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processAndExpression(Expression expr)
    {
        HBaseBooleanExpression right = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression left = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression andExpr = new HBaseBooleanExpression(left, right, Expression.OP_AND);
        stack.push(andExpr);
        return andExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processEqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processEqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_EQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_EQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processEqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNoteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_NOTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_NOTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processNoteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_LTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_GT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processGtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_GTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_LT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processLtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_LT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_GTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processGteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_GT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            Object litVal = TypeConversionHelper.convertTo(litExpr.getValue(), fieldExpr.getType());
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litVal, Expression.OP_LTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processLteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processPrimaryExpression(org.datanucleus.query.expression.PrimaryExpression)
     */
    @Override
    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
        Expression left = expr.getLeft();
        if (left == null)
        {
            List<String> tuples = expr.getTuples();
            PrimaryDetails primDetails = getFamilyColumnNameForPrimary(tuples);
            if (primDetails == null)
            {
                if (compileComponent == CompilationComponent.FILTER)
                {
                    filterComplete = false;
                }
                NucleusLogger.QUERY.debug(">> Primary " + expr +
                    " is not stored in this document, so unexecutable in datastore");
            }
            else
            {
                HBaseFieldExpression fieldExpr = new HBaseFieldExpression(primDetails.type, primDetails.family, primDetails.column);
                stack.push(fieldExpr);
                return fieldExpr;
            }
        }

        // TODO Auto-generated method stub
        return super.processPrimaryExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processParameterExpression(org.datanucleus.query.expression.ParameterExpression)
     */
    @Override
    protected Object processParameterExpression(ParameterExpression expr)
    {
        if (expr.getPosition() >= 0)
        {
            if (paramNameByPosition == null)
            {
                paramNameByPosition = new HashMap<Integer, String>();
            }
            paramNameByPosition.put(Integer.valueOf(expr.getPosition()), expr.getId());
        }

        Object paramValue = null;
        boolean paramValueSet = false;
        if (parameters != null && !parameters.isEmpty())
        {
            // Check if the parameter has a value
            if (parameters.containsKey(expr.getId()))
            {
                // Named parameter
                paramValue = parameters.get(expr.getId());
                paramValueSet = true;
            }
            else if (parameterValueByName != null && parameterValueByName.containsKey(expr.getId()))
            {
                // Positional parameter, but already encountered
                paramValue = parameterValueByName.get(expr.getId());
                paramValueSet = true;
            }
            else
            {
                // Positional parameter, not yet encountered
                int position = positionalParamNumber;
                if (positionalParamNumber < 0)
                {
                    position = 0;
                }
                if (parameters.containsKey(Integer.valueOf(position)))
                {
                    paramValue = parameters.get(Integer.valueOf(position));
                    paramValueSet = true;
                    positionalParamNumber = position+1;
                    if (parameterValueByName == null)
                    {
                        parameterValueByName = new HashMap<String, Object>();
                    }
                    parameterValueByName.put(expr.getId(), paramValue);
                }
            }
        }

        if (paramValueSet && (paramValue instanceof Number || paramValue instanceof String))
        {
            HBaseLiteral paramLit = new HBaseLiteral(paramValue);
            precompilable = false;
            stack.push(paramLit);
            return paramLit;
        }

        // TODO Auto-generated method stub
        return super.processParameterExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLiteral(org.datanucleus.query.expression.Literal)
     */
    @Override
    protected Object processLiteral(Literal expr)
    {
        Object litValue = expr.getLiteral();
        if (litValue instanceof Number)
        {
            HBaseLiteral lit = new HBaseLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        else if (litValue instanceof String)
        {
            HBaseLiteral lit = new HBaseLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        // TODO Handle all HBase supported (literal) types

        return super.processLiteral(expr);
    }

    static class PrimaryDetails
    {
        String family;
        String column;
        Class type;
        public PrimaryDetails(Class type, String fam, String col)
        {
            this.type = type;
            this.family = fam;
            this.column = col;
        }
    }

    /**
     * Convenience method to return the "{familyName}###{columnName}" in candidate for this primary.
     * Allows for non-relation fields, and (nested) embedded PC fields - i.e all fields that are present
     * in the table.
     * @param tuples Tuples for the primary
     * @return The family+column name for this primary (or null if not resolvable in this document)
     */
    protected PrimaryDetails getFamilyColumnNameForPrimary(List<String> tuples)
    {
        if (tuples == null || tuples.isEmpty())
        {
            return null;
        }

        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
        AbstractMemberMetaData embMmd = null;
        boolean firstTuple = true;
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData cmd = candidateCmd;
        Table table = ec.getStoreManager().getStoreDataForClass(candidateCmd.getFullClassName()).getTable();

        Iterator<String> iter = tuples.iterator();
        while (iter.hasNext())
        {
            String name = iter.next();
            if (firstTuple && name.equals(candidateAlias))
            {
                cmd = candidateCmd;
            }
            else
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(name);
                RelationType relationType = mmd.getRelationType(ec.getClassLoaderResolver());
                if (relationType == RelationType.NONE)
                {
                    if (iter.hasNext())
                    {
                        throw new NucleusUserException("Query has reference to " +
                            StringUtils.collectionToString(tuples) + " yet " + name + " is a non-relation field!");
                    }

                    Column col = null;
                    if (embMmd != null)
                    {
                        // Get property name for field of embedded object
                        embMmds.add(mmd);
                        col = table.getMemberColumnMappingForEmbeddedMember(embMmds).getColumn(0);
                    }
                    else
                    {
                        col = table.getMemberColumnMappingForMember(mmd).getColumn(0);
                    }
                    String familyName = HBaseUtils.getFamilyNameForColumn(col);
                    String qualifName = HBaseUtils.getQualifierNameForColumn(col);
                    return new PrimaryDetails(mmd.getType(), familyName, qualifName);
                }
                else
                {
                    boolean embedded = MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, 
                        embMmds.isEmpty() ? null : embMmds.get(embMmds.size()-1));

                    if (embedded)
                    {
                        if (RelationType.isRelationSingleValued(relationType))
                        {
                            cmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), ec.getClassLoaderResolver());
                            if (embMmd != null)
                            {
                                embMmd = embMmd.getEmbeddedMetaData().getMemberMetaData()[mmd.getAbsoluteFieldNumber()];
                            }
                            else
                            {
                                embMmd = mmd;
                            }
                            embMmds.add(embMmd);
                        }
                        else if (RelationType.isRelationMultiValued(relationType))
                        {
                            throw new NucleusUserException("Do not support the querying of embedded collection/map/array fields : " + mmd.getFullFieldName());
                        }
                    }
                    else
                    {
                        // Not embedded
                        embMmds.clear();

                        if (compileComponent == CompilationComponent.FILTER)
                        {
                            filterComplete = false;
                        }
                        NucleusLogger.QUERY.debug("Query has reference to " + StringUtils.collectionToString(tuples) + " and " + mmd.getFullFieldName() +
                            " is not persisted into this document, so unexecutable in the datastore");
                        return null;
                    }
                }
            }
            firstTuple = false;
        }

        return null;
    }
}