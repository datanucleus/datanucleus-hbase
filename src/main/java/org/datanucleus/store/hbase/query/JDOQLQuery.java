/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
2009 Erik Bengtson - original stub using in-memory evaluation
    ...
***********************************************************************/
package org.datanucleus.store.hbase.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.Filter;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.hbase.HBaseManagedConnection;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.hbase.query.expression.HBaseBooleanExpression;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.query.QueryManager;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Implementation of JDOQL for HBase datastores.
 */
public class JDOQLQuery extends AbstractJDOQLQuery
{
    /** The compilation of the query for this datastore. Not applicable if totally in-memory. */
    protected transient HBaseQueryCompilation datastoreCompilation;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec Execution Context
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JDOQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec Execution Context
     * @param q The query from which to copy criteria.
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, JDOQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec Execution Context
     * @param query The query string
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    /**
     * Utility to remove any previous compilation of this Query.
     */
    protected void discardCompiled()
    {
        super.discardCompiled();

        datastoreCompilation = null;
    }

    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    protected boolean isCompiled()
    {
        if (evaluateInMemory())
        {
            // Don't need datastore compilation here since evaluating in-memory
            return compilation != null;
        }
        else
        {
            // Need both to be present to say "compiled"
            if (compilation == null || datastoreCompilation == null)
            {
                return false;
            }
            return true;
        }
    }

    /**
     * Convenience method to return whether the query should be evaluated in-memory.
     * @return Use in-memory evaluation?
     */
    protected boolean evaluateInMemory()
    {
        if (candidateCollection != null)
        {
            if (compilation != null && compilation.getSubqueryAliases() != null)
            {
                // TODO In-memory evaluation of subqueries isn't fully implemented yet, so remove this when it is
                NucleusLogger.QUERY.warn("In-memory evaluator doesn't currently handle subqueries completely so evaluating in datastore");
                return false;
            }

            Object val = getExtension(EXTENSION_EVALUATE_IN_MEMORY);
            if (val == null)
            {
                return true;
            }
            return Boolean.valueOf((String)val);
        }
        return super.evaluateInMemory();
    }

    /**
     * Method to compile the JDOQL query.
     * Uses the superclass to compile the generic query populating the "compilation", and then generates
     * the datastore-specific "datastoreCompilation".
     * @param parameterValues Map of param values keyed by param name (if available at compile time)
     */
    protected synchronized void compileInternal(Map parameterValues)
    {
        if (isCompiled())
        {
            return;
        }

        // Compile the generic query expressions
        super.compileInternal(parameterValues);

        boolean inMemory = evaluateInMemory();
        if (candidateCollection != null && inMemory)
        {
            // Querying a candidate collection in-memory, so just return now (don't need datastore compilation)
            return;
        }

        if (candidateClass == null)
        {
            throw new NucleusUserException(Localiser.msg("021009", candidateClassName));
        }

        // Make sure any persistence info is loaded
        ec.hasPersistenceInformationForClass(candidateClass);

        QueryManager qm = getQueryManager();
        String datastoreKey = getStoreManager().getQueryCacheKey();
        String cacheKey = getQueryCacheKey();
        if (useCaching())
        {
            // Allowing caching so try to find compiled (datastore) query
            datastoreCompilation = (HBaseQueryCompilation)qm.getDatastoreQueryCompilation(datastoreKey, getLanguage(), cacheKey);
            if (datastoreCompilation != null)
            {
                // Cached compilation exists for this datastore so reuse it
                setResultDistinct(compilation.getResultDistinct());
                return;
            }
        }

        datastoreCompilation = new HBaseQueryCompilation();
        AbstractClassMetaData cmd = getCandidateClassMetaData();
        synchronized (datastoreCompilation)
        {
            if (inMemory)
            {
                // Generate statement to just retrieve all candidate objects for later processing
            }
            else
            {
                // Try to generate statement to perform the full query in the datastore
                compileQueryFull(parameterValues, cmd);
            }
        }

        if (datastoreCompilation.isPrecompilable() && cacheKey != null)
        {
            qm.addDatastoreQueryCompilation(datastoreKey, getLanguage(), cacheKey, datastoreCompilation);
        }
    }

    protected AbstractClassMetaData getCandidateClassMetaData()
    {
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
        if (candidateClass.isInterface())
        {
            // Query of interface
            String[] impls = ec.getMetaDataManager().getClassesImplementingInterface(candidateClass.getName(), clr);
            if (impls.length == 1 && cmd.isImplementationOfPersistentDefinition())
            {
                // Only the generated implementation, so just use its metadata
            }
            else
            {
                // Use metadata for the persistent interface
                cmd = ec.getMetaDataManager().getMetaDataForInterface(candidateClass, clr);
                if (cmd == null)
                {
                    throw new NucleusUserException("Attempting to query an interface yet it is not declared 'persistent'." +
                        " Define the interface in metadata as being persistent to perform this operation, and make sure" +
                        " any implementations use the same identity and identity member(s)");
                }
            }
        }

        return cmd;
    }

    protected Object performExecute(Map parameters)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) getStoreManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", "JDOQL", getSingleStringQuery(), null));
            }

            boolean filterInMemory = true;
            List candidates = null;
            if (candidateCollection != null)
            {
                candidates = new ArrayList(candidateCollection);
            }
            else
            {
                // Generate the HBase Filter, from the query filter plus any necessary discriminator restriction
                Filter filter = null;

                HBaseBooleanExpression filterExpr = null;
                AbstractClassMetaData cmd = getCandidateClassMetaData();
                if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null)
                {
                    if ("true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
                    {
                        // Don't bother with multitenancy for this class
                    }
                    else
                    {
                        // Add filter on discriminator for this tenant
                        Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
                        String tableName = table.getName();
                        String name = table.getMultitenancyColumn().getName();
                        String familyName = HBaseUtils.getFamilyNameForColumnName(name, tableName);
                        String qualifName = HBaseUtils.getQualifierNameForColumnName(name);
                        String value = storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID);
                        filterExpr = new HBaseBooleanExpression(familyName, qualifName, value, Expression.OP_EQ);
                    }
                }
                if (datastoreCompilation != null)
                {
                    if (datastoreCompilation.isFilterComplete())
                    {
                        HBaseBooleanExpression userFilterExpr = datastoreCompilation.getFilterExpression();
                        if (filterExpr == null)
                        {
                            filterExpr = userFilterExpr;
                        }
                        else if (userFilterExpr == null)
                        {
                            // Nothing to do
                        }
                        else
                        {
                            filterExpr = new HBaseBooleanExpression(filterExpr, userFilterExpr, Expression.OP_AND);
                        }
                    }
                }

                if (filterExpr != null)
                {
                    filter = filterExpr.getFilter();
                }

                candidates = HBaseQueryUtils.getObjectsOfCandidateType(ec, mconn, candidateClass, subclasses, ignoreCache, getFetchPlan(), filter, storeMgr);
                if (filter != null && datastoreCompilation.isFilterComplete())
                {
                    filterInMemory = false;
                }
            }

            // Apply any other restrictions not handled in the datastore
            JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, candidates, compilation, parameters, ec.getClassLoaderResolver());
            Collection results = resultMapper.execute(filterInMemory, true, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", "JDOQL", "" + (System.currentTimeMillis() - startTime)));
            }

            if (type == BULK_DELETE)
            {
                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == BULK_UPDATE)
            {
                // TODO Support BULK UPDATE
                throw new NucleusException("Bulk Update is not yet supported");
            }
            else
            {
                return results;
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to compile the query for the datastore attempting to evaluate the whole query in the datastore
     * if possible. Sets the components of the "datastoreCompilation".
     * @param parameters Input parameters (if known)
     * @param candidateCmd Metadata for the candidate class
     */
    private void compileQueryFull(Map parameters, AbstractClassMetaData candidateCmd)
    {
        long startTime = 0;
        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            startTime = System.currentTimeMillis();
            NucleusLogger.QUERY.debug(Localiser.msg("021083", getLanguage(), toString()));
        }

        // Create a Scan object with filter, result etc as appropriate
        QueryToHBaseMapper mapper = new QueryToHBaseMapper(compilation, parameters, candidateCmd, ec, this);
        mapper.compile();
        datastoreCompilation.setFilterExpression(mapper.getFilterExpression());
        datastoreCompilation.setFilterComplete(mapper.isFilterComplete());
        datastoreCompilation.setPrecompilable(mapper.isPrecompilable());

        // Apply any range
        if (range != null)
        {
        }

        // Set any extensions (TODO Support locking if possible with HBase)

        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(Localiser.msg("021084", getLanguage(), System.currentTimeMillis()-startTime));
        }
    }
}