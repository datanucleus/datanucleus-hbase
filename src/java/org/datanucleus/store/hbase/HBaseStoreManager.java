/**********************************************************************
Copyright (c) 2009 Erik Bengtson and others. All rights reserved.
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
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.metadata.TableGeneratorMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.schema.SchemaAwareStoreManager;

public class HBaseStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    MetaDataListener metadataListener;

    private Configuration hbaseConfig; 

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param ctx context
     * @param props Properties for the datastore
     */
    public HBaseStoreManager(ClassLoaderResolver clr, NucleusContext ctx, Map<String, Object> props)
    {
        super("hbase", clr, ctx, props);

        persistenceHandler = new HBasePersistenceHandler(this);

        // Add listener so we can check all metadata for unsupported features and required schema
        metadataListener = new HBaseMetaDataListener(this);
        MetaDataManager mmgr = nucleusContext.getMetaDataManager();
        mmgr.registerListener(metadataListener);
        Collection<String> classNamesLoaded = mmgr.getClassesWithMetaData();
        if (classNamesLoaded != null && classNamesLoaded.size() > 0)
        {
            Iterator<String> iter = classNamesLoaded.iterator();
            while (iter.hasNext())
            {
                AbstractClassMetaData cmd = mmgr.getMetaDataForClass(iter.next(), clr);
                metadataListener.loaded(cmd);
            }
        }

        logConfiguration();
    }

    protected void registerConnectionMgr()
    {
        super.registerConnectionMgr();
        this.connectionMgr.disableConnectionPool();
    }

    public void close()
    {
        nucleusContext.getMetaDataManager().deregisterListener(metadataListener);
        super.close();
    }

    public NucleusConnection getNucleusConnection(ExecutionContext om)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Accessor for the supported options in string form.
     * @return Supported options for this store manager
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("DatastoreIdentity");
        set.add("NonDurableIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        set.add("ORM");
        return set;
    }

    public Configuration getHbaseConfig()
    {
        if (hbaseConfig == null)
        {
            hbaseConfig = HBaseConfiguration.create();
        }
        return hbaseConfig;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getClassNameForObjectID(java.lang.Object, org.datanucleus.ClassLoaderResolver, org.datanucleus.store.ExecutionContext)
     */
    @Override
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr, ExecutionContext ec)
    {
        // TODO Override this so we can check inheritance level of provided id
        return super.getClassNameForObjectID(id, clr, ec);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#addClasses(java.lang.String[], org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public void addClasses(String[] classNames, ClassLoaderResolver clr)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = 
            getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE)
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        registerStoreData(newStoreData(cmd, clr));
                    }

                    if (isAutoCreateTables() || isAutoCreateColumns())
                    {
                        HBaseUtils.createSchemaForClass(this, cmd, false);
                    }
                }
            }
        }
    }

    /**
     * Method to return the properties to pass to the generator for the specified field.
     * Takes the superclass properties and adds on the "table-name" where appropriate.
     * @param cmd MetaData for the class
     * @param absoluteFieldNumber Number of the field (-1 = datastore identity)
     * @param ec execution context
     * @param seqmd Any sequence metadata
     * @param tablegenmd Any table generator metadata
     * @return The properties to use for this field
     */
    protected Properties getPropertiesForGenerator(AbstractClassMetaData cmd, int absoluteFieldNumber,
            ExecutionContext ec, SequenceMetaData seqmd, TableGeneratorMetaData tablegenmd)
    {
        Properties props = super.getPropertiesForGenerator(cmd, absoluteFieldNumber, ec, seqmd, tablegenmd);

        IdentityStrategy strategy = null;
        if (absoluteFieldNumber >= 0)
        {
            // real field
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);
            strategy = mmd.getValueStrategy();
        }
        else
        {
            // datastore-identity surrogate field
            // always use the root IdentityMetaData since the root class defines the identity
            IdentityMetaData idmd = cmd.getBaseIdentityMetaData();
            strategy = idmd.getValueStrategy();
        }

        String tableName = getNamingFactory().getTableName(cmd);
        props.setProperty("table-name", tableName);
        if (strategy == IdentityStrategy.INCREMENT && tablegenmd != null)
        {
            // User has specified a TableGenerator (JPA)
            // Using JPA generator so don't enable initial value detection
            props.remove("table-name");
        }

        return props;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#createSchema(java.util.Set, java.util.Properties)
     */
    public void createSchema(Set<String> classNames, Properties props)
    {
        if (isAutoCreateTables() || isAutoCreateColumns())
        {
            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    HBaseUtils.createSchemaForClass(this, cmd, false);
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#deleteSchema(java.util.Set)
     */
    public void deleteSchema(Set<String> classNames, Properties props)
    {
        Iterator<String> classIter = classNames.iterator();
        ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
        while (classIter.hasNext())
        {
            String className = classIter.next();
            AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
            if (cmd != null)
            {
                HBaseUtils.deleteSchemaForClass(this, cmd);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#validateSchema(java.util.Set)
     */
    public void validateSchema(Set<String> classNames, Properties props)
    {
        if (isValidateTables() || isValidateColumns())
        {
            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    HBaseUtils.createSchemaForClass(this, cmd, true);
                }
            }
        }
    }
}