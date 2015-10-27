/**********************************************************************
Copyright (c) 2011 Peter Rainer and others. All rights reserved.
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
package org.datanucleus.store.hbase.valuegenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.hbase.HBaseStoreManager;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Generator that uses a table in HBase to store and allocate identity values.
 */
public class IncrementGenerator extends AbstractDatastoreGenerator<Long>
{
    static final String INCREMENT_COL_NAME = "increment";

    /** Key used in the Table to access the increment count */
    private String key;

    private Table table;

    private String tableNameString = null;

    /**
     * Constructor. Will receive the following properties (as a minimum) through this constructor.
     * <ul>
     * <li>class-name : Name of the class whose object is being inserted.</li>
     * <li>root-class-name : Name of the root class in this inheritance tree</li>
     * <li>field-name : Name of the field with the strategy (unless datastore identity field)</li>
     * <li>catalog-name : Catalog of the table (if specified)</li>
     * <li>schema-name : Schema of the table (if specified)</li>
     * <li>table-name : Name of the root table for this inheritance tree (containing the field).</li>
     * <li>column-name : Name of the column in the table (for the field)</li>
     * <li>sequence-name : Name of the sequence (if specified in MetaData as "sequence)</li>
     * </ul>
     * @param name Symbolic name for this generator
     * @param props Properties controlling the behaviour of the generator (or null if not required).
     */
    public IncrementGenerator(String name, Properties props)
    {
        super(name, props);
        this.key = properties.getProperty("field-name", name);
        this.tableNameString = properties.getProperty("sequence-table-name");
        if (this.tableNameString == null)
        {
            this.tableNameString = "IncrementTable";
        }
        if (properties.containsKey("key-cache-size"))
        {
            allocationSize = Integer.valueOf(properties.getProperty("key-cache-size"));
        }
        else
        {
            allocationSize = 1;
        }
    }

    private synchronized void initialiseTable()
    {
        if (this.table == null)
        {
            try
            {
                TableName tableName = TableName.valueOf(tableNameString);
                HBaseStoreManager hbaseMgr = (HBaseStoreManager) storeMgr;
                Configuration config = hbaseMgr.getHbaseConfig();
                HBaseAdmin admin = new HBaseAdmin(config);
                try
                {
                    if (!admin.tableExists(this.tableNameString))
                    {
                        if (!storeMgr.getSchemaHandler().isAutoCreateTables())
                        {
                            throw new NucleusUserException(Localiser.msg("040011", tableNameString));
                        }

                        NucleusLogger.VALUEGENERATION.debug("IncrementGenerator: Creating Table '" + this.tableNameString + "'");
                        HTableDescriptor ht = new HTableDescriptor(tableName);
                        HColumnDescriptor hcd = new HColumnDescriptor(INCREMENT_COL_NAME);
                        hcd.setCompressionType(Compression.Algorithm.NONE);
                        hcd.setMaxVersions(1);
                        ht.addFamily(hcd);
                        admin.createTable(ht);
                    }
                }
                finally
                {
                    admin.close();
                }

                this.table = new HTable(config, this.tableNameString);
                if (!this.table.exists(new Get(Bytes.toBytes(key))))
                {
                    long initialValue = 0;
                    if (properties.containsKey("key-initial-value"))
                    {
                        initialValue = Long.valueOf(properties.getProperty("key-initial-value"))-1;
                    }
                    this.table.put(new Put(Bytes.toBytes(key)).addColumn(Bytes.toBytes(INCREMENT_COL_NAME), Bytes.toBytes(INCREMENT_COL_NAME), Bytes.toBytes(initialValue)));
                }
            }
            catch (IOException ex)
            {
                NucleusLogger.VALUEGENERATION.fatal("Error instantiating IncrementGenerator", ex);
            }
        }
    }

    public String getName()
    {
        return this.name;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractGenerator#reserveBlock(long)
     */
    protected synchronized ValueGenerationBlock<Long> reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        if (this.table == null)
        {
            this.initialiseTable();
        }

        // Allocate value(s)
        long number;
        List<Long> oids = new ArrayList<Long>();
        try
        {
            number = table.incrementColumnValue(Bytes.toBytes(key), Bytes.toBytes(INCREMENT_COL_NAME), Bytes.toBytes(INCREMENT_COL_NAME), size);
            long nextNumber = number - size + 1;
            for (int i=0;i<size;i++)
            {
                oids.add(nextNumber++);
            }
        }
        catch (IOException ex)
        {
            NucleusLogger.VALUEGENERATION.error("IncrementGenerator: Error incrementing generated value", ex);
            throw new NucleusDataStoreException("Error incrementing generated value.", ex);
        }
        return new ValueGenerationBlock<Long>(oids);
    }
}