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

Contributors:
   ...
**********************************************************************/
package org.datanucleus.store.hbase;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ValueGenerationStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.util.NucleusLogger;

/**
 * Listener for the load of metadata for classes.
 * Allows us to reject metadata when it isn't supported by this datastore.
 */
public class HBaseMetaDataListener implements MetaDataListener
{
    HBaseMetaDataListener(HBaseStoreManager storeManager)
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.metadata.MetaDataListener#loaded(org.datanucleus.metadata.AbstractClassMetaData)
     */
    public void loaded(AbstractClassMetaData cmd)
    {
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            if (cmd.getDatastoreIdentityMetaData() != null && cmd.getDatastoreIdentityMetaData().getValueStrategy() == ValueGenerationStrategy.IDENTITY)
            {
                // Change to INCREMENT since we don't support IDENTITY
                cmd.getDatastoreIdentityMetaData().setValueStrategy(ValueGenerationStrategy.INCREMENT);
                NucleusLogger.METADATA.warn("Class " + cmd.getFullClassName() +
                    " has been specified to use datastore-identity with IDENTITY value generation, but not supported on HBase. Using INCREMENT");
            }
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            int[] pkFieldNumbers = cmd.getPKMemberPositions();
            if (pkFieldNumbers != null)
            {
                for (int i=0;i<pkFieldNumbers.length;i++)
                {
                    AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                    if (mmd.getValueStrategy() == ValueGenerationStrategy.IDENTITY)
                    {
                        // Change to INCREMENT since we don't support IDENTITY
                        cmd.getDatastoreIdentityMetaData().setValueStrategy(ValueGenerationStrategy.INCREMENT);
                        NucleusLogger.METADATA.warn("Field " + mmd.getFullFieldName() +
                            " has been specified to use IDENTITY value generation, but not supported on HBase. Using INCREMENT");
                    }
                }
            }
        }

        /*if (storeManager.getSchemaHandler().isAutoCreateTables() || storeManager.getSchemaHandler().isAutoCreateColumns())
        {
            ((HBaseSchemaHandler)storeManager.getSchemaHandler()).createSchemaForClass(storeManager, cmd, false);
        }*/
    }
}