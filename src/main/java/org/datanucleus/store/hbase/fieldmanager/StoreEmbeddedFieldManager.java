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
package org.datanucleus.store.hbase.fieldmanager;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.ClassUtils;

/**
 * FieldManager for the persistence of a related embedded object (1-1 relation).
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public StoreEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Put put, Delete delete, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, cmd, put, delete, insert, table);
        this.mmds = mmds;
    }

    public StoreEmbeddedFieldManager(ObjectProvider sm, Put put, Delete delete, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(sm, put, delete, insert, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.FieldConsumer#storeObjectField(int, java.lang.Object)
     */
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMemberMetaData lastMmd = mmds.get(mmds.size()-1);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this member being a link back to the owner. TODO Repeat this for nested and their owners
            if (sm != null)
            {
                ObjectProvider[] ownerOPs = ec.getOwnersForEmbeddedObjectProvider(sm);
                if (ownerOPs != null && ownerOPs.length == 1 && value != ownerOPs[0].getObject())
                {
                    // Make sure the owner field is set
                    sm.replaceField(fieldNumber, ownerOPs[0].getObject());
                }
            }
            return;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, lastMmd))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);

                if (value == null)
                {
                    // Store null in all columns of this and any nested embedded objects // TODO Do this for all embedded fields (and nested)
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, put, delete, insert, embMmds, table);
                    int[] embMmdPosns = embCmd.getAllMemberPositions();
                    for (int i=0;i<embMmdPosns.length;i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                        {
                            // Remove property for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            Column col = mapping.getColumn(0);
                            String colFamName = HBaseUtils.getFamilyNameForColumn(col);
                            String colQualName = HBaseUtils.getQualifierNameForColumn(col);
                            delete.addColumn(colFamName.getBytes(), colQualName.getBytes());
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                }
                else
                {
                    ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, sm, mmd);
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embOP, put, delete, insert, embMmds, table);
                    embOP.provideFields(embCmd.getAllMemberPositions(), storeEmbFM);
                    return;
                }
            }
            else
            {
                throw new NucleusUserException("Field " + mmd.getFullFieldName() + " specified as embedded but field of this type not suppported");
            }
        }

        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }
}