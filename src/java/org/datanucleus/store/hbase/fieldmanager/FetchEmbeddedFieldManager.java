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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * FieldManager for the retrieval of a related embedded object (1-1 relation).
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    private final AbstractMemberMetaData ownerMmd;

    public FetchEmbeddedFieldManager(ObjectProvider sm, Result result, AbstractMemberMetaData mmd, String tableName)
    {
        super(sm, result, tableName);
        this.ownerMmd = mmd;
    }

    protected String getFamilyName(int fieldNumber)
    {
        return HBaseUtils.getFamilyName(ownerMmd, fieldNumber, tableName);
    }

    protected String getQualifierName(int fieldNumber)
    {
        return HBaseUtils.getQualifierName(ownerMmd, fieldNumber);
    }

    protected AbstractMemberMetaData getMemberMetaData(int fieldNumber)
    {
        return ownerMmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData embMmd = ownerMmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
        RelationType relationType = embMmd.getRelationType(clr);
        if (embMmd.isEmbedded() && RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object embedded into table of this object
            Class embcls = embMmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null)
            {
                // Check for null value (currently need all columns to return null)
                // TODO Cater for null using embmd.getNullIndicatorColumn etc
                EmbeddedMetaData embmd = ownerMmd.getEmbeddedMetaData();
                AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
                boolean isNull = true;
                for (int i=0;i<embmmds.length;i++)
                {
                    String familyName = HBaseUtils.getFamilyName(ownerMmd, i, tableName);
                    String columnName = HBaseUtils.getQualifierName(ownerMmd, i);
                    if (result.getValue(familyName.getBytes(), columnName.getBytes()) != null)
                    {
                        isNull = false;
                        break;
                    }
                }
                if (isNull)
                {
                    return null;
                }

                ObjectProvider embSM = ec.newObjectProviderForEmbedded(embcmd, sm, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, result, embMmd, tableName);
                embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            throw new NucleusUserException("Field " + ownerMmd.getFullFieldName() + " marked as embedded but no such metadata");
        }

        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        Object value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            if (bytes != null)
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readObject();
                ois.close();
                bis.close();
            }
            else
            {
                return null;
            }
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (embMmd.isSerialized())
            {
                return value;
            }
            else
            {
                // The stored value was the identity
                return ec.findObject(value, true, true, null);
            }
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (embMmd.hasCollection())
            {
                if (embMmd.isSerialized())
                {
                    return value;
                }

                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(embMmd.getType(), embMmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                Collection collIds = (Collection)value;
                Iterator idIter = collIds.iterator();
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    coll.add(ec.findObject(elementId, true, true, null));
                }

                if (sm != null)
                {
                    return sm.wrapSCOField(fieldNumber, coll, false, false, true);
                }
                return coll;
            }
            else if (embMmd.hasMap())
            {
                if (embMmd.isSerialized())
                {
                    return value;
                }
            }
            else if (embMmd.hasArray())
            {
                if (embMmd.isSerialized())
                {
                    return value;
                }

                Collection arrIds = (Collection)value;
                Object array = Array.newInstance(embMmd.getType().getComponentType(), arrIds.size());
                Iterator idIter = arrIds.iterator();
                int i=0;
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    Array.set(array, i, ec.findObject(elementId, true, true, null));
                }
                return array;
            }
            throw new NucleusUserException("No container that isnt collection/map/array");
        }
        else
        {
            Object returnValue = null;
            if (!embMmd.isSerialized())
            {
                if (Enum.class.isAssignableFrom(embMmd.getType()))
                {
                    // TODO Retrieve as number when requested
                    return Enum.valueOf(embMmd.getType(), (String)value);
                }

                TypeConverter strConv = sm.getExecutionContext().getTypeManager().getTypeConverterForType(embMmd.getType(), String.class);
                if (strConv != null)
                {
                    // Persisted as a String, so convert back
                    String strValue = (String)value;
                    returnValue = strConv.toMemberType(strValue);
                }
            }

            if (sm != null)
            {
                return sm.wrapSCOField(fieldNumber, returnValue, false, false, true);
            }
            return returnValue;
        }
    }
}