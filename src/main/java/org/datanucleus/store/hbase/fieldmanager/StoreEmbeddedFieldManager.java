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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * FieldManager for the persistence of a related embedded object (1-1 relation).
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    private final AbstractMemberMetaData ownerMmd;

    public StoreEmbeddedFieldManager(ObjectProvider sm, Put put, Delete delete, AbstractMemberMetaData mmd,
            String tableName, boolean insert)
    {
        super(sm, put, delete, insert, tableName);
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
     * @see org.datanucleus.store.fieldmanager.FieldConsumer#storeObjectField(int, java.lang.Object)
     */
    public void storeObjectField(int fieldNumber, Object value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        if (value == null)
        {
            // TODO What about delete-orphans?
            delete.deleteColumn(familyName.getBytes(), columnName.getBytes());
        }
        else
        {
            ExecutionContext ec = op.getExecutionContext();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            AbstractMemberMetaData embMmd = ownerMmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
            RelationType relationType = embMmd.getRelationType(clr);
            if ((relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.ONE_TO_ONE_UNI) && embMmd.isEmbedded())
            {
                // Embedded PC object
                Class embcls = embMmd.getType();
                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
                if (embcmd != null)
                {
                    ObjectProvider embSM = ec.findObjectProviderForEmbedded(value, op, embMmd);
                    FieldManager ffm = new StoreEmbeddedFieldManager(embSM, put, delete, embMmd, tableName, insert);
                    embSM.provideFields(embcmd.getAllMemberPositions(), ffm);
                    return;
                }
                else
                {
                    throw new NucleusUserException("Field " + embMmd.getFullFieldName() +
                        " specified as embedded but metadata not found for the class of type " + embMmd.getTypeName());
                }
            }
            else if (RelationType.isRelationSingleValued(relationType))
            {
                // PC object, so make sure it is persisted
                Object valuePC = op.getExecutionContext().persistObjectInternal(value, op, fieldNumber, -1);
                if (embMmd.isSerialized())
                {
                    // Persist as serialised into the column of this object
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(value);
                        put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                        oos.close();
                        bos.close();
                    }
                    catch (IOException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
                else
                {
                    // Persist identity in the column of this object
                    Object valueId = ec.getApiAdapter().getIdForObject(valuePC);
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(valueId);
                        put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                        oos.close();
                        bos.close();
                    }
                    catch (IOException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // Collection/Map/Array
                if (embMmd.hasCollection())
                {
                    Collection collIds = new ArrayList();
                    Collection coll = (Collection)value;
                    Iterator collIter = coll.iterator();
                    while (collIter.hasNext())
                    {
                        Object element = collIter.next();
                        Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                        Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                        collIds.add(elementID);
                    }

                    if (embMmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(value);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    else
                    {
                        // Persist comma-separated element key list into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(collIds);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                }
                else if (embMmd.hasMap())
                {
                    Map map = (Map)value;
                    Iterator<Map.Entry> mapIter = map.entrySet().iterator();
                    while (mapIter.hasNext())
                    {
                        Map.Entry entry = mapIter.next();
                        Object mapKey = entry.getKey();
                        Object mapValue = entry.getValue();
                        if (ec.getApiAdapter().isPersistable(mapKey))
                        {
                            ec.persistObjectInternal(mapKey, op, fieldNumber, -1);
                        }
                        if (ec.getApiAdapter().isPersistable(mapValue))
                        {
                            ec.persistObjectInternal(mapValue, op, fieldNumber, -1);
                        }
                    }
                    if (embMmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(value);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    else
                    {
                        // TODO Implement map persistence non-serialised
                        throw new NucleusException("Only currently support maps serialised with HBase. Mark the field as serialized");
                    }
                }
                else if (embMmd.hasArray())
                {
                    Collection arrIds = new ArrayList();
                    for (int i=0;i<Array.getLength(value);i++)
                    {
                        Object element = Array.get(value, i);
                        Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        arrIds.add(elementID);
                    }

                    if (embMmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(value);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    else
                    {
                        // Persist list of array element ids into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(arrIds);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                }
            }
            else
            {
                if (Enum.class.isAssignableFrom(value.getClass()) && !embMmd.isSerialized())
                {
                    // TODO Persist as number when requested
                    put.add(familyName.getBytes(), columnName.getBytes(), ((Enum)value).name().getBytes());
                }

                TypeConverter strConv =
                    op.getExecutionContext().getTypeManager().getTypeConverterForType(embMmd.getType(), String.class);
                if (!embMmd.isSerialized() && strConv != null)
                {
                    // Persist as a String
                    String strValue = (String) strConv.toDatastoreType(value);
                    put.add(familyName.getBytes(), columnName.getBytes(), strValue.getBytes());
                }
                else
                {
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(value);
                        put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                        oos.close();
                        bos.close();
                    }
                    catch (IOException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
            }
        }
    }
}