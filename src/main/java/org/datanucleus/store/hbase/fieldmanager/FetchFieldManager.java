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
2011 Andy Jefferson - clean up of NPE code
2011 Andy Jefferson - rewritten to support relationships
    ...
***********************************************************************/
package org.datanucleus.store.hbase.fieldmanager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseStoreManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.EnumConversionHelper;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager to use for retrieving values from HBase to put into a persistable object.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    Table table;

    Result result;

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Result result, Table table)
    {
        super(ec, cmd);
        this.result = result;
        this.table = table;
    }

    public FetchFieldManager(DNStateManager sm, Result result, Table table)
    {
        super(sm);
        this.result = result;
        this.table = table;
    }

    protected AbstractMemberMetaData getMemberMetaData(int fieldNumber)
    {
        return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchBooleanInternal(bytes, mmd.isSerialized(), null);
    }

    public byte fetchByteField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchByteInternal(bytes, mmd.isSerialized(), null);
    }

    public char fetchCharField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchCharInternal(bytes, mmd.isSerialized(), null);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchDoubleInternal(bytes, mmd.isSerialized(), null);
    }

    public float fetchFloatField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchFloatInternal(bytes, mmd.isSerialized(), null);
    }

    public int fetchIntField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchIntInternal(bytes, mmd.isSerialized(), null);
    }

    public long fetchLongField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchLongInternal(bytes, mmd.isSerialized(), null);
    }

    public short fetchShortField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchShortInternal(bytes, mmd.isSerialized(), null);
    }

    public String fetchStringField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchStringInternal(bytes, mmd.isSerialized(), null);
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Persistable object embedded into table of this object
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);

                Class embcls = mmd.getType();
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);

                // Check for null value (currently need all columns to return null)
                boolean isNull = true;
                EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
                if (embmd != null)
                {
                    // TODO Cater for null (use embmd.getNullIndicatorColumn/Value)
                }
                // Fallback null check
                int[] embAllPosns = embCmd.getAllMemberPositions();
                for (int i=0;i<embAllPosns.length;i++)
                {
                    AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(i);
                    List<AbstractMemberMetaData> subEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                    subEmbMmds.add(embMmd);
                    MemberColumnMapping embMapping = table.getMemberColumnMappingForEmbeddedMember(subEmbMmds);
                    if (embMapping != null)
                    {
                        // This embedded field has a mapping (if this was a nested embedded then would have no mapping)
                        Column col = embMapping.getColumn(0);
                        String familyName = HBaseUtils.getFamilyNameForColumn(col);
                        String columnName = HBaseUtils.getQualifierNameForColumn(col);
                        if (result.getValue(familyName.getBytes(), columnName.getBytes()) != null)
                        {
                            isNull = false;
                            break;
                        }
                    }
                }
                if (isNull)
                {
                    return null;
                }

                DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embCmd, sm, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, result, embMmds, table);
                embSM.replaceFields(embCmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded not supported for this type");
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        boolean optional = false;
        Class type = mmd.getType();
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }
            type = clr.classForName(mmd.getCollection().getElementType());
            optional = true;
        }

        String relationStorageMode = ec.getStoreManager().getStringProperty(PropertyNames.PROPERTY_RELATION_IDENTITY_STORAGE_MODE);
        if (RelationType.isRelationSingleValued(relationType))
        {
            // 1-1/N-1 relation
            Column col = mapping.getColumn(0);
            String familyName = HBaseUtils.getFamilyNameForColumn(col);
            String qualifName = HBaseUtils.getQualifierNameForColumn(col);
            Object value = readObjectField(col, familyName, qualifName, result, mmd, FieldRole.ROLE_FIELD);

            if (value == null)
            {
                return optional ? Optional.empty() : null;
            }

            if (mmd.isSerialized())
            {
                // Make sure it has an StateManager
                DNStateManager pcSM = ec.findStateManager(value);
                if (pcSM == null || ec.getApiAdapter().getExecutionContext(value) == null)
                {
                    ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, value, false, sm, fieldNumber);
                }
                return value;
            }

            // The stored value was the identity
            Object pc = null;
            if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
            {
                String persistableId = (String) value;
                try
                {
                    AbstractClassMetaData mmdCmd = ec.getMetaDataManager().getMetaDataForClass(type, clr);
                    if (mmdCmd != null)
                    {
                        pc = IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
                        return optional ? Optional.of(pc) : pc;
                    }

                    String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_FIELD, clr, ec.getMetaDataManager());
                    if (implNames != null && implNames.length == 1)
                    {
                        // Only one possible implementation, so use that
                        mmdCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                        pc = IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
                        return optional ? Optional.of(pc) : pc;
                    }
                    else if (implNames != null && implNames.length > 1)
                    {
                        // Multiple implementations, so try each implementation in turn (note we only need this if
                        // some impls have different "identity" type from each other)
                        for (String implName : implNames)
                        {
                            try
                            {
                                mmdCmd = ec.getMetaDataManager().getMetaDataForClass(implName, clr);
                                pc = IdentityUtils.getObjectFromPersistableIdentity(persistableId, mmdCmd, ec);
                                return optional ? Optional.of(pc) : pc;
                            }
                            catch (NucleusObjectNotFoundException nonfe)
                            {
                                // Object no longer present in the datastore, must have been deleted
                                throw nonfe;
                            }
                            catch (Exception e)
                            {
                                // Not possible with this implementation
                            }
                        }
                    }
                    else
                    {
                        throw new NucleusUserException(
                            "We do not currently support the field type of " + mmd.getFullFieldName() + " which has an interdeterminate type (e.g interface or Object element types)");

                    }
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    NucleusLogger.PERSISTENCE.warn("Object=" + sm + " field=" + mmd.getFullFieldName() + " has id=" + persistableId + " but could not instantiate object with that identity");
                }
            }
            else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
            {
                // Legacy TODO Remove this
                pc = ec.findObject(value, true, true, null);
            }
            else
            {
                throw new NucleusUserException("This store plugin does not support relation identity storageMode of " + relationStorageMode);
            }
            return optional ? (pc != null ? Optional.of(pc) : Optional.empty()) : pc;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // 1-N/M-N relation
            Column col = mapping.getColumn(0);
            String familyName = HBaseUtils.getFamilyNameForColumn(col);
            String qualifName = HBaseUtils.getQualifierNameForColumn(col);
            Object value = readObjectField(col, familyName, qualifName, result, mmd, FieldRole.ROLE_FIELD);

            if (value == null)
            {
                return optional ? Optional.empty() : null;
            }

            if (mmd.isSerialized())
            {
                Object returnValue = optional ? Optional.of(value) : value;
                return (sm != null) ? SCOUtils.wrapSCOField(sm, fieldNumber, returnValue, true) : returnValue;
            }

            if (mmd.hasCollection())
            {
                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                if (mmd.getCollection().isSerializedElement())
                {
                    // TODO Support this
                    throw new NucleusException("Don't currently support serialized collection elements (field=" + mmd.getFullFieldName() + ")");
                }

                AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
                if (elemCmd == null)
                {
                    // Try any listed implementations
                    String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_COLLECTION_ELEMENT, clr, ec.getMetaDataManager());
                    if (implNames != null && implNames.length > 0)
                    {
                        // Just use first implementation TODO What if the impls have different id type?
                        elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                    }
                    if (elemCmd == null)
                    {
                        throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() + 
                            " which has a collection of interdeterminate element type (e.g interface or Object element types)");
                    }
                }

                Collection collIds = (Collection)value;
                Iterator idIter = collIds.iterator();
                boolean changeDetected = false;
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                    {
                        String persistableId = (String)elementId;
                        if (persistableId.equals("NULL"))
                        {
                            // Null element
                            coll.add(null);
                        }
                        else
                        {
                            try
                            {
                                coll.add(IdentityUtils.getObjectFromPersistableIdentity(persistableId, elemCmd, ec));
                            }
                            catch (NucleusObjectNotFoundException onfe)
                            {
                                // Object no longer exists. Deleted by user? so ignore
                                changeDetected = true;
                            }
                        }
                    }
                    else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                    {
                        coll.add(ec.findObject(elementId, true, true, null));
                    }
                }

                if (coll instanceof List && mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getOrdering() != null && !mmd.getOrderMetaData().getOrdering().equals("#PK"))
                {
                    // Reorder the collection as per the ordering clause
                    Collection newColl = QueryUtils.orderCandidates((List)coll, clr.classForName(mmd.getCollection().getElementType()), mmd.getOrderMetaData().getOrdering(), ec, clr);
                    if (newColl.getClass() != coll.getClass())
                    {
                        // Type has changed, so just reuse the input
                        coll.clear();
                        coll.addAll(newColl);
                    }
                }

                if (sm != null)
                {
                    // Wrap if SCO
                    coll = (Collection) SCOUtils.wrapSCOField(sm, fieldNumber, coll, true);
                    if (changeDetected)
                    {
                        sm.makeDirty(mmd.getAbsoluteFieldNumber());
                    }
                }
                return coll;
            }
            else if (mmd.hasMap())
            {
                if (mmd.getMap().isSerializedKey() || mmd.getMap().isSerializedValue())
                {
                    // TODO Support this
                    throw new NucleusException("Don't currently support serialized map keys/values (field=" + mmd.getFullFieldName() + ")");
                }

                Map mapIds = (Map)value;
                AbstractClassMetaData keyCmd = null;
                if (mmd.getMap().keyIsPersistent())
                {
                    keyCmd = mmd.getMap().getKeyClassMetaData(clr);
                    if (keyCmd == null)
                    {
                        // Try any listed implementations
                        String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_MAP_KEY, clr, ec.getMetaDataManager());
                        if (implNames != null && implNames.length == 1)
                        {
                            keyCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                        }
                        if (keyCmd == null)
                        {
                            throw new NucleusUserException(
                                    "We do not currently support the field type of " + mmd.getFullFieldName() + " which has a map of interdeterminate key type (e.g interface or Object element types)");
                        }
                    }
                }
                AbstractClassMetaData valCmd = null;
                if (mmd.getMap().valueIsPersistent())
                {
                    valCmd = mmd.getMap().getValueClassMetaData(clr);
                    if (valCmd == null)
                    {
                        // Try any listed implementations
                        String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_MAP_VALUE, clr, ec.getMetaDataManager());
                        if (implNames != null && implNames.length == 1)
                        {
                            valCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                        }
                        if (valCmd == null)
                        {
                            throw new NucleusUserException(
                                    "We do not currently support the field type of " + mmd.getFullFieldName() + " which has a map of interdeterminate value type (e.g interface or Object element types)");
                        }
                    }
                }

                Map map;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                    map = (Map) instanceType.getDeclaredConstructor().newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                Iterator<Map.Entry> mapIdIter = mapIds.entrySet().iterator();
                boolean changeDetected = false;
                while (mapIdIter.hasNext())
                {
                    Map.Entry entry = mapIdIter.next();
                    Object mapKey = entry.getKey();
                    Object mapValue = entry.getValue();
                    boolean keySet = true;
                    boolean valSet = true;

                    if (mmd.getMap().keyIsPersistent())
                    {
                        if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                        {
                            String keyPersistableId = (String)mapKey;
                            try
                            {
                                mapKey = IdentityUtils.getObjectFromPersistableIdentity(keyPersistableId, keyCmd, ec);
                            }
                            catch (NucleusObjectNotFoundException onfe)
                            {
                                // Object no longer exists. Deleted by user? so ignore
                                changeDetected = true;
                                keySet = false;
                            }
                        }
                        else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                        {
                            // Legacy "id"
                            mapKey = ec.findObject(mapKey, true, true, null);
                        }
                    }

                    if (mmd.getMap().valueIsPersistent())
                    {
                        if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                        {
                            // TODO Support serialised value which will be of type ByteBuffer
                            String valPersistableId = (String)mapValue;
                            if (valPersistableId.equals("NULL"))
                            {
                                mapValue = null;
                            }
                            else
                            {
                                try
                                {
                                    mapValue = IdentityUtils.getObjectFromPersistableIdentity(valPersistableId, valCmd, ec);
                                }
                                catch (NucleusObjectNotFoundException onfe)
                                {
                                    // Object no longer exists. Deleted by user? so ignore
                                    changeDetected = true;
                                    valSet = false;
                                }
                            }
                        }
                        else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                        {
                            // Legacy "id"
                            mapValue = ec.findObject(mapValue, true, true, null);
                        }
                    }

                    if (keySet && valSet)
                    {
                        map.put(mapKey, mapValue);
                    }
                }

                if (sm != null)
                {
                    // Wrap if SCO
                    map = (Map) SCOUtils.wrapSCOField(sm, fieldNumber, map, true);
                    if (changeDetected)
                    {
                        sm.makeDirty(fieldNumber);
                    }
                }
                return map;
            }
            else if (mmd.hasArray())
            {
                if (mmd.getArray().isSerializedElement())
                {
                    // TODO Support this
                    throw new NucleusException("Don't currently support serialized array elements (field=" + mmd.getFullFieldName() + ")");
                }

                AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr);
                if (elemCmd == null)
                {
                    // Try any listed implementations
                    String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_ARRAY_ELEMENT, clr, ec.getMetaDataManager());
                    if (implNames != null && implNames.length == 1)
                    {
                        elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                    }
                    if (elemCmd == null)
                    {
                        throw new NucleusUserException(
                                "We do not currently support the field type of " + mmd.getFullFieldName() + " which has an array of interdeterminate element type (e.g interface or Object element types)");
                    }
                }

                Collection arrIds = (Collection)value;
                Object array = Array.newInstance(mmd.getType().getComponentType(), arrIds.size());
                Iterator idIter = arrIds.iterator();
                boolean changeDetected = false;
                int pos=0;
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                    {
                        String persistableId = (String)elementId;
                        
                        if (persistableId.equals("NULL"))
                        {
                            // Null element
                            Array.set(array, pos++, null);
                        }
                        else
                        {
                            try
                            {
                                Array.set(array, pos++, IdentityUtils.getObjectFromPersistableIdentity(persistableId, elemCmd, ec));
                            }
                            catch (NucleusObjectNotFoundException onfe)
                            {
                                // Object no longer exists. Deleted by user? so ignore
                                changeDetected = true;
                            }
                        }
                    }
                    else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                    {
                        // Legacy TODO Remove this
                        Array.set(array, pos, ec.findObject(elementId, true, true, null));
                    }
                }

                if (changeDetected)
                {
                    if (pos < arrIds.size())
                    {
                        // Some elements not found, so resize the array
                        Object arrayOld = array;
                        array = Array.newInstance(mmd.getType().getComponentType(), pos);
                        for (int j = 0; j < pos; j++)
                        {
                            Array.set(array, j, Array.get(arrayOld, j));
                        }
                    }
                    if (sm != null)
                    {
                        sm.makeDirty(mmd.getAbsoluteFieldNumber());
                    }
                }
                return array;
            }
            throw new NucleusUserException("No container that isnt collection/map/array");
        }
        else
        {
            Object returnValue = null;
            if (mmd.isSerialized())
            {
                Column col = mapping.getColumn(0);
                String familyName = HBaseUtils.getFamilyNameForColumn(col);
                String qualifName = HBaseUtils.getQualifierNameForColumn(col);
                Object value = readObjectField(col, familyName, qualifName, result, mmd, FieldRole.ROLE_FIELD);
                if (value == null)
                {
                    return optional ? Optional.empty() : null;
                }

                returnValue = value;
            }
            else
            {
                if (mapping.getTypeConverter() != null)
                {
                    // Persist using the provided converter
                    TypeConverter conv = mapping.getTypeConverter();
                    if (mapping.getNumberOfColumns() > 1)
                    {
                        boolean isNull = true;
                        Object valuesArr = null;
                        Class[] colTypes = ((MultiColumnConverter)conv).getDatastoreColumnTypes();
                        if (colTypes[0] == int.class)
                        {
                            valuesArr = new int[mapping.getNumberOfColumns()];
                        }
                        else if (colTypes[0] == long.class)
                        {
                            valuesArr = new long[mapping.getNumberOfColumns()];
                        }
                        else if (colTypes[0] == double.class)
                        {
                            valuesArr = new double[mapping.getNumberOfColumns()];
                        }
                        else if (colTypes[0] == float.class)
                        {
                            valuesArr = new double[mapping.getNumberOfColumns()];
                        }
                        else if (colTypes[0] == String.class)
                        {
                            valuesArr = new String[mapping.getNumberOfColumns()];
                        }
                        // TODO Support other types
                        else
                        {
                            valuesArr = new Object[mapping.getNumberOfColumns()];
                        }

                        for (int i=0;i<mapping.getNumberOfColumns();i++)
                        {
                            Column col = mapping.getColumn(i);
                            String familyName = HBaseUtils.getFamilyNameForColumn(col);
                            String qualifName = HBaseUtils.getQualifierNameForColumn(col);
                            Object colValue = null;
                            if (result.containsColumn(familyName.getBytes(), qualifName.getBytes()))
                            {
                                byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
                                if (bytes != null)
                                {
                                    isNull = false;
                                    if (colTypes[i] == String.class)
                                    {
                                        colValue = new String(bytes);
                                    }
                                    else if (colTypes[i] == Integer.class || colTypes[i] == int.class)
                                    {
                                        colValue = Bytes.toInt(bytes);
                                    }
                                    else if (colTypes[i] == Long.class || colTypes[i] == long.class)
                                    {
                                        colValue = Bytes.toLong(bytes);
                                    }
                                    else if (colTypes[i] == Double.class || colTypes[i] == double.class)
                                    {
                                        colValue = Bytes.toDouble(bytes);
                                    }
                                    else if (colTypes[i] == Float.class || colTypes[i] == float.class)
                                    {
                                        colValue = Bytes.toFloat(bytes);
                                    }
                                    else if (colTypes[i] == Short.class || colTypes[i] == short.class)
                                    {
                                        colValue = Bytes.toShort(bytes);
                                    }
                                    else if (colTypes[i] == Boolean.class || colTypes[i] == boolean.class)
                                    {
                                        colValue = Bytes.toBoolean(bytes);
                                    }
                                    else if (colTypes[i] == BigDecimal.class)
                                    {
                                        colValue = Bytes.toBigDecimal(bytes);
                                    }
                                    else if (java.util.Date.class.isAssignableFrom(colTypes[i]))
                                    {
                                        String datastoreValue = new String(bytes);
                                        TypeConverter<java.util.Date, String> dateStrConv = ec.getTypeManager().getTypeConverterForType(colTypes[i], String.class);
                                        java.util.Date datastoreDate = dateStrConv.toMemberType(datastoreValue);
                                        colValue = conv.toMemberType(datastoreDate);
                                    }
                                    else
                                    {
                                        NucleusLogger.PERSISTENCE.warn("Retrieve of column " + col + " is for type " + colTypes[i] + " but this is not yet supported. Report this");
                                    }
                                }
                            }
                            Array.set(valuesArr, i, colValue);
                        }
                        if (isNull)
                        {
                            return null;
                        }

                        Object memberValue = conv.toMemberType(valuesArr);
                        if (sm != null && memberValue != null)
                        {
                            memberValue = SCOUtils.wrapSCOField(sm, fieldNumber, memberValue, true);
                        }
                        return memberValue;
                    }

                    // Single column converter
                    Column col = mapping.getColumn(0);
                    String familyName = HBaseUtils.getFamilyNameForColumn(col);
                    String qualifName = HBaseUtils.getQualifierNameForColumn(col);

                    Class datastoreType = ec.getTypeManager().getDatastoreTypeForTypeConverter(conv, mmd.getType());
                    byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
                    if (datastoreType == byte[].class)
                    {
                        // Special case : TypeConverter converts field type to byte[] so just pass in to converter
                        returnValue = conv.toMemberType(bytes);
                    }
                    else
                    {
                        if (bytes == null)
                        {
                            return null;
                        }

                        if (datastoreType == String.class)
                        {
                            returnValue = conv.toMemberType(new String(bytes));
                        }
                        else if (datastoreType == Long.class)
                        {
                            returnValue = conv.toMemberType(Bytes.toLong(bytes));
                        }
                        else if (datastoreType == Integer.class)
                        {
                            returnValue = conv.toMemberType(Bytes.toInt(bytes));
                        }
                        else if (datastoreType == Double.class)
                        {
                            returnValue = conv.toMemberType(Bytes.toDouble(bytes));
                        }
                        else if (datastoreType == Boolean.class)
                        {
                            returnValue = conv.toMemberType(Bytes.toBoolean(bytes));
                        }
                        else if (java.util.Date.class.isAssignableFrom(datastoreType))
                        {
                            String datastoreValue = new String(bytes);
                            TypeConverter<java.util.Date, String> dateStrConv = ec.getTypeManager().getTypeConverterForType(datastoreType, String.class);
                            java.util.Date datastoreDate = dateStrConv.toMemberType(datastoreValue);
                            returnValue = conv.toMemberType(datastoreDate);
                        }
                        // TODO Cater for other types
                    }
                }
                else
                {
                    Column col = mapping.getColumn(0);
                    String familyName = HBaseUtils.getFamilyNameForColumn(col);
                    String qualifName = HBaseUtils.getQualifierNameForColumn(col);
                    Object value = readObjectField(col, familyName, qualifName, result, mmd, FieldRole.ROLE_FIELD);
                    if (value == null)
                    {
                        return optional ? Optional.empty() : null;
                    }

                    returnValue = value;

                    if (Boolean.class.isAssignableFrom(type) ||
                        Byte.class.isAssignableFrom(type) ||
                        Integer.class.isAssignableFrom(type) ||
                        Double.class.isAssignableFrom(type) ||
                        Float.class.isAssignableFrom(type) ||
                        Long.class.isAssignableFrom(type) ||
                        Character.class.isAssignableFrom(type) ||
                        Short.class.isAssignableFrom(type))
                    {
                        return optional ? Optional.of(value) : value;
                    }
                    else if (String.class.isAssignableFrom(type))
                    {
                        return optional ? Optional.of(value) : value;
                    }
                    else if (Enum.class.isAssignableFrom(type))
                    {
                        returnValue = EnumConversionHelper.getEnumForStoredValue(mmd, FieldRole.ROLE_FIELD, value, clr);
                        return optional ? Optional.of(returnValue) : returnValue;
                    }
                    else if (Collection.class.isAssignableFrom(type))
                    {
                        Collection<Object> coll;
                        try
                        {
                            Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                            coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
                        }
                        catch (Exception e)
                        {
                            throw new NucleusDataStoreException(e.getMessage(), e);
                        }

                        TypeConverter elemConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_COLLECTION_ELEMENT);

                        Collection dbColl = (Collection)value;
                        Iterator dbCollIter = dbColl.iterator();
                        while (dbCollIter.hasNext())
                        {
                            Object dbElem = dbCollIter.next();
                            Object elem = dbElem;
                            if (elemConv != null)
                            {
                                elem = elemConv.toMemberType(dbElem);
                            }
                            coll.add(elem);
                        }

                        return (sm!=null) ? SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), coll, true) : coll;
                    }
                    else if (Map.class.isAssignableFrom(type))
                    {
                        Map map;
                        try
                        {
                            Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), null);
                            map = (Map) instanceType.getDeclaredConstructor().newInstance();
                        }
                        catch (Exception e)
                        {
                            throw new NucleusDataStoreException(e.getMessage(), e);
                        }

                        TypeConverter keyConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_KEY);
                        TypeConverter valConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_VALUE);

                        Map dbMap = (Map)value;
                        Iterator<Map.Entry> dbMapEntryIter = dbMap.entrySet().iterator();
                        while (dbMapEntryIter.hasNext())
                        {
                            Map.Entry dbMapEntry = dbMapEntryIter.next();
                            Object key = dbMapEntry.getKey();
                            if (keyConv != null)
                            {
                                key = keyConv.toMemberType(dbMapEntry.getKey());
                            }

                            Object val = dbMapEntry.getValue();
                            if (valConv != null)
                            {
                                val = valConv.toMemberType(dbMapEntry.getValue());
                            }

                            map.put(key, val);
                        }

                        return (sm!=null) ? SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), map, true) : map;          
                    }
                    else if (type.isArray())
                    {
                        // TODO Support this
                    }
                    else
                    {
                        // Fallback to built-in type converter
                        // TODO Make use of default TypeConverter for a type before falling back to String/Long
                        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                        if (strConv != null)
                        {
                            // Persisted as a String, so convert back
                            String strValue = (String)value;
                            returnValue = strConv.toMemberType(strValue);
                        }
                    }
                }
            }
            returnValue = optional ? Optional.of(returnValue) : returnValue;
            return (sm!=null) ? SCOUtils.wrapSCOField(sm, fieldNumber, returnValue, true) : returnValue;
        }
    }

    protected Object readObjectField(Column col, String familyName, String qualifName, Result result, AbstractMemberMetaData mmd, FieldRole role)
    {
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        if (bytes == null)
        {
            return null;
        }

        // TODO Get default value for column from Table/Column structure
        Class type = (Optional.class.isAssignableFrom(mmd.getType()) ? ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType()) : mmd.getType());
        if (type == Boolean.class)
        {
            return fetchBooleanInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == Byte.class)
        {
            return fetchByteInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == Character.class)
        {
            return fetchCharInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == Double.class)
        {
            return fetchDoubleInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == Float.class)
        {
            return fetchFloatInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == Integer.class)
        {
            return fetchIntInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == Long.class)
        {
            return fetchLongInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == Short.class)
        {
            return fetchShortInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (type == String.class)
        {
            return fetchStringInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            JdbcType jdbcType = MetaDataUtils.getJdbcTypeForEnum(mmd, role, ec.getClassLoaderResolver());
            if (MetaDataUtils.isJdbcTypeNumeric(jdbcType))
            {
                return fetchIntInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
            }
            return fetchStringInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (java.util.Date.class.isAssignableFrom(type))
        {
            // TODO What about java.util.Date?
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        try
        {
            ois = new ObjectInputStream(bis);
            return ois.readObject();
        }
        catch (IOException e)
        {
            // Failure in deserialisation, so must be persisted as String
            // Return as a String TODO Allow persist using ObjectLongConverter as non-serialised
            return new String(bytes);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        finally
        {
            try
            {
                if (ois != null)
                {
                    ois.close();
                }
                bis.close();
            }
            catch (IOException ioe)
            {
                throw new NucleusException(ioe.getMessage(), ioe);
            }
        }
    }

    private boolean fetchBooleanInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        boolean value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Boolean.valueOf(defaultValue).booleanValue();
            }
            return false;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readBoolean();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toBoolean(bytes);
        }
        return value;
    }

    private byte fetchByteInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return defaultValue.getBytes()[0];
            }
            return 0;
        }

        return bytes[0];
    }

    private char fetchCharInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        char value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null && defaultValue.length() > 0)
            {
                return defaultValue.charAt(0);
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readChar();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            String strValue = new String(bytes);
            value = strValue.charAt(0);
        }
        return value;
    }

    private double fetchDoubleInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        double value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Double.valueOf(defaultValue).doubleValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readDouble();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toDouble(bytes);
        }
        return value;
    }

    private float fetchFloatInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        float value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Float.valueOf(defaultValue).floatValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readFloat();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toFloat(bytes);
        }
        return value;
    }

    private int fetchIntInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        int value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Integer.valueOf(defaultValue).intValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readInt();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toInt(bytes);
        }
        return value;
    }

    private long fetchLongInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        long value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Long.valueOf(defaultValue).longValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readLong();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toLong(bytes);
        }
        return value;
    }

    private short fetchShortInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        short value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Short.valueOf(defaultValue).shortValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readShort();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toShort(bytes);
        }
        return value;
    }

    private String fetchStringInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return defaultValue;
            }
            return null;
        }

        if (serialised)
        {
            // Early version of this plugin serialised values
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                String value = (String) ois.readObject();
                ois.close();
                bis.close();
                return value;
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
            catch (ClassNotFoundException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }

        return new String(bytes);
    }
}