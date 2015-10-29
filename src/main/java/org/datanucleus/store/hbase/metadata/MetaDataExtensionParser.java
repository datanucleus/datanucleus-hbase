/**********************************************************************
Copyright (c) 2012 Nicolas Seyvet and others. All rights reserved.
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
package org.datanucleus.store.hbase.metadata;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.ExtensionMetaData;
import org.datanucleus.util.NucleusLogger;

/**
 * Parser to process metadata extensions for a class.
 */
public class MetaDataExtensionParser
{
    public final static String BASE = "hbase.columnFamily.";

    private Map<String, BloomType> bloomFilterPerCf = new HashMap<String, BloomType>();
    private Map<String, Boolean> inMemoryPerCf = new HashMap<String, Boolean>();
    private Map<String, KeepDeletedCells> keepDeletedCellsPerCf = new HashMap<String, KeepDeletedCells>();
    private Map<String, Boolean> blockCacheEnabledPerCf = new HashMap<String, Boolean>();
    private Map<String, Compression.Algorithm> compressionPerCf = new HashMap<String, Compression.Algorithm>();
    private Map<String, Integer> maxVersionsPerCf = new HashMap<String, Integer>();
    private Map<String, Integer> ttlPerCf = new HashMap<String, Integer>();
    private boolean extensionsFound = false;

    public MetaDataExtensionParser(AbstractClassMetaData acmd)
    {
        ExtensionMetaData[] eMetaData = acmd.getExtensions();
        if (eMetaData == null)
        {
            // nothing to do.
            return;
        }
        for (ExtensionMetaData anEMetaData : eMetaData)
        {
            String key = anEMetaData.getKey();
            String value = anEMetaData.getValue();
            if (value == null || value.length() == 0)
            {
                // no value -> add log
                continue;
            }
            if (key != null && key.startsWith("hbase.columnFamily."))
            {
                if (NucleusLogger.METADATA.isDebugEnabled())
                {
                    NucleusLogger.METADATA.debug("Found HBase extension: " + anEMetaData);
                }
                int nextDot = key.indexOf(".", BASE.length());
                if (nextDot != -1)
                {
                    String cf = key.substring(BASE.length(), nextDot);
                    String extension = key.substring(nextDot + 1, key.length());
                    if (NucleusLogger.METADATA.isDebugEnabled())
                    {
                        NucleusLogger.METADATA.debug("cf:" + cf + ", extension: " + extension);
                    }
                    MetaDataExtension hBaseExtension = MetaDataExtension.parse(extension);
                    if (hBaseExtension == null)
                    {
                        continue;
                    }
                    if (NucleusLogger.METADATA.isDebugEnabled())
                    {
                        NucleusLogger.METADATA.debug("Adding \'" + hBaseExtension + "\' \'" + value +"\'");
                    }
                    switch (hBaseExtension)
                    {
                        case IN_MEMORY:
                            inMemoryPerCf.put(cf, toBoolean(value));
                            break;
                        case BLOOM_FILTER:
                            bloomFilterPerCf.put(cf, toBloomFilter(value));
                            extensionsFound = true;
                            break;
                        case MAX_VERSIONS:
                            addMaxVersionsPerCf(cf, value);
                            break;
                        case KEEP_DELETED_CELLS:
                            keepDeletedCellsPerCf.put(cf, toKeepDeletedCells(value));
                            break;
                        case COMPRESSION:
                            compressionPerCf.put(cf, toCompression(value));
                            break;
                        case BLOCK_CACHE_ENABLED:
                            blockCacheEnabledPerCf.put(cf, toBoolean(value));
                            break;
                        case TIME_TO_LIVE:
                            ttlPerCf.put(cf, toInteger(value));
                            break;
                        default:
                            break;
                    }
                    extensionsFound = true;
                }
            }
        }
    }

    public boolean hasExtensions()
    {
        return extensionsFound;
    }

    public boolean applyExtensions(HTableDescriptor hTable, final String familyName)
    {
        if (NucleusLogger.METADATA.isDebugEnabled())
        {
            NucleusLogger.METADATA.debug("Applying extensions: {BF = " + bloomFilterPerCf + "}");
        }
        boolean modified = false;
        if (!extensionsFound || familyName == null)
        {
            return modified;
        }
        HColumnDescriptor hColumnDescriptor = hTable.getFamily(familyName.getBytes());
        if(hColumnDescriptor == null) {
            throw new IllegalArgumentException("No such family name corresponding HTable: " + familyName);
        }
        BloomType configuredBloomFilter = getBloomFilterForCf(familyName);
        if (configuredBloomFilter != hColumnDescriptor.getBloomFilterType())
        {
            hColumnDescriptor.setBloomFilterType(configuredBloomFilter);
            modified = true;
        }
        Boolean isInMemory = inMemoryPerCf.get(familyName);
        if (isInMemory != null && isInMemory != hColumnDescriptor.isInMemory())
        {
            hColumnDescriptor.setInMemory(isInMemory);
            modified = true;
        }
        Boolean blockCacheEnabled = blockCacheEnabledPerCf.get(familyName);
        if (blockCacheEnabled != null && blockCacheEnabled != hColumnDescriptor.isBlockCacheEnabled())
        {
            hColumnDescriptor.setBlockCacheEnabled(blockCacheEnabled);
            modified = true;
        }
        Integer ttl = ttlPerCf.get(familyName);
        if (ttl != null && ttl != hColumnDescriptor.getTimeToLive())
        {
            hColumnDescriptor.setTimeToLive(ttl);
            modified = true;
        }
        Compression.Algorithm compression = compressionPerCf.get(familyName);
        if (compression != null && compression != hColumnDescriptor.getCompression())
        {
            hColumnDescriptor.setCompressionType(compression);
            modified = true;
        }       
        KeepDeletedCells keepDeletedCells = keepDeletedCellsPerCf.get(familyName);
        if (keepDeletedCells != null && keepDeletedCells != hColumnDescriptor.getKeepDeletedCells())
        {
            hColumnDescriptor.setKeepDeletedCells(keepDeletedCells);
            modified = true;
        }
        Integer maxVersion = maxVersionsPerCf.get(familyName);
        if (maxVersion != null && maxVersion != hColumnDescriptor.getMaxVersions())
        {
            hColumnDescriptor.setMaxVersions(maxVersion);
            modified = true;
        }
        return modified;
    }

    private BloomType getBloomFilterForCf(String familyName)
    {
        BloomType result = bloomFilterPerCf.get(familyName);
        return result != null ? result : BloomType.NONE;
    }

    private BloomType toBloomFilter(String value)
    {
        if (value == null || value.length() == 0)
        {
            return BloomType.NONE;
        }
        try
        {
            return BloomType.valueOf(value);
        }
        catch (IllegalArgumentException e)
        {
            return BloomType.NONE;
        }
    }
    
    private KeepDeletedCells toKeepDeletedCells(String value)
    {
        if (value == null || value.length() == 0)
        {
            return KeepDeletedCells.FALSE;
        }
        try
        {
            return KeepDeletedCells.valueOf(value.toUpperCase(Locale.UK)); // toUpperCase(...) for compatibility with previous boolean (lower-case 'true'/'false')
        }
        catch (IllegalArgumentException e)
        {
            return KeepDeletedCells.FALSE;
        }
    }

    private Compression.Algorithm toCompression(String value)
    {
        if (value == null || value.length() == 0)
        {
            return Compression.Algorithm.NONE;
        }
        try
        {
            return Compression.Algorithm.valueOf(value);
        }
        catch (IllegalArgumentException e)
        {
            return Compression.Algorithm.NONE;
        }
    }

    private boolean toBoolean(String value)
    {
        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    private Integer toInteger(String value)
    {
        if ("MAX_VALUE".equals(value))
        {
            return Integer.MAX_VALUE;
        }
        try
        {
            return Integer.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    private void addMaxVersionsPerCf(final String cf, final String value)
    {
        Integer res = toInteger(value);
        if (res != null && res > 0)
        {
            maxVersionsPerCf.put(cf, res);
        }
        else
        {
            maxVersionsPerCf.put(cf, HColumnDescriptor.DEFAULT_VERSIONS);
        }
    }
}