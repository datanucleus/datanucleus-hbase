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

/**
 * List of supported metadata extensions.
 */
public enum MetaDataExtension
{
    IN_MEMORY("inMemory"),
    BLOOM_FILTER("bloomFilter"),
    KEEP_DELETED_CELLS("keepDeletedCells"),
    COMPRESSION("compression"),
    BLOCK_CACHE_ENABLED("blockCacheEnabled"),
    TIME_TO_LIVE("timeToLive"),
    MAX_VERSIONS("maxVersions");

    private String value;
    private MetaDataExtension(String value)
    {
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    public static MetaDataExtension parse(final String extension)
    {
        if (extension == null)
        {
            return null;
        }
        try
        {
            // try the enum default value
            return MetaDataExtension.valueOf(extension);
        }
        catch (IllegalArgumentException e)
        {
            for (MetaDataExtension ext: MetaDataExtension.values())
            {
                if (ext.value.equalsIgnoreCase(extension))
                {
                    return ext;
                }
            }
            return null;
        }
    }
}