/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store.fs;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

// TODO: move to Lucene

/** Wraps another {@link Directory} instance and logs when certain IO operations are slow. */

class SlowIOLoggingDirectoryWrapper extends FilterDirectory {

    private static final double LOG_ABOVE_MS = 50.0;

    private final ESLogger logger;
    
    public SlowIOLoggingDirectoryWrapper(Settings settings, ShardId shardId, Directory in) {
        super(in);
        logger = Loggers.getLogger(getClass(), settings, shardId);
    }

    @Override
    public String[] listAll() throws IOException {
        long startNS = System.nanoTime();
        try {
            return in.listAll();
        } finally {
            maybeLog("listAll", startNS);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        long startNS = System.nanoTime();
        try {
            in.deleteFile(name);
        } finally {
            maybeLog("deleteFile: " + name, startNS);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        long startNS = System.nanoTime();
        try {
            return in.fileLength(name);
        } finally {
            maybeLog("fileLength: " + name, startNS);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        long startNS = System.nanoTime();
        try {
            return in.createOutput(name, context);
        } finally {
            maybeLog("createOutput: " + name, startNS);
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        long startNS = System.nanoTime();
        try {
            in.sync(names);
        } finally {
            maybeLog("sync: " + names, startNS);
        }
    }

    @Override
    public void renameFile(String source, String dest) throws IOException {
        long startNS = System.nanoTime();
        try {
            in.renameFile(source, dest);
        } finally {
            maybeLog("renameFile: src=" + source + " dest=" + dest, startNS);
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        long startNS = System.nanoTime();
        try {
            return in.openInput(name, context);
        } finally {
            maybeLog("openInput: " + name, startNS);
        }
    }

    @Override
    public Lock makeLock(String name) {
        long startNS = System.nanoTime();
        try {
            return in.makeLock(name);
        } finally {
            maybeLog("makeLock: " + name, startNS);
        }
    }

    @Override
    public void close() throws IOException {
        long startNS = System.nanoTime();
        try {
            in.close();
        } finally {
            maybeLog("close", startNS);
        }
    }

    private void maybeLog(String desc, long startNS) {
        double msec = (System.nanoTime() - startNS)/1000000.0;
        if (msec >= LOG_ABOVE_MS) {
            logger.debug("op {} on directory {} took {} msec", desc, in, msec);
        }
    }
}
