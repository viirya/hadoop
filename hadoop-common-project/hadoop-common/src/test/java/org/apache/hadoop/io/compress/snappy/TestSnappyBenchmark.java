/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress.snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.TestCodec;
import org.apache.hadoop.util.StopWatch;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests for performing basic benchmark for snappy compressor/decompressor.
 */
public class TestSnappyBenchmark {
    private static final Logger LOG= LoggerFactory.getLogger(TestSnappyBenchmark.class);

    @Test
    public void testSnappy() throws Exception {
        DecimalFormat df = new DecimalFormat("#.##");

        int[] size = { 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
        for (int i = 0; i < size.length; i++) {
            TestSnappyCompressorDecompressor.compressDecompressLoop(size[i]);
        }
    }

    @Test
    public void testSequenceFileSnappyCodec() throws Exception {
        Configuration conf = new Configuration();
        sequenceFileCodecTest(conf, 20000000, "org.apache.hadoop.io.compress.SnappyCodec", 1000000);
    }

    private static void sequenceFileCodecTest(Configuration conf, int lines,
                                              String codecClass, int blockSize)
            throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        DecimalFormat df = new DecimalFormat("#.##");

        Path filePath = new Path("SequenceFileCodecTest." + codecClass);
        // Configuration
        conf.setInt("io.seqfile.compress.blocksize", blockSize);

        // Create the SequenceFile
        FileSystem fs = FileSystem.get(conf);
        LOG.info("Creating SequenceFile with codec \"" + codecClass + "\"");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, filePath,
                Text.class, Text.class, SequenceFile.CompressionType.BLOCK,
                (CompressionCodec)Class.forName(codecClass).newInstance());

        // Write some data
        LOG.info("Writing to SequenceFile...");
        StopWatch sw = new StopWatch().start();
        for (int i=0; i<lines; i++) {
            Text key = new Text("key" + i);
            Text value = new Text("value" + i);
            writer.append(key, value);
        }
        writer.close();
        long duration = sw.now(TimeUnit.MILLISECONDS);
        LOG.info("Total time: " + df.format(duration / 1000.0) + " s.");

        // Read the data back and check
        LOG.info("Reading from the SequenceFile...");
        sw = new StopWatch().start();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);

        Writable key = (Writable)reader.getKeyClass().newInstance();
        Writable value = (Writable)reader.getValueClass().newInstance();

        int lc = 0;
        try {
            while (reader.next(key, value)) {
                assertEquals("key" + lc, key.toString());
                assertEquals("value" + lc, value.toString());
                lc ++;
            }
        } finally {
            reader.close();
        }
        duration = sw.now(TimeUnit.MILLISECONDS);
        LOG.info("Total time: " + df.format(duration / 1000.0) + " s.");
        assertEquals(lines, lc);

        LOG.info("file size: " + fs.getFileStatus(filePath).getLen());
        // Delete temporary files
        fs.delete(filePath, false);

        LOG.info("SUCCESS! Completed SequenceFileCodecTest with codec \"" + codecClass + "\"");
    }
}
