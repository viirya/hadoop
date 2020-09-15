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

import org.apache.hadoop.util.StopWatch;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

/**
 * Tests for performing basic benchmark for snappy compressor/decompressor.
 */
public class TestSnappyBenchmark {
    @Test
    public void testSnappy() throws Exception {
        DecimalFormat df = new DecimalFormat("#.##");

        int[] size = { 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
        for (int i = 0; i < size.length; i++) {
            TestSnappyCompressorDecompressor.compressDecompressLoop(size[i]);
        }
    }
}
