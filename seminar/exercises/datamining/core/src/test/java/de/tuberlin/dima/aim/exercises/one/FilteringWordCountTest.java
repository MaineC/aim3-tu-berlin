/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.dima.aim.exercises.one;

import de.tuberlin.dima.aim.exercises.hadoop.HadoopTestcase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for word count task.
 */
public class FilteringWordCountTest extends HadoopTestcase {

  @Test
  public void countWords() throws Exception {

    File inputFile = getTestTempFile("lotr.txt");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(inputFile,
        "One Ring to rule them all,",
        "One Ring to find them,",
        "One Ring to bring them all",
        "and in the darkness bind them");

    Configuration conf = new Configuration();
    FilteringWordCount wordCount = new FilteringWordCount();
    wordCount.setConf(conf);

    wordCount.run(new String[] { "--input", inputFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath() });

    Map<String, Integer> counts = getCounts(new File(outputDir, "part-r-00000"));

    assertEquals(new Integer(3), counts.get("ring"));
    assertEquals(new Integer(2), counts.get("all"));
    assertEquals(new Integer(1), counts.get("darkness"));
  }

  protected Map<String,Integer> getCounts(File outputFile) throws IOException {
    Map<String,Integer> counts = new HashMap<String,Integer>();
    for (String line : new FileLineIterable(outputFile)) {
      String[] tokens = line.split("\t");
      counts.put(tokens[0], Integer.parseInt(tokens[1]));
    }
    return counts;
  }
}
