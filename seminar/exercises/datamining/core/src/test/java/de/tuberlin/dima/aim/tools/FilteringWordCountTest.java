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

package de.tuberlin.dima.aim.tools;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for word count task.
 */
public class FilteringWordCountTest {

  private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> driver;

  @Before
  public void setUp() {
    FilteringWordCountMapper mapper = new FilteringWordCountMapper();
    WordCountReducer reducer = new WordCountReducer();
    driver = new MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable>(mapper, reducer);
  }
  /**
   * Tests correct stop word filtering, counting and aggregation.
   */
  @Test
  public void filteredCounting() throws Exception {
    Text text = new Text("This is an initial text used for testing the text word count code.");
    List<Pair<Text, IntWritable>> out = this.driver.withInput(new Object(), text).run();
    assertEquals(7, out.size());
    assertTrue(out.contains(generate("initial", 1)));
    assertTrue(out.contains(generate("used", 1)));
    assertTrue(out.contains(generate("testing", 1)));
    assertTrue(out.contains(generate("word", 1)));
    assertTrue(out.contains(generate("count", 1)));
    assertTrue(out.contains(generate("code", 1)));
    assertTrue(out.contains(generate("text", 2)));
  }

  private Pair<Text, IntWritable> generate(final String word, final int counter) {
    return new Pair<Text, IntWritable>(
        new Text(word),
        new IntWritable(counter));
  }
}
