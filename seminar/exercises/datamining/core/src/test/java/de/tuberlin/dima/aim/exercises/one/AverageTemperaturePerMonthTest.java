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

import com.google.common.collect.Maps;
import de.tuberlin.dima.aim.exercises.hadoop.HadoopTestcase;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class AverageTemperaturePerMonthTest extends HadoopTestcase {

  @Test
  public void countWords() throws Exception {

    File inputFile = getTestTempFile("temperatures.tsv");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(inputFile, readTemperatures("/one/temperatures.tsv"));

    double minimumQuality = 0.25;

    Configuration conf = new Configuration();
    AverageTemperaturePerMonth averageTemperaturePerMonth = new AverageTemperaturePerMonth();
    averageTemperaturePerMonth.setConf(conf);

    averageTemperaturePerMonth.run(new String[] { "--input", inputFile.getAbsolutePath(),
        "--output", outputDir.getAbsolutePath(), "--minimumQuality", String.valueOf(minimumQuality) });


    Map<YearAndMonth, Double> results = readResults(new File(outputDir, "part-r-00000"));

    assertEquals(results.get(new YearAndMonth(1990, 8)), 8, EPSILON);
    assertEquals(results.get(new YearAndMonth(1992, 4)), 7.888, EPSILON);
    assertEquals(results.get(new YearAndMonth(1994, 1)), 8.24, EPSILON);
  }


  private List<String> readTemperatures(String path) throws IOException {
    List<String> temperatures = new ArrayList<String>(10000);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(path)));
      String line;
      while ((line = reader.readLine()) != null) {
        temperatures.add(line);
      }
    } finally {
      IOUtils.quietClose(reader);
    }
    return temperatures;
  }

  class YearAndMonth {

    private final int year;
    private final int month;

    public YearAndMonth(int year, int month) {
      this.year = year;
      this.month = month;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof YearAndMonth) {
        YearAndMonth other = (YearAndMonth) o;
        return year == other.year && month == other.month;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 * year + month;
    }
  }

  private Map<YearAndMonth,Double> readResults(File outputFile) throws IOException {
    Pattern separator = Pattern.compile("\t");
    Map<YearAndMonth,Double> averageTemperatures = Maps.newHashMap();
    for (String line : new FileLineIterable(outputFile)) {
      String[] tokens = separator.split(line);
      int year = Integer.parseInt(tokens[0]);
      int month = Integer.parseInt(tokens[1]);
      double temperature = Double.parseDouble(tokens[2]);
      averageTemperatures.put(new YearAndMonth(year, month), temperature);
    }
    return averageTemperatures;
  }

}
