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

package de.tuberlin.dima.aim.exercises.three;

import com.google.common.collect.Lists;
import de.tuberlin.dima.aim.exercises.HadoopAndPactTestcase;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.testing.TestPlan;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class AverageTemperaturePerMonthPactTest extends HadoopAndPactTestcase {

  private static final Pattern SEPARATOR = Pattern.compile("\t");

  @Test
  public void computeTemperatures() throws IOException {

    MapContract<PactNull, PactString, AverageTemperaturePerMonthPact.YearMonthKey, PactInteger> mapContract =
        new MapContract<PactNull, PactString, AverageTemperaturePerMonthPact.YearMonthKey, PactInteger>(
        AverageTemperaturePerMonthPact.TemperaturePerYearAndMonthMapper.class);

    ReduceContract<AverageTemperaturePerMonthPact.YearMonthKey, PactInteger,
        AverageTemperaturePerMonthPact.YearMonthKey, PactDouble> reduceContract =
        new ReduceContract<AverageTemperaturePerMonthPact.YearMonthKey, PactInteger,
        AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>(
        AverageTemperaturePerMonthPact.TemperatePerYearAndMonthReducer.class);

    reduceContract.setInput(mapContract);

    TestPlan testPlan = new TestPlan(reduceContract);

    for (String line : readLines("/one/temperatures.tsv")) {
      testPlan.getInput().add(PactNull.getInstance(), new PactString(line));
    }

    testPlan.setAllowedPactDoubleDelta(0.0001);
    for (KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble> expectedResult : expectedResults()) {
      testPlan.getExpectedOutput().add(expectedResult.getKey(), expectedResult.getValue());
    }

    testPlan.run();
  }

  Iterable<KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>> expectedResults() throws IOException {
    List<KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>> results = Lists.newArrayList();
    for (String line : readLines("/three/averageTemperatures.tsv")) {
      String[] tokens = SEPARATOR.split(line);
      results.add(new KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>(
          new AverageTemperaturePerMonthPact.YearMonthKey(Short.parseShort(tokens[0]), Short.parseShort(tokens[1])),
          new PactDouble(Double.parseDouble(tokens[2]))));
    }
    return results;
  }



}
