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
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.testing.TestPairs;
import eu.stratosphere.pact.testing.TestPlan;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class BookAndAuthorJoinPactTest extends HadoopAndPactTestcase {

  private static final Pattern SEPARATOR = Pattern.compile("\t");

  @Test
  public void join() throws IOException {
    MatchContract<PactLong,PactString,BookAndAuthorJoinPact.BookAndYear,PactString,BookAndAuthorJoinPact.BookAndYear>
        matchContract = new MatchContract<PactLong,PactString,BookAndAuthorJoinPact.BookAndYear,PactString,
        BookAndAuthorJoinPact.BookAndYear>(BookAndAuthorJoinPact.BookAndAuthorMatch.class);

    TestPlan testPlan = new TestPlan(matchContract);

    for (KeyValuePair<PactLong, PactString> author : authors()) {
      testPlan.getInput().add(author.getKey(), author.getValue());
    }

    TestPairs<Key,Value> secondInput =
        testPlan.getInput((DataSourceContract<Key, Value>) matchContract.getSecondInput());
    for (KeyValuePair<PactLong,BookAndAuthorJoinPact.BookAndYear> book : books()) {
      secondInput.add(book.getKey(), book.getValue());
    }

    for (KeyValuePair<PactString,BookAndAuthorJoinPact.BookAndYear> result : results()) {
      testPlan.getExpectedOutput().add(result.getKey(), result.getValue());
    }

    testPlan.run();
  }

  Iterable<KeyValuePair<PactLong, PactString>> authors() throws IOException {
    List<KeyValuePair<PactLong,PactString>> results = Lists.newArrayList();
    for (String line : readLines("/two/authors.tsv")) {
      String[] tokens = SEPARATOR.split(line);
      results.add(new KeyValuePair<PactLong, PactString>(new PactLong(Long.parseLong(tokens[0])),
          new PactString(tokens[1])));
    }
    return results;
  }

  Iterable<KeyValuePair<PactLong,BookAndAuthorJoinPact.BookAndYear>> books() throws IOException {
    List<KeyValuePair<PactLong,BookAndAuthorJoinPact.BookAndYear>> results = Lists.newArrayList();
    for (String line : readLines("/two/books.tsv")) {
      String[] tokens = SEPARATOR.split(line);
      BookAndAuthorJoinPact.BookAndYear bookAndYear = new BookAndAuthorJoinPact.BookAndYear(tokens[2],
          Short.parseShort(tokens[1]));
      results.add(new KeyValuePair<PactLong, BookAndAuthorJoinPact.BookAndYear>(new PactLong(Long.parseLong(tokens[0])),
          bookAndYear));
    }
    return results;
  }

  Iterable<KeyValuePair<PactString,BookAndAuthorJoinPact.BookAndYear>> results() throws IOException {
    List<KeyValuePair<PactString,BookAndAuthorJoinPact.BookAndYear>> results = Lists.newArrayList();
    for (String line : readLines("/three/joinedBooksAndAuthors.tsv")) {
      System.out.println(line);
      String[] tokens = SEPARATOR.split(line);
      BookAndAuthorJoinPact.BookAndYear bookAndYear = new BookAndAuthorJoinPact.BookAndYear(tokens[1],
          Short.parseShort(tokens[2]));
      results.add(new KeyValuePair<PactString, BookAndAuthorJoinPact.BookAndYear>(new PactString(tokens[0]),
          bookAndYear));
    }
    return results;
  }
}