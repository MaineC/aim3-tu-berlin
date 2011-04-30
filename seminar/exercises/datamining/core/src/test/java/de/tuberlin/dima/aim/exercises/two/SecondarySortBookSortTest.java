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

package de.tuberlin.dima.aim.exercises.two;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import de.tuberlin.dima.aim.exercises.hadoop.HadoopTestcase;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecondarySortBookSortTest extends HadoopTestcase {

  @Test
  public void testSorting() throws Exception {
    File booksFile = getTestTempFile("books.tsv");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(booksFile, readLines("/two/books.tsv"));

    SecondarySortBookSort bookSort = new SecondarySortBookSort();

    Configuration conf = new Configuration();

    bookSort.setConf(conf);
    bookSort.run(new String[] { "--input", booksFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath() });

    File outputFile = new File(outputDir, "part-r-00000");

    if (!outputFile.exists()) {
      fail();
    }

    CenturyAndTitle[] centuryAndTitlesFromInput = asListFromInput(booksFile);
    CenturyAndTitle[] centuryAndTitlesFromHadoop = asListFromHadoopOut(outputFile);

    Arrays.sort(centuryAndTitlesFromInput);

    assertTrue(Arrays.deepEquals(centuryAndTitlesFromInput, centuryAndTitlesFromHadoop));
  }

  CenturyAndTitle[] asListFromHadoopOut(File file) throws IOException {
    Pattern separator = Pattern.compile("\t");
    List<CenturyAndTitle> centuryAndTitles = Lists.newArrayList();
    for (String line : new FileLineIterable(file)) {
      String[] tokens = separator.split(line);
      centuryAndTitles.add(new CenturyAndTitle(Integer.parseInt(tokens[0]), tokens[1]));
    }

    return centuryAndTitles.toArray(new CenturyAndTitle[centuryAndTitles.size()]);
  }

  CenturyAndTitle[] asListFromInput(File file) throws IOException {
    Pattern separator = Pattern.compile("\t");
    List<CenturyAndTitle> centuryAndTitles = Lists.newArrayList();
    for (String line : new FileLineIterable(file)) {
      String[] tokens = separator.split(line);
      centuryAndTitles.add(new CenturyAndTitle(Integer.parseInt(tokens[1].substring(0, 2)), tokens[2]));
    }

    return centuryAndTitles.toArray(new CenturyAndTitle[centuryAndTitles.size()]);
  }

  static class CenturyAndTitle implements Comparable<CenturyAndTitle> {

    private final int century;
    private final String title;

    public CenturyAndTitle(int century, String title) {
      this.century = century;
      this.title = Preconditions.checkNotNull(title);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CenturyAndTitle) {
        CenturyAndTitle other = (CenturyAndTitle) o;
        return title.equals(other.title) && century == other.century;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 * title.hashCode() + century;
    }

    @Override
    public int compareTo(CenturyAndTitle other) {
      return ComparisonChain.start()
          .compare(century, other.century)
          .compare(title, other.title).result();
    }
  }

}
