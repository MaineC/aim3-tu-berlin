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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.tuberlin.dima.aim.exercises.HadoopAndPactTestcase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class BookAndAuthorJoinTest extends HadoopAndPactTestcase {

  @Test
  public void mapSideInMemoryJoin() throws Exception {
    testJoin(new MapSideInMemoryBookAndAuthorJoin());
  }

  @Test
  public void reduceSideJoin() throws Exception {
    testJoin(new ReduceSideBookAndAuthorJoin());
  }

  void testJoin(Tool bookAndAuthorJoin) throws Exception {
    File authorsFile = getTestTempFile("authors.tsv");
    File booksFile = getTestTempFile("books.tsv");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(authorsFile, readLines("/two/authors.tsv"));
    writeLines(booksFile, readLines("/two/books.tsv"));

    Configuration conf = new Configuration();

    bookAndAuthorJoin.setConf(conf);
    bookAndAuthorJoin.run(new String[]{"--authors", authorsFile.getAbsolutePath(),
        "--books", booksFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath()});

    Multimap<String, Book> booksByAuthors = readBooksByAuthors(new File(outputDir, "part-r-00000"));

    assertTrue(booksByAuthors.containsKey("Charles Bukowski"));
    assertTrue(booksByAuthors.get("Charles Bukowski")
        .contains(new Book("Confessions of a Man Insane Enough to Live with Beasts", 1965)));
    assertTrue(booksByAuthors.get("Charles Bukowski")
        .contains(new Book("Hot Water Music", 1983)));

    assertTrue(booksByAuthors.containsKey("Fyodor Dostoyevsky"));
    assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("Crime and Punishment", 1866)));
    assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("The Brothers Karamazov", 1880)));

  }

  Multimap<String,Book> readBooksByAuthors(File outputFile) throws IOException {
    Multimap<String,Book> booksByAuthors = HashMultimap.create();

    Pattern separator = Pattern.compile("\t");
    for (String line : new FileLineIterable(outputFile)) {
      String[] tokens = separator.split(line);
      booksByAuthors.put(tokens[0], new Book(tokens[1], Integer.parseInt(tokens[2])));
    }
    return booksByAuthors;
  }


  static class Book {

    private final String title;
    private final int year;

    public Book(String title, int year) {
      this.title = Preconditions.checkNotNull(title);
      this.year = year;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Book) {
        Book other = (Book) o;
        return title.equals(other.title) && year == other.year;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 * title.hashCode() + year;
    }
  }

}
