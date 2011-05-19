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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.regex.Pattern;

public class MapSideInMemoryBookAndAuthorJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job inMemoryJoin = prepareJob(books, outputPath, TextInputFormat.class, JoinMapper.class, Text.class, Book.class,
        Reducer.class, Text.class, Book.class, TextOutputFormat.class);
    inMemoryJoin.getConfiguration().set(JoinMapper.AUTHORS_FILE, authors.toString());
    inMemoryJoin.waitForCompletion(true);

    return 0;
  }

  static class JoinMapper extends Mapper<Object,Text,Text,Book> {

    static final String AUTHORS_FILE = JoinMapper.class.getName() + ".authorsFile";

    private FastByIDMap<String> authors;

    private static final Pattern SEPARATOR = Pattern.compile("\t");

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      authors = new FastByIDMap<String>();
      Path authorsPath = new Path(ctx.getConfiguration().get(AUTHORS_FILE));
      FileSystem fs = authorsPath.getFileSystem(ctx.getConfiguration());
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new InputStreamReader(fs.open(authorsPath), Charsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
          String[] tokens = SEPARATOR.split(line);
          long authorID = Long.parseLong(tokens[0]);
          String author = tokens[1];
          authors.put(authorID, author);
        }
      } finally {
        Closeables.closeQuietly(reader);
      }
    }

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      String[] tokens = SEPARATOR.split(line.toString());

      long authorID = Long.parseLong(tokens[0]);
      short yearOfPublication = Short.parseShort(tokens[1]);
      String title = tokens[2];
      String author = authors.get(authorID);

      ctx.write(new Text(author), new Book(title, yearOfPublication));
    }
  }

  static class Book implements Writable {

    private String title;
    private short yearOfPublication;

    Book() {}

    Book(String title, short yearOfPublication) {
      this.title = Preconditions.checkNotNull(title);
      this.yearOfPublication = yearOfPublication;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(title);
      out.writeShort(yearOfPublication);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      title = in.readUTF();
      yearOfPublication = in.readShort();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Book) {
        Book other = (Book) o;
        return title.equals(other.title) && yearOfPublication == other.yearOfPublication;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return title.hashCode() * 31 + yearOfPublication;
    }

    @Override
    public String toString() {
      return title + '\t' + yearOfPublication;
    }
  }

}
