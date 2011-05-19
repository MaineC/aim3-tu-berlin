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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.regex.Pattern;

public class SecondarySortBookSort extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job secondarySort = prepareJob(inputPath, outputPath, TextInputFormat.class, ByCenturyAndTitleMapper.class,
        BookSortKey.class, Text.class, SecondarySortBookSortReducer.class, BookSortKey.class, Text.class,
        TextOutputFormat.class);
    secondarySort.setGroupingComparatorClass(BookSortKey.GroupingComparator.class);
    secondarySort.waitForCompletion(true);

    return 0;
  }

  static class ByCenturyAndTitleMapper extends Mapper<Object,Text,BookSortKey,Text> {

    private static final Pattern SEPARATOR = Pattern.compile("\t");

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      String[] tokens = SEPARATOR.split(line.toString());
      short century = Short.parseShort(tokens[1].substring(0, 2));
      String title = tokens[2];
      ctx.write(new BookSortKey(century, title), new Text(title));
    }
  }

  static class SecondarySortBookSortReducer extends Reducer<BookSortKey,Text,Text,NullWritable> {
    @Override
    protected void reduce(BookSortKey bookSortKey, Iterable<Text> values, Context ctx)
        throws IOException, InterruptedException {
      for (Text value : values) {
        String out = Joiner.on('\t').skipNulls().join(new Object[] { bookSortKey.toString(), value.toString() });
        ctx.write(new Text(out), NullWritable.get());
      }
    }
  }

  static class BookSortKey implements WritableComparable<BookSortKey> {

    private short century;
    private String title;

    BookSortKey() {}

    BookSortKey(short century, String title) {
      this.century = century;
      this.title = Preconditions.checkNotNull(title);
    }

    static {
      WritableComparator.define(BookSortKey.class, new SecondarySortComparator());
    }

    @Override
    public int compareTo(BookSortKey other) {
      return century == other.century ? 0 : century < other.century ? -1 : 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(century);
      out.writeUTF(title);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      century = in.readShort();
      title = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof BookSortKey) {
        BookSortKey other = (BookSortKey) o;
        return century == other.century;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return century;
    }

    @Override
    public String toString() {
      return String.valueOf(century);
    }

    static class SecondarySortComparator extends WritableComparator implements Serializable {

      protected SecondarySortComparator() {
        super(BookSortKey.class, true);
      }

      @Override
      public int compare(WritableComparable a, WritableComparable b) {
        BookSortKey keyA = (BookSortKey) a;
        BookSortKey keyB = (BookSortKey) b;

        return ComparisonChain.start()
            .compare(keyA.century, keyB.century)
            .compare(keyA.title, keyB.title)
            .result();
      }
    }

    static class GroupingComparator extends WritableComparator implements Serializable {

      protected GroupingComparator() {
        super(BookSortKey.class, true);
      }
    }

  }

}
