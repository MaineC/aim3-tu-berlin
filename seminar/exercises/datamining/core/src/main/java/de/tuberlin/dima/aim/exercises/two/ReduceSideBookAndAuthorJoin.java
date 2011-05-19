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
import com.google.common.primitives.Longs;
import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class ReduceSideBookAndAuthorJoin extends HadoopJob {

  private static final Pattern SEPARATOR = Pattern.compile("\t");

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job join = new Job(new Configuration(getConf()));
    Configuration jobConf = join.getConfiguration();

    MultipleInputs.addInputPath(join, authors, TextInputFormat.class, ConvertAuthorsMapper.class);
    MultipleInputs.addInputPath(join, books, TextInputFormat.class, ConvertBooksMapper.class);

    join.setMapOutputKeyClass(SecondarySortedAuthorID.class);
    join.setMapOutputValueClass(AuthorOrTitleAndYearOfPublication.class);
    jobConf.setBoolean("mapred.compress.map.output", true);

    join.setReducerClass(JoinReducer.class);
    join.setOutputKeyClass(Text.class);
    join.setOutputValueClass(NullWritable.class);
    join.setJarByClass(JoinReducer.class);
    join.setJobName("reduceSideBookAuthorJoin");

    join.setOutputFormatClass(TextOutputFormat.class);
    jobConf.set("mapred.output.dir", outputPath.toString());

    join.setGroupingComparatorClass(SecondarySortedAuthorID.GroupingComparator.class);
    join.waitForCompletion(true);

    return 0;
  }

  static class ConvertAuthorsMapper
      extends Mapper<Object,Text,SecondarySortedAuthorID,AuthorOrTitleAndYearOfPublication> {
    @Override
    protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
      String line = value.toString();
      if (line.length() > 0) {
        String[] tokens = SEPARATOR.split(line.toString());
        long authorID = Long.parseLong(tokens[0]);
        String author = tokens[1];
        ctx.write(new SecondarySortedAuthorID(authorID, true), new AuthorOrTitleAndYearOfPublication(author));
      }
    }
  }

  static class ConvertBooksMapper
      extends Mapper<Object,Text,SecondarySortedAuthorID,AuthorOrTitleAndYearOfPublication> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      String[] tokens = SEPARATOR.split(line.toString());
      long authorID = Long.parseLong(tokens[0]);
      short yearOfPublication = Short.parseShort(tokens[1]);
      String title = tokens[2];
      ctx.write(new SecondarySortedAuthorID(authorID, false), new AuthorOrTitleAndYearOfPublication(title,
          yearOfPublication));
    }
  }

  static class JoinReducer
      extends Reducer<SecondarySortedAuthorID,AuthorOrTitleAndYearOfPublication,Text,NullWritable> {
    @Override
    protected void reduce(SecondarySortedAuthorID key, Iterable<AuthorOrTitleAndYearOfPublication> values, Context ctx)
        throws IOException, InterruptedException {
      String author = null;
      for (AuthorOrTitleAndYearOfPublication value : values) {
        if (author == null && !value.containsAuthor()) {
          /* no author found, should not happen in this example */
          throw new IllegalStateException("No author found for book: " + value.getTitle());
        } else if (author == null && value.containsAuthor()) {
          author = value.getAuthor();
        } else {
          ctx.write(new Text(author + '\t' + value.getTitle() + '\t' + value.getYearOfPublication()),
              NullWritable.get());
        }
      }
    }
  }

  static class SecondarySortedAuthorID implements WritableComparable<SecondarySortedAuthorID> {

    private boolean containsAuthor;
    private long id;

    static {
      WritableComparator.define(SecondarySortedAuthorID.class, new SecondarySortComparator());
    }

    SecondarySortedAuthorID() {}

    SecondarySortedAuthorID(long id, boolean containsAuthor) {
      this.id = id;
      this.containsAuthor = containsAuthor;
    }

    @Override
    public int compareTo(SecondarySortedAuthorID other) {
      return ComparisonChain.start()
          .compare(id, other.id)
          .result();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(containsAuthor);
      out.writeLong(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      containsAuthor = in.readBoolean();
      id = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SecondarySortedAuthorID) {
        return id == ((SecondarySortedAuthorID) o).id;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Longs.hashCode(id);
    }

    static class SecondarySortComparator extends WritableComparator implements Serializable {

      protected SecondarySortComparator() {
        super(SecondarySortedAuthorID.class, true);
      }

      @Override
      public int compare(WritableComparable a, WritableComparable b) {
        SecondarySortedAuthorID keyA = (SecondarySortedAuthorID) a;
        SecondarySortedAuthorID keyB = (SecondarySortedAuthorID) b;

        return ComparisonChain.start()
            .compare(keyA.id, keyB.id)
            .compare(!keyA.containsAuthor, !keyB.containsAuthor)
            .result();
      }
    }

    static class GroupingComparator extends WritableComparator implements Serializable {

      protected GroupingComparator() {
        super(SecondarySortedAuthorID.class, true);
      }
    }

  }

  static class AuthorOrTitleAndYearOfPublication implements Writable {

    private boolean containsAuthor;
    private String author;
    private String title;
    private Short yearOfPublication;

    AuthorOrTitleAndYearOfPublication() {}

    AuthorOrTitleAndYearOfPublication(String author) {
      this.containsAuthor = true;
      this.author = Preconditions.checkNotNull(author);
    }

    AuthorOrTitleAndYearOfPublication(String title, short yearOfPublication) {
      this.containsAuthor = false;
      this.title = Preconditions.checkNotNull(title);
      this.yearOfPublication = yearOfPublication;
    }

    public boolean containsAuthor() {
      return containsAuthor;
    }

    public String getAuthor() {
      return author;
    }

    public String getTitle() {
      return title;
    }

    public Short getYearOfPublication() {
      return yearOfPublication;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(containsAuthor);
      if (containsAuthor) {
        out.writeUTF(author);
      } else {
        out.writeUTF(title);
        out.writeShort(yearOfPublication);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      author = null;
      title = null;
      yearOfPublication = null;
      containsAuthor = in.readBoolean();
      if (containsAuthor) {
        author = in.readUTF();
      } else {
        title = in.readUTF();
        yearOfPublication = in.readShort();
      }
    }
  }

}
