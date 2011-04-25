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
import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class SecondarySortBookSort extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    //IMPLEMENT ME

    return 0;
  }

  static class ByCenturyAndTitleMapper extends Mapper<Object,Text,BookSortKey,Text> {

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      // IMPLEMENT ME
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

    BookSortKey() {}

    @Override
    public int compareTo(BookSortKey other) {
      // IMPLEMENT ME
      return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // IMPLEMENT ME
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // IMPLEMENT ME
    }

    @Override
    public boolean equals(Object o) {
      // IMPLEMENT ME
      return true;
    }

    @Override
    public int hashCode() {
      // IMPLEMENT ME
      return 0;
    }
  }

}
