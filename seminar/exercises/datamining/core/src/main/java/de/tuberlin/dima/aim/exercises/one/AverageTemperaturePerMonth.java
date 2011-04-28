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

import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;

import com.google.common.collect.ComparisonChain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

public class AverageTemperaturePerMonth extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job averageTemperature = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringDataMapper.class, 
        YearMonthWritable.class, IntWritable.class, AveragingReducer.class, YearMonthWritable.class, 
        DoubleWritable.class, TextOutputFormat.class);
    averageTemperature.getConfiguration().set(FilteringDataMapper.MINIMUM_QUALITY_PARAM, 
        String.valueOf(minimumQuality));
    averageTemperature.waitForCompletion(true);
    
    return 0;
  }
  
  static class FilteringDataMapper extends Mapper<Object,Text,YearMonthWritable,IntWritable> {

    private double minimumQuality;

    static final String MINIMUM_QUALITY_PARAM = FilteringDataMapper.class.getName() + ".minimumQuality";

    private static final Pattern SEPARATOR = Pattern.compile("\t");

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      minimumQuality = Double.parseDouble(ctx.getConfiguration().get(MINIMUM_QUALITY_PARAM, "0"));
    }

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      String[] tokens = SEPARATOR.split(line.toString());

      int year = Integer.parseInt(tokens[0]);
      int month = Integer.parseInt(tokens[1]);
      int temperature = Integer.parseInt(tokens[2]);
      double quality = Double.parseDouble(tokens[3]);

      if (quality >= minimumQuality) {
      ctx.write(new YearMonthWritable(year, month), new IntWritable(temperature));
      }
    }
  }
  
  static class AveragingReducer extends Reducer<YearMonthWritable,IntWritable,YearMonthWritable,DoubleWritable> {
    @Override
    protected void reduce(YearMonthWritable yearMonth, Iterable<IntWritable> temperatures, Context ctx)
        throws IOException, InterruptedException {
      RunningAverage avg = new FullRunningAverage();
      for (IntWritable temperature : temperatures) {
        avg.addDatum(temperature.get());
      }
      ctx.write(yearMonth, new DoubleWritable(avg.getAverage()));
    }
  }
  
  static class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    private int year;
    private int month;
	
    public YearMonthWritable() {
      super();
    }

    public YearMonthWritable(int year, int month) {
      super();
      this.year = year;
      this.month = month;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      year = in.readInt();
      month = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(year);
      out.writeInt(month);
    }

    @Override
    public int compareTo(YearMonthWritable other) {
      return ComparisonChain.start()
          .compare(year, other.year)
          .compare(month, other.month)
          .result();
    }

    @Override
    public String toString() {
      return year + "\t" + month;
    }
  }
  
}