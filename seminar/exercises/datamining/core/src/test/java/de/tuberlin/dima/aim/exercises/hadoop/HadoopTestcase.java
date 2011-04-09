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

package de.tuberlin.dima.aim.exercises.hadoop;

import de.tuberlin.dima.aim.exercises.Testcase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.RandomUtils;
import org.junit.After;
import org.junit.Before;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;

public abstract class HadoopTestcase extends Testcase {

  private File testTempDir;
  private Path testTempDirPath;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    RandomUtils.useTestSeed();
    testTempDir = null;
    testTempDirPath = null;
    fs = null;
  }

  @After
  public void tearDown() throws Exception {
    if (testTempDirPath != null) {
      try {
        fs.delete(testTempDirPath, true);
      } catch (IOException e) {
        throw new IllegalStateException("Test file not found");
      }
      testTempDirPath = null;
      fs = null;
    }
    if (testTempDir != null) {
      new DeletingVisitor().accept(testTempDir);
    }
  }

  protected static void writeLines(File file, String... lines) throws FileNotFoundException {
    writeLines(file, Arrays.asList(lines));
  }

  protected static void writeLines(File file, Iterable<String> lines) throws FileNotFoundException {
    PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), Charset.forName("UTF-8")));
    try {
      for (String line : lines) {
        writer.println(line);
      }
    } finally {
      writer.close();
    }
  }

  protected final Path getTestTempDirPath() throws IOException {
    if (testTempDirPath == null) {
      fs = FileSystem.get(new Configuration());
      long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
      testTempDirPath = fs.makeQualified(
          new Path("/tmp/mahout-" + getClass().getSimpleName() + '-' + simpleRandomLong));
      if (!fs.mkdirs(testTempDirPath)) {
        throw new IOException("Could not create " + testTempDirPath);
      }
      fs.deleteOnExit(testTempDirPath);
    }
    return testTempDirPath;
  }

  protected final Path getTestTempFilePath(String name) throws IOException {
    return getTestTempFileOrDirPath(name, false);
  }

  protected final Path getTestTempDirPath(String name) throws IOException {
    return getTestTempFileOrDirPath(name, true);
  }

  private Path getTestTempFileOrDirPath(String name, boolean dir) throws IOException {
    Path testTempDirPath = getTestTempDirPath();
    Path tempFileOrDir = fs.makeQualified(new Path(testTempDirPath, name));
    fs.deleteOnExit(tempFileOrDir);
    if (dir && !fs.mkdirs(tempFileOrDir)) {
      throw new IOException("Could not create " + tempFileOrDir);
    }
    return tempFileOrDir;
  }

  protected final File getTestTempDir() throws IOException {
    if (testTempDir == null) {
      String systemTmpDir = System.getProperty("java.io.tmpdir");
      long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
      testTempDir = new File(systemTmpDir, "mahout-" + getClass().getSimpleName() + '-' + simpleRandomLong);
      if (!testTempDir.mkdir()) {
        throw new IOException("Could not create " + testTempDir);
      }
      testTempDir.deleteOnExit();
    }
    return testTempDir;
  }

  protected final File getTestTempFile(String name) throws IOException {
    return getTestTempFileOrDir(name, false);
  }

  protected final File getTestTempDir(String name) throws IOException {
    return getTestTempFileOrDir(name, true);
  }

  private File getTestTempFileOrDir(String name, boolean dir) throws IOException {
    File f = new File(getTestTempDir(), name);
    f.deleteOnExit();
    if (dir && !f.mkdirs()) {
      throw new IOException("Could not make directory " + f);
    }
    return f;
  }

  private static class DeletingVisitor implements FileFilter {
    @Override
    public boolean accept(File f) {
      if (!f.isFile()) {
        f.listFiles(this);
      }
      f.delete();
      return false;
    }
  }
}
