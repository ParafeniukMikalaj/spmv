/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.examples.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;

/**
 * This class is supposed to hide some operations for converting matrices and
 * vectors from file format to in-memory format. Also contains method for
 * converting output of SpMV task to DenseVectorWritable Most of methods are
 * only needed for test purposes.
 */
public class WritableUtil {

  protected static final Log LOG = LogFactory.getLog(WritableUtil.class);

  /**
   * Method used to test RandomMatrixGenerator. Reads input matrix from
   * specified path and prints to System.out
   */
  public static void readMatrix(String pathString) throws IOException {
    HamaConfiguration conf = new HamaConfiguration();
    Path dir = new Path(pathString);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stats = fs.listStatus(dir);
    for (FileStatus stat : stats) {
      String filePath = stat.getPath().toUri().getPath(); // gives directory
                                                          // name
      try {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
            filePath), conf);
        IntWritable key = new IntWritable();
        SparseVectorWritable value = new SparseVectorWritable();
        while (reader.next(key, value)) {
          System.out.println(key.toString());
          System.out.println(value.toString());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  /**
   * This method gives the ability to write matrix from memory to file. It
   * should be used with small matrices and for test purposes only.
   * 
   * @param pathString
   *          path to file where matrix will be writed.
   * @param conf
   *          configuration
   * @param matrix
   *          map of row indeces and values presented as Writable
   * @throws IOException
   */
  public static void writeMatrix(String pathString, Configuration conf,
      Map<Integer, Writable> matrix) throws IOException {
    boolean inited = false;
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Writer writer = null;
    try {
      for (Integer index : matrix.keySet()) {
        IntWritable key = new IntWritable(index);
        Writable value = matrix.get(index);
        if (!inited) {
          writer = new SequenceFile.Writer(fs, conf, new Path(pathString),
              IntWritable.class, value.getClass());
          inited = true;
        }
        writer.append(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null)
        writer.close();
    }

  }

  /**
   * This method is used to read vector from specified path in SpMVTest. For
   * test purposes only.
   * 
   * @param pathString
   *          input path for vector
   * @param result
   *          instanse of vector writable which should be filled.
   * @param conf
   *          configuration
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public static void readFromFile(String pathString, Writable result,
      Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Reader reader = null;
    Path path = new Path(pathString);
    List<String> filePaths = new ArrayList<String>();
    // TODO this deprecation should be fixed.
    if (fs.isDirectory(path)) {
      FileStatus[] stats = fs.listStatus(path);
      for (FileStatus stat : stats) {
        filePaths.add(stat.getPath().toUri().getPath());
      }
    } else if (fs.isFile(path)) {
      filePaths.add(path.toString());
    }
    try {
      for (String filePath : filePaths) {
        reader = new SequenceFile.Reader(fs, new Path(filePath), conf);
        IntWritable key = new IntWritable();
        reader.next(key, result);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * This method is used to write vector from memory to specified path.
   * 
   * @param pathString
   *          output path
   * @param result
   *          instance of vector to be writed
   * @param conf
   *          configuration
   * @throws IOException
   */
  public static void writeToFile(String pathString, Writable result, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Writer writer = null;
    try {
      writer = new SequenceFile.Writer(fs, conf, new Path(pathString),
          IntWritable.class, result.getClass());
      IntWritable key = new IntWritable();
      writer.append(key, result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null)
        writer.close();
    }
  }

  /**
   * SpMV produces a file, which contains result dense vector in format of pairs
   * of integer and double. The aim of this method is to convert SpMV output to
   * format usable in subsequent computation - dense vector. It can be usable
   * for iterative solvers. IMPORTANT: currently it is used in SpMV. It can be a
   * bottle neck, because all input needs to be stored in memory.
   * 
   * @param SpMVoutputPathString
   *          output path, which represents directory with part files.
   * @param conf
   *          configuration
   * @return path to output vector.
   * @throws IOException
   */
  public static String convertSpMVOutputToDenseVector(String SpMVoutputPathString,
      Configuration conf) throws IOException {
    List<Integer> indeces = new ArrayList<Integer>();
    List<Double> values = new ArrayList<Double>();

    FileSystem fs = FileSystem.get(conf);
    Path SpMVOutputPath = new Path(SpMVoutputPathString);
    Path resultOutputPath = SpMVOutputPath.getParent().suffix("/result");
    FileStatus[] stats = fs.listStatus(SpMVOutputPath);
    for (FileStatus stat : stats) {
      String filePath = stat.getPath().toUri().getPath();
      SequenceFile.Reader reader = null;
      fs.open(new Path(filePath));
      try {
        reader = new SequenceFile.Reader(fs, new Path(filePath), conf);
        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();
        while (reader.next(key, value)) {
          indeces.add(key.get());
          values.add(value.get());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (reader != null)
          reader.close();
      }
    }
    DenseVectorWritable result = new DenseVectorWritable();
    result.setSize(indeces.size());
    for (int i = 0; i < indeces.size(); i++)
      result.addCell(indeces.get(i), values.get(i));
    writeToFile(resultOutputPath.toString(), result, conf);
    return resultOutputPath.toString();
  }
}
