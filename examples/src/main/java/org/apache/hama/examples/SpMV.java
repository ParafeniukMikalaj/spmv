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
package org.apache.hama.examples;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.examples.util.DenseVectorWritable;
import org.apache.hama.examples.util.SparseVectorWritable;
import org.apache.hama.examples.util.WritableUtil;
import org.apache.hama.util.KeyValuePair;

/**
 * Sparse matrix vector multiplication. Currently it uses row-wise access.
 * Assumptions: 1) each peer should have copy of input vector for efficient
 * operations. 2) row-wise implementation is good because we don't need to care
 * about communication 3) the main way to improve performance - create custom
 * Partitioner
 */
public class SpMV {

  private static final String outputPathString = "spmv.outputpath";
  private static final String resultPathString = "spmv.resultpath";
  private static final String inputMatrixPathString = "spmv.inputmatrixpath";
  private static final String inputVectorPathString = "spmv.inputvectorpath";
  private static String requestedBspTasksString = "bsptask.count";
  private static final String spmvSuffix = "/spmv/";
  private static final String intermediate = "/part";

  enum RowCounter {
    TOTAL_ROWS
  }

  private static Configuration conf;

  public static Configuration getConf() {
    return conf;
  }

  public static void setConf(Configuration conf) {
    SpMV.conf = conf;
  }

  public static String getOutputPath(Configuration conf) {
    return conf.get(outputPathString, null);
  }

  public static void setOutputPath(Configuration conf, String outputPath) {
    Path path = new Path(outputPath);
    path = path.suffix(intermediate);
    conf.set(outputPathString, path.toString());
  }

  public static String getResultPath(Configuration conf) {
    return conf.get(resultPathString, null);
  }

  public static String getResultPath() {
    return getConf().get(resultPathString, null);
  }

  private static void setResultPath(Configuration conf, String resultPath) {
    conf.set(resultPathString, resultPath);
  }

  public static String getInputMatrixPath(Configuration conf) {
    return conf.get(inputMatrixPathString, null);
  }

  public static void setInputMatrixPath(Configuration conf, String inputPath) {
    conf.set(inputMatrixPathString, inputPath);
  }

  public static String getInputVectorPath(Configuration conf) {
    return conf.get(inputVectorPathString, null);
  }

  public static void setInputVectorPath(Configuration conf, String inputPath) {
    conf.set(inputVectorPathString, inputPath);
  }

  public static int getRequestedBspTasksCount(Configuration conf) {
    return conf.getInt(requestedBspTasksString, -1);
  }

  public static void setRequestedBspTasksCount(Configuration conf,
      int requestedBspTasksCount) {
    conf.setInt(requestedBspTasksString, requestedBspTasksCount);
  }

  private static String generateOutPath(Configuration conf) {
    String prefix = conf.get("hadoop.tmp.dir", "/tmp");
    String pathString = prefix + spmvSuffix + System.currentTimeMillis();
    return pathString;
  }

  /**
   * IMPORTANT: This can be a bottle neck. Problem can be here{@core
   * WritableUtil.convertSpMVOutputToDenseVector()}
   */
  private static void convertToDenseVector(Configuration conf, int size)
      throws IOException {
    WritableUtil util = new WritableUtil();
    String resultPath = util.convertSpMVOutputToDenseVector(
        getOutputPath(conf), conf, size);
    setResultPath(conf, resultPath);
  }

  /**
   * This class performs sparse matrix vector multiplication. u = m * v.
   */
  private static class SpMVExecutor
      extends
      BSP<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, NullWritable> {
    private DenseVectorWritable v;

    /**
     * Each peer reads input dense vector.
     */
    @Override
    public void setup(
        BSPPeer<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      // reading input vector, which represented as matrix row
      Configuration conf = peer.getConfiguration();
      WritableUtil util = new WritableUtil();
      v = new DenseVectorWritable();
      util.readFromFile(getInputVectorPath(conf), v, conf);
    }

    /**
     * Local inner product computation and output.
     */
    @Override
    public void bsp(
        BSPPeer<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      KeyValuePair<IntWritable, SparseVectorWritable> row = null;
      while ((row = peer.readNext()) != null) {
        // it will be needed in conversion of output to result vector
        peer.getCounter(RowCounter.TOTAL_ROWS).increment(1L);
        int key = row.getKey().get();
        int sum = 0;
        SparseVectorWritable mRow = row.getValue();
        if (v.getSize() != mRow.getSize())
          throw new RuntimeException("Matrix row with index = " + key
              + " is not consistent with input vector. Row size = "
              + mRow.getSize() + " vector size = " + v.getSize());
        List<Integer> mIndeces = mRow.getIndeces();
        List<Double> mValues = mRow.getValues();
        for (int i = 0; i < mIndeces.size(); i++)
          sum += v.get(mIndeces.get(i)) * mValues.get(i);
        peer.write(new IntWritable(key), new DoubleWritable(sum));
      }
    }

  }

  /**
   * Method which actually starts SpMV.
   */
  private static void startTask(HamaConfiguration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    BSPJob bsp = new BSPJob(conf, SpMV.class);
    bsp.setJobName("Sparse matrix vector multiplication");
    bsp.setBspClass(SpMVExecutor.class);
    /*
     * Input matrix is presented as pairs of integer and {@ link
     * SparseVectorWritable}. Output is pairs of integer and double
     */
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setInputPath(new Path(getInputMatrixPath(conf)));

    if (getOutputPath(conf) == null)
      setOutputPath(conf, generateOutPath(conf));
    FileOutputFormat.setOutputPath(bsp, new Path(getOutputPath(conf)));

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    if (getRequestedBspTasksCount(conf) != -1) {
      bsp.setNumBspTask(getRequestedBspTasksCount(conf));
    } else {
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds.");
      // FIXME get counter value instead of hardcode
      convertToDenseVector(conf, 4);
      System.out.println("Result is in " + getResultPath(conf));
    } else {
      setResultPath(conf, null);
    }
  }

  private static void printUsage() {
    System.out
        .println("Usage: spmv <input matrix> <intput vector> [output vector] [number of tasks (default 20)]");
    System.exit(-1);
  }

  /**
   * Function parses command line in standart form. See {@link #printUsage()}
   * for more info.
   */
  private static void parseArgs(HamaConfiguration conf, String[] args) {
    if (args.length < 2)
      printUsage();

    SpMV.setInputMatrixPath(conf, args[0]);
    SpMV.setInputVectorPath(conf, args[1]);

    if (args.length == 4) {
      try {
        int taskCount = Integer.parseInt(args[3]);
        if (taskCount < 0)
          throw new IllegalArgumentException(
              "The number of requested tasks can't be negative. Actual value: "
                  + String.valueOf(taskCount));
        SpMV.setRequestedBspTasksCount(conf, taskCount);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "The format of requested task count is int. Can not parse value: "
                + args[3]);
      }
    }

    if (args.length >= 3) {
      SpMV.setOutputPath(conf, args[2]);
    }
  }

  /**
   * This method gives opportunity to start SpMV with command line.
   */
  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration();
    parseArgs(conf, args);
    setConf(conf);
    startTask(conf);
  }

}
