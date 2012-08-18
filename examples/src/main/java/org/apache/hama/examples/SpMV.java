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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  protected static final Log LOG = LogFactory.getLog(SpMV.class);
  private static String resultPath;
  private static final String outputPathString = "spmv.outputpath";
  private static final String inputMatrixPathString = "spmv.inputmatrixpath";
  private static final String inputVectorPathString = "spmv.inputvectorpath";
  private static String requestedBspTasksString = "bsptask.count";
  private static final String intermediate = "/part";

  enum RowCounter {
    TOTAL_ROWS
  }

  public static String getResultPath() {
    return resultPath;
  }

  public static void setResultPath(String resultPath) {
    SpMV.resultPath = resultPath;
  }

  /**
   * IMPORTANT: This can be a bottle neck. Problem can be here{@core
   * WritableUtil.convertSpMVOutputToDenseVector()}
   */
  private static void convertToDenseVector(Configuration conf)
      throws IOException {
    WritableUtil util = new WritableUtil();
    String resultPath = util.convertSpMVOutputToDenseVector(
        conf.get(outputPathString), conf);
    setResultPath(resultPath);
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
      util.readFromFile(conf.get(inputVectorPathString), v, conf);
      peer.sync();
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
        double sum = 0;
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
     * Input matrix is presented as pairs of integer and SparseVectorWritable.
     * Output is pairs of integer and double
     */
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setInputPath(new Path(conf.get(inputMatrixPathString)));

    FileOutputFormat.setOutputPath(bsp, new Path(conf.get(outputPathString)));

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    int requestedTasks = conf.getInt(requestedBspTasksString, -1);
    if (requestedTasks != -1) {
      bsp.setNumBspTask(requestedTasks);
    } else {
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds.");
      convertToDenseVector(conf);
      System.out.println("Result is in " + getResultPath());
    } else {
      setResultPath(null);
    }
  }

  private static void printUsage() {
    System.out
        .println("Usage: spmv <input matrix> <intput vector> <output vector> [number of tasks (default max)]");
  }

  /**
   * Function parses command line in standart form.
   */
  private static void parseArgs(HamaConfiguration conf, String[] args) {
    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }

    conf.set(inputMatrixPathString, args[0]);
    conf.set(inputVectorPathString, args[1]);

    Path path = new Path(args[2]);
    path = path.suffix(intermediate);
    conf.set(outputPathString, path.toString());

    if (args.length == 4) {
      try {
        int taskCount = Integer.parseInt(args[3]);
        if (taskCount < 0) {
          printUsage();
          throw new IllegalArgumentException(
              "The number of requested tasks can't be negative. Actual value: "
                  + String.valueOf(taskCount));
        }
        conf.setInt(requestedBspTasksString, taskCount);
      } catch (NumberFormatException e) {
        printUsage();
        throw new IllegalArgumentException(
            "The format of requested task count is int. Can not parse value: "
                + args[3]);
      }
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration();
    parseArgs(conf, args);
    startTask(conf);
  }

}
