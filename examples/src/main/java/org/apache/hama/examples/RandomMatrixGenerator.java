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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.examples.util.DenseVectorWritable;
import org.apache.hama.examples.util.SparseVectorWritable;

/**
 * This class can generate random matrix. It uses {@link MyGenerator}. You can
 * specify different options in command line. {@link parseArgs} for more info.
 * Option for symmetric matrices is not supported yet. Currently it implements
 * row-wise logic, which is usable for {@link SpMV}
 */
public class RandomMatrixGenerator {

  public static String requestedBspTasksString = "bsptask.count";
  public static String sparsityString = "randomgenerator.sparsity";
  public static String rowsString = "randomgenerator.rows";
  public static String columnsString = "randomgenerator.columns";
  public static String outputString = "randomgenerator.output";

  /*
   * IMPORTANT NOTE: I tried to use enums for counters, but I couldn't access
   * them after bsp computations. So I preferred static counter. It sould be
   * checked.
   */
  enum TotalCounter {
    TOTAL
  }

  /**
   * Class which currently implements row-wise logic. In case of sparsity > 0.5
   * can get not exact number of generated items, as expected. But it was made
   * for efficiency.
   */
  public static class MyGenerator
      extends
      BSP<NullWritable, NullWritable, IntWritable, Writable, NullWritable> {
    public static final Log LOG = LogFactory.getLog(MyGenerator.class);

    private static int rows, columns;
    private static float sparsity;
    private static int remainder, quotient, needed;
    private static int peerCount = 1;
    private static Random rand;
    private static double criticalSparsity = 0.5;

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, IntWritable, Writable, NullWritable> peer)
        throws IOException {
      Configuration conf = peer.getConfiguration();
      sparsity = conf.getFloat(sparsityString, 0.1f);
      rows = conf.getInt(rowsString, 10);
      columns = conf.getInt(columnsString, 10);
      int total = rows * columns;
      peerCount = peer.getNumPeers();
      rand = new Random();
      needed = (int) (total * sparsity);
      remainder = needed % rows;
      quotient = needed / rows;
    }

    /**
     * The algorithm is as follows: each peer counts, how many items it needs to
     * generate, after that it uses algorithms for sparse matrices if sparsity <
     * 0.5 and algorithm for dense matrices otherwise. NOTE: in case of sparsity
     * > 0.5 number of generated items can differ from expected count.
     */
    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, IntWritable, Writable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {

      List<String> peerNamesList = Arrays.asList(peer.getAllPeerNames());
      int peerIndex = peerNamesList.indexOf(peer.getPeerName());

      HashSet<Integer> createdIndeces = new HashSet<Integer>();
      List<Integer> rowIndeces = new ArrayList<Integer>();
      int tmpIndex = peerIndex;
      while (tmpIndex < rows) {
        rowIndeces.add(tmpIndex);
        tmpIndex += peerCount;
      }

      for (int rowIndex : rowIndeces) {
        Writable row;
        createdIndeces.clear();
        int needsToGenerate = quotient;
        if (rowIndex < remainder)
          needsToGenerate++;
        if (sparsity < criticalSparsity) {
          // algorithm for sparse matrices.
          SparseVectorWritable vector = new SparseVectorWritable();
          vector.setSize(columns);
          while (createdIndeces.size() < needsToGenerate) {
            int index = (int) (rand.nextDouble() * columns);
            if (!createdIndeces.contains(index)) {
              peer.getCounter(TotalCounter.TOTAL).increment(1L);
              double value = rand.nextDouble();
              vector.addCell(index, value);
              createdIndeces.add(index);
            }
          }
          row = vector;
        } else {
          // algorithm for dense matrices
          DenseVectorWritable vector = new DenseVectorWritable();
          vector.setSize(columns);
          for (int i = 0; i < columns; i++)
            if (rand.nextDouble() < sparsity) {
              peer.getCounter(TotalCounter.TOTAL).increment(1L);
              double value = rand.nextDouble();
              vector.addCell(i, value);
            }
          row = vector;
        }
        /*
         * IMPORTANT: Maybe some optimization can be performed here in case of
         * very sparse matrices with empty rows. But I am confused: how to store
         * number of non-zero rows with saving partitioning by rows in {@link
         * SpMV}.
         */
        // if (row.getSize() > 0)
        peer.write(new IntWritable(rowIndex), row);
      }
    }
  }

  /**
   * Method which actually starts generator.
   */
  private static void startTask(HamaConfiguration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    BSPJob bsp = new BSPJob(conf, RandomMatrixGenerator.class);
    bsp.setJobName("Random Matrix Generator");
    /*
     * Generator doesn't reads input. the output will be presented as matrix
     * rows with row index key. TextOutputFormat is for readability it will be
     * replaces by SequenceFileOutputFormat.
     */
    bsp.setBspClass(MyGenerator.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    if (conf.getFloat(sparsityString, 0) < 0.5)
      bsp.setOutputValueClass(SparseVectorWritable.class);
    else
      bsp.setOutputValueClass(DenseVectorWritable.class);
    FileOutputFormat.setOutputPath(bsp, new Path(conf.get(outputString)));

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
          + " seconds. Output is in " + conf.get(outputString));
    }

  }

  public static void printUsage() {
    System.out.println("Usage: rmgenerator <output path> "
        + "[rows (default 10)] [columns (default 10)] "
        + "[sparsity (default 0.1)] [tasks (default max)]");
  }

  /**
   * Function parses command line in standart format. See {@link #printUsage()}
   * for more details
   **/
  public static void parseArgs(HamaConfiguration conf, String[] args) {
    if (args.length < 1) {
      printUsage();
      System.exit(-1);
    }

    conf.set(outputString, args[0]);

    if (args.length == 5) {
      try {
        int requestedBspTasksCount = Integer.parseInt(args[4]);
        if (requestedBspTasksCount < 0)
          throw new IllegalArgumentException(
              "The number of requested bsp tasks can't be negative. Actual value: "
                  + String.valueOf(requestedBspTasksCount));
        conf.setInt(requestedBspTasksString, requestedBspTasksCount);
      } catch (NumberFormatException e) {
        printUsage();
        throw new IllegalArgumentException(
            "The format of requested bsp tasks is int. Can not parse value: "
                + args[4]);
      }
    }

    if (args.length >= 4) {
      try {
        float sparsity = Float.parseFloat(args[3]);
        if (sparsity < 0.0 || sparsity > 1.0) {
          printUsage();
          throw new IllegalArgumentException(
              "Sparsity must be between 0.0 and 1.0. Actual value: "
                  + String.valueOf(sparsity));
        }
        conf.setFloat(sparsityString, sparsity);
      } catch (NumberFormatException e) {
        printUsage();
        throw new IllegalArgumentException(
            "The format of sparsity is float. Can not parse value: " + args[3]);
      }
    }

    if (args.length >= 3) {
      try {
        int columns = Integer.parseInt(args[2]);
        if (columns < 0) {
          printUsage();
          throw new IllegalArgumentException(
              "The number of matrix columns can't be negative. Actual value: "
                  + String.valueOf(columns));
        }
        conf.setInt(columnsString, columns);
      } catch (NumberFormatException e) {
        printUsage();
        throw new IllegalArgumentException(
            "The format of matrix columns is int. Can not parse value: "
                + args[2]);
      }
    }

    if (args.length >= 2) {
      try {
        int rows = Integer.parseInt(args[1]);
        if (rows < 0) {
          printUsage();
          throw new IllegalArgumentException(
              "The number of matrix rows can't be negative. Actual value: "
                  + String.valueOf(rows));
        }
        conf.setInt(rowsString, rows);
      } catch (NumberFormatException e) {
        printUsage();
        throw new IllegalArgumentException(
            "The format of matrix rows is int. Can not parse value: " + args[1]);
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