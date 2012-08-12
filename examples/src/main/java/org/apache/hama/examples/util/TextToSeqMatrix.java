/**
 * Copyright 2007 The Apache Software Foundation
 *
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
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public class TextToSeqMatrix {

  private static final String outputPathString = "converter.output";
  private static final String inputPathString = "converter.intput";
  private static final String converterTypeString = "converter.type";
  private static final String requestedBspTasksString = "bsp.taskcount";

  private static class TextToSeqBSP extends
      BSP<NullWritable, Writable, IntWritable, Writable, NullWritable> {
    private boolean sparse;

    @Override
    public void setup(
        BSPPeer<NullWritable, Writable, IntWritable, Writable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      String converterType = peer.getConfiguration().get(converterTypeString);
      sparse = (converterType.equals("sparse"));
    }

    @Override
    public void bsp(
        BSPPeer<NullWritable, Writable, IntWritable, Writable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      KeyValuePair<NullWritable, Writable> row = null;
      while ((row = peer.readNext()) != null) {
        // it will be needed in conversion of output to result vector
        String input = row.getValue().toString();
        String[] arr = input.split(" ");
        int rowIndex = Integer.parseInt(arr[0].trim());
        int size = Integer.parseInt(arr[1].trim());
        if (sparse) {
          SparseVectorWritable vector = new SparseVectorWritable();
          vector.setSize(size);
          for (int i = 3; i < arr.length; i += 2) {
            int index = Integer.parseInt(arr[i]);
            double value = Double.parseDouble(arr[i + 1]);
            vector.addCell(index, value);
          }
          peer.write(new IntWritable(rowIndex), vector);
        } else {
          DenseVectorWritable vector = new DenseVectorWritable();
          vector.setSize(size);
          for (int i = 3; i < arr.length; i += 2) {
            int index = Integer.parseInt(arr[i]);
            double value = Double.parseDouble(arr[i + 1]);
            vector.addCell(index, value);
          }
          peer.write(new IntWritable(rowIndex), vector);
        }
      }
    }

  }

  private static void printUsage() {
    System.out
        .println("Usage: matrixtoseq <input matrix dir> <output matrix dir> <dense|sparse> [number of tasks (default max)]");
  }

  private static void parseArgs(HamaConfiguration conf, String[] args) {
    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }

    conf.set(inputPathString, args[0]);
    conf.set(outputPathString, args[1]);
    conf.set(converterTypeString, args[2]);

    if (args.length == 3) {
      try {
        int taskCount = Integer.parseInt(args[2]);
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

  private static void startTask(HamaConfiguration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    BSPJob bsp = new BSPJob(conf, TextToSeqMatrix.class);
    bsp.setJobName("Conversion of matrix from text format to sequence file format.");
    bsp.setBspClass(TextToSeqBSP.class);
    
    String converterType = conf.get(converterTypeString);
    boolean sparse = (converterType.equals("sparse"));
    
    /*
     * Input matrix is presented as pairs of integer and {@ link
     * SparseVectorWritable}. Output is pairs of integer and double
     */
    bsp.setInputFormat(TextInputFormat.class);
    bsp.setInputKeyClass(IntWritable.class);
    bsp.setInputValueClass(SparseVectorWritable.class);
    bsp.setOutputKeyClass(IntWritable.class);
    if (sparse)
      bsp.setOutputValueClass(SparseVectorWritable.class);
    else
      bsp.setOutputValueClass(DenseVectorWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setInputPath(new Path(conf.get(inputPathString, "/dev/null")));
    bsp.setOutputPath(new Path(conf.get(outputPathString, "/dev/null")));

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
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration();
    parseArgs(conf, args);
    startTask(conf);
  }
}
