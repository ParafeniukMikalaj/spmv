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
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public class SeqToTextMatrix {

  private static class SeqToTextBSP
      extends
      BSP<IntWritable, Writable, IntWritable, Writable, NullWritable> {

    @Override
    public void bsp(
        BSPPeer<IntWritable, Writable, IntWritable, Writable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      KeyValuePair<IntWritable, Writable> row = null;
      while ((row = peer.readNext()) != null) {
        // it will be needed in conversion of output to result vector
        int key = row.getKey().get();
        Writable mRow = row.getValue();
        peer.write(new IntWritable(key), mRow);
      }
    }

  }

  private static void printUsage() {
    System.out
        .println("Usage: SeqToTextMatrix <input matrix dir> <output matrix dir>");
    System.exit(-1);
  }

  private static void parseArgs(HamaConfiguration conf, String[] args) {
    if (args.length < 2)
      printUsage();

    conf.set("converter.input", args[0]);
    conf.set("converter.output", args[1]);

    if (args.length == 3) {
      try {
        int taskCount = Integer.parseInt(args[2]);
        if (taskCount < 0)
          throw new IllegalArgumentException(
              "The number of requested tasks can't be negative. Actual value: "
                  + String.valueOf(taskCount));
        conf.setInt("converter.taskcount", taskCount);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "The format of requested task count is int. Can not parse value: "
                + args[3]);
      }
    }
  }

  private static void startTask(HamaConfiguration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    BSPJob bsp = new BSPJob(conf, SeqToTextMatrix.class);
    bsp.setJobName("Conversion of matrix from sequence file format to text format.");
    bsp.setBspClass(SeqToTextBSP.class);
    /*
     * Input matrix is presented as pairs of integer and {@ link
     * SparseVectorWritable}. Output is pairs of integer and double
     */
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setInputKeyClass(IntWritable.class);
    bsp.setInputValueClass(SparseVectorWritable.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(SparseVectorWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    bsp.setInputPath(new Path(conf.get("converter.input", "/dev/null")));
    bsp.setOutputPath(new Path(conf.get("converter.output", "/dev/null")));

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    int requestedTasks = conf.getInt("converter.taskcount", -1);
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
