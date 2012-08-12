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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.SpMV;
import org.apache.hama.examples.util.DenseVectorWritable;
import org.apache.hama.examples.util.SparseVectorWritable;
import org.apache.hama.examples.util.WritableUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * This class is test cases for {@link SpMV}. It will contain simple hand
 * calculated cases, and cases of different matrix and vector sizes given with
 * help of {@link RandomMatrixGenerator}
 */
public class SpMVTest {

  private HamaConfiguration conf;
  private FileSystem fs;
  private String baseDir;

  @Before
  public void prepare() throws IOException {
    conf = new HamaConfiguration();
    fs = FileSystem.get(conf);
    baseDir = fs.getHomeDirectory().toString() + "/spmv";
  }

  //@Test
  public void runFromDriver() {
    try {
      ExampleDriver.main(new String[] { "spmv", "inputMatrix", "inputVector",
          "outputVector", "4" });
    } catch (Exception e) {
      // everything ok, because here we check only parsing of command-line
      // arguments
    }
  }

  /**
   * Simple test. multiplying 
   * [1 0 6 0]   [2]   [38] 
   * [0 4 0 0] * [3] = [12] 
   * [0 2 3 0]   [6]   [24] 
   * [3 0 0 5]   [1]   [11]
   */
  //@Test
  public void simpleSpMVTest() {
    try {
      HamaConfiguration conf = new HamaConfiguration();
      WritableUtil writableUtil = new WritableUtil();
      String testDir = "/simple/";
      int size = 4;

      // creating test matrix
      HashMap<Integer, Writable> inputMatrix = new HashMap<Integer, Writable>();
      SparseVectorWritable vector0 = new SparseVectorWritable();
      vector0.setSize(size);
      vector0.addCell(0, 1);
      vector0.addCell(2, 6);
      SparseVectorWritable vector1 = new SparseVectorWritable();
      vector1.setSize(size);
      vector1.addCell(1, 4);
      SparseVectorWritable vector2 = new SparseVectorWritable();
      vector2.setSize(size);
      vector2.addCell(1, 2);
      vector2.addCell(2, 3);
      SparseVectorWritable vector3 = new SparseVectorWritable();
      vector3.setSize(size);
      vector3.addCell(0, 3);
      vector3.addCell(3, 5);
      inputMatrix.put(0, vector0);
      inputMatrix.put(1, vector1);
      inputMatrix.put(2, vector2);
      inputMatrix.put(3, vector3);
      String matrixPath = baseDir + testDir + "inputMatrix";
      writableUtil.writeMatrix(matrixPath, conf, inputMatrix);

      HashMap<Integer, Writable> inputVector = new HashMap<Integer, Writable>();
      DenseVectorWritable vector = new DenseVectorWritable();
      vector.setSize(size);
      vector.addCell(0, 2);
      vector.addCell(1, 3);
      vector.addCell(2, 6);
      vector.addCell(3, 1);
      inputVector.put(0, vector);
      String vectorPath = baseDir + testDir + "inputVector";
      writableUtil.writeMatrix(vectorPath, conf, inputVector);

      String outputPath = baseDir + testDir;
      SpMV.main(new String[]{matrixPath, vectorPath, outputPath, "4"});

      String resultPath = SpMV.getResultPath();
      DenseVectorWritable result = new DenseVectorWritable();
      writableUtil.readFromFile(resultPath, result, conf);

      double expected[] = { 38, 12, 24, 11 };
      if (result.getSize() != size)
        throw new Exception("Incorrect size of output vector");
      for (int i = 0; i < result.getSize(); i++)
        if ((result.get(i) - expected[i]) < 0.01)
          expected[i] = 0;

      for (int i = 0; i < expected.length; i++)
        if (expected[i] != 0)
          throw new Exception("Result doesn't meets expectations");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void simpleSpMVTestFile() {
    try {
      WritableUtil writableUtil = new WritableUtil();
      int size = 4;
      String matrixPath = "/home/mikalaj/spmv/simple/input-matrix-seq";
      String vectorPath = "/home/mikalaj/spmv/simple/input-vector-seq";
      String outputPath = "/home/mikalaj/spmv/simple/results";
      SpMV.main(new String[]{matrixPath, vectorPath, outputPath, "4"});

      String resultPath = SpMV.getResultPath();
      DenseVectorWritable result = new DenseVectorWritable();
      writableUtil.readFromFile(resultPath, result, conf);

      double expected[] = { 38, 12, 24, 11 };
      if (result.getSize() != size)
        throw new Exception("Incorrect size of output vector");
      for (int i = 0; i < result.getSize(); i++)
        if ((result.get(i) - expected[i]) < 0.01)
          expected[i] = 0;

      for (int i = 0; i < expected.length; i++)
        if (expected[i] != 0)
          throw new Exception("Result doesn't meets expectations");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getLocalizedMessage());
    }
  }
}
