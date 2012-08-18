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

import org.apache.hama.examples.ExampleDriver;
import org.apache.hama.examples.RandomMatrixGenerator;
import org.junit.Test;

/**
 * This is class for test cases for RandomMatrixGenerator. It will contain tests
 * for different sizes of matrices and different sparsities, command line
 * parsing. In most examples you should specify input and output paths.
 */
public class RandomMatrixGeneratorTest {

  /**
   * Simple test for running from ExampleDriver. You should specify paths.
   */
  @Test
  public void runFromDriver() {
    try {
      String outputPath = "";
      if (outputPath.isEmpty()) {
        System.out
            .println("Please setup input path for vector and matrix and output path for result, "
                + "if you want to run this example");
        return;
      }
      ExampleDriver.main(new String[] { "rmgenerator", outputPath, "1", "4",
          "0.9", "3" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }

  /**
   * Test for incorrect command-line arguments. NOTE: should be commented.
   */
  // @Test
  public void testRandomMatrixGeneratorEmptyArgs() {
    try {
      RandomMatrixGenerator.main(new String[0]);
    } catch (Exception e) {
      // everything ok
    }
  }

  /**
   * Test for incorrect command-line arguments.
   */
  @Test
  public void testRandomMatrixGeneratorIncorrectArgs() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-1", "200",
          "bar", "0.1" });
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      // everything ok
    }
  }

  /**
   * Test for incorrect command-line arguments.
   */
  @Test
  public void testRandomMatrixGeneratorIncorrectArgs1() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-1", "200",
          "-200" });
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      // everything ok
    }
  }

  /**
   * Test for incorrect command-line arguments.
   */
  @Test
  public void testRandomMatrixGeneratorIncorrectArgs2() {
    try {
      RandomMatrixGenerator.main(new String[] { "-c=200", "-r=200", "-s=#" });
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      // everything ok
    }
  }

  /**
   * Test for small 4x4 sparse matrix. You should specify paths.
   */
  @Test
  public void testRandomMatrixGeneratorSmallSparse() {
    try {
      String outputPath = "";
      if (outputPath.isEmpty()) {
        System.out
            .println("Please setup input path for vector and matrix and output path for result, "
                + "if you want to run this example");
        return;
      }
      RandomMatrixGenerator.main(new String[] { outputPath, "4", "4", "0.1",
          "2" });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getLocalizedMessage());
    }
  }

  /**
   * Test for big 10000x10000 sparse matrix. You should specify paths.
   */
  @Test
  public void testRandomMatrixGeneratorLargeSparse() {
    try {
      String outputPath = "";
      if (outputPath.isEmpty()) {
        System.out
            .println("Please setup input path for vector and matrix and output path for result, "
                + "if you want to run this example");
        return;
      }
      RandomMatrixGenerator.main(new String[] { outputPath, "10000", "10000",
          "0.1", "4" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }

  /**
   * Test for small 4x4 sparse matrix. You should specify paths.
   */
  @Test
  public void testRandomMatrixGeneratorSmallDense() {
    try {
      String outputPath = "";
      if (outputPath.isEmpty()) {
        System.out
            .println("Please setup input path for vector and matrix and output path for result, "
                + "if you want to run this example");
        return;
      }
      RandomMatrixGenerator.main(new String[] { outputPath, "4", "4", "0.8",
          "4" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }

  /**
   * Test for big 200x200 dense matrix. You should specify paths.
   */
  @Test
  public void testRandomMatrixGeneratorLargeDense() {
    try {
      String outputPath = "";
      if (outputPath.isEmpty()) {
        System.out
            .println("Please setup input path for vector and matrix and output path for result, "
                + "if you want to run this example");
        return;
      }
      RandomMatrixGenerator.main(new String[] { outputPath, "200", "200",
          "0.8", "4" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
}
