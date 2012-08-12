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

public class RandomMatrixGeneratorTest {

  //@Test
  public void runFromDriver() {
    try {
      ExampleDriver.main(new String[] { "rmgenerator", "/tmp/matrix-gen-1",
          "4", "4", "0.1", "4" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  // @Test
  public void testRandomMatrixGeneratorEmptyArgs() {
    try {
      RandomMatrixGenerator.main(new String[0]);
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }

  // @Test
  public void testRandomMatrixGeneratorIncorrectArgs() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-1", "200",
          "bar", "0.1" });
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      // everything ok
    }
  }

  // @Test
  public void testRandomMatrixGeneratorIncorrectArgs1() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-1", "200",
          "-200" });
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      // everything ok
    }
  }

  // @Test
  public void testRandomMatrixGeneratorIncorrectArgs2() {
    try {
      RandomMatrixGenerator.main(new String[] { "-c=200", "-r=200", "-s=#" });
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      // everything ok
    }
  }

  @Test
  public void testRandomMatrixGeneratorSmallSparse() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-1", "4", "4",
          "0.1", "2" });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getLocalizedMessage());
    }
  }

  // @Test
  public void testRandomMatrixGeneratorLargeSparse() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-2", "10000",
          "10000", "0.1", "4" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }

  // @Test
  public void testRandomMatrixGeneratorSmallDense() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-1", "4", "4",
          "0.8", "4" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }

  // @Test
  public void testRandomMatrixGeneratorLargeDense() {
    try {
      RandomMatrixGenerator.main(new String[] { "/tmp/matrix-gen-1", "200",
          "200", "0.8", "4" });
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
}
