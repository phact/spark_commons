/*
 * Copyright 2015 Foundational Development
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pro.foundev.commons.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

class StreamingObjectMother {
  /**
   * TODO: make single global streaming run for all testing. will need to play with cleanup
   * @return
   */
  def createStream():StreamingContext = {
    val sparkConf = new SparkConf()
      .setAppName("streaming_tester")
    .setMaster("local[*]")
      //.setMaster("spark:///localhost:7077")
     // .set("spark.cassandra.connection.host", "127.0.0.1")
    new StreamingContext(sparkConf, Duration(500))
  }
}
