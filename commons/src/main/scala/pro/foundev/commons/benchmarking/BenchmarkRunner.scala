/*
 *  Copyright 2015 Foundational Development
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package pro.foundev.commons.benchmarking

import org.apache.commons.lang.time.StopWatch

class BenchmarkRunner (benchmarks: Benchmark*){
  def exec: Map[String, Seq[BenchmarkReport]] = {
    if (benchmarks.length==0) {
      throw new IllegalArgumentException("need to have some benchmarks to actually run")
    }
    benchmarks.map(b => {
      val watch = new StopWatch()
      watch.start()
      b.callback.apply()
      watch.stop()
      val milliseconds = watch.getTime
      val seconds: Double = milliseconds / 1000.0
      new Tuple2[String, BenchmarkReport](b.tag, new BenchmarkReport(seconds, b.name))
    }).groupBy(_._1)
      .map { case (k, v) => (k, v.map(_._2)) }
  }

}
