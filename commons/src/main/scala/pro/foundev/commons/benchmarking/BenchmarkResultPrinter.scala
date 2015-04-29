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

class BenchmarkResultPrinter(benchmarkRunner: BenchmarkRunner, outputWriter: OutputWriter) {
  def outputReports: Unit = {
    val reports = benchmarkRunner.exec
    val maxLengthForTitles:Int = reports.map(x=>x._1.length).max
    val maxLengthForReports: Int = reports.flatMap(x=>x._2).map(y=>y.name.length+3+y.timeElapsed.toString.length).max
    val strLength: Int = maxLengthForReports.max(maxLengthForTitles) + 3
    reports.foreach((m)=>{
    })
  }

}
