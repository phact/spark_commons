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
package pro.foundev.reporting

import scala.collection.mutable

class BenchmarkReport(titleOfStrategy: String) {
  var list:mutable.MutableList[(String, Double, Double, Double, Double, Double)]
  = new mutable.MutableList[(String, Double, Double, Double, Double, Double)]()
  def generateFinalReport: String = {
    if(list.size==0) {
      "There are no report items to display for '"+titleOfStrategy+"'"
    }else{
      val builder = new mutable.StringBuilder()
      val sep = System.lineSeparator()
      builder.append("Benchmarks for strategy '"+ titleOfStrategy + "'" + sep)
      list.foreach(l=> {
        builder.append("Report from " + l._1 +sep +
          "------------------------" + sep +
          "Average:         " + l._6 + sep +
          "Min:             " + l._2 + sep +
          "50th Percentile: " + l._4 + sep +
          "90th Percentile: " + l._5 + sep +
          "Max:             " + l._3 + sep +
        "------------------------" + sep
        )
      })
      builder.toString()
    }
  }

  def addLine(title: String, min: Double,
              max: Double,
              percentile50th: Double,
              percentile90th: Double,
              average: Double): Unit = {
    list.+=:((title, min, max, percentile50th, percentile90th, average))
  }
}
