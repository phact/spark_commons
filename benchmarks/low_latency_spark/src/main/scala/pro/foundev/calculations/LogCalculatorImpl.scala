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
package pro.foundev.calculations
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import pro.foundev.dto.{IpLog, SessionReport}

/**
 *
 */
class LogCalculatorImpl extends LogCalculator{

  /**
   * returns state with most clicks and the average clicks per user
   * @param table
   * @return
   */
  override def sessionReport(table: RDD[IpLog]):SessionReport  = {
    val biggestState: String = table.
      map(log => (log.originState, log.urls.size))
      .reduceByKey((t1, t2) => t1 + t2)
      .reduce((t1, t2) => {
      if (t1._2 > t2._2) {
        t1
      } else {
        t2
      }
    })._1
    val averageClicksBySession: Double = table.map(log => log.urls.size).mean()
    new SessionReport(biggestState, averageClicksBySession)
  }
}
