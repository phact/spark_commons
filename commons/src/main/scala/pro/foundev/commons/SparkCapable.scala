/*
 *
 *   Copyright 2015 Foundational Development
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

package pro.foundev.commons

import org.apache.spark.SparkConf

trait SparkCapable {
  def configureSpark(master: String, appName: String): SparkConf={
    new SparkConf()
      .setMaster("spark://"+ master+":7077")
      .setAppName(appName)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "cfs:///spark_logs/")
  }
}
