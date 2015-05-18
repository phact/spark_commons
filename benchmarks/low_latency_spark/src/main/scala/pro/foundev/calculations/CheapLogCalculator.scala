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
import pro.foundev.dto.IpLog

/**
 * simple reduction of total URL count in all sessions
 */
class CheapLogCalculator extends Serializable {
  /**
   * filters out emptyRDDs and
   * @param table
   * @return
   */
  def totalUrls(table: RDD[IpLog]): Long = {
    table.cache()
    //guard against empty RDD FIXME breaks pipelining. Consider documented invariant instead
    if(table.count()>0 ) {
      val totalValidUrls = table
        .filter(r => !r.urls.isEmpty)
      //guard against useless urls FIXME breaks pipeline. Consider documented invariant instead
      if(totalValidUrls.count()>0) {
        //totals url count in all rows
        totalValidUrls
          .map(f => f.urls.size)
          .reduce(_ + _)
      }else{
        0
      }
    }else{
      0
    }
  }
}
