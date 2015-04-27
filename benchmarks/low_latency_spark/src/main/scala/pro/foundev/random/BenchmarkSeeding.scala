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
package pro.foundev.random

import com.github.javafaker.Faker

import scala.util.Random

//FIXME make this aware of runs so all tests get the same parameters
class BenchmarkSeeding(recordCount: Long) extends Serializable{
  private val random = new Random()

  def state(): String = {
    val faker = new Faker()
    faker.options().option(Array("TX", "AL", "MN", "MO", "NE", "IN", "IL", "CA", "MI"))
  }

  val maxId = recordCount - 1

  def randId(): Long = {
    (random.nextDouble * maxId).toLong
  }

  def ids(): Set[Long] = {
    var list: Set[Long] = Set[Long]()
    while (list.size < 100) {
      val id: Long = (random.nextDouble * maxId).toLong
      list += id
    }
    list
  }
}

