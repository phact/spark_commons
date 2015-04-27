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
package pro.foundev

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfter, FunSpec}
import pro.foundev.reporting.RunTimeOptions

class BenchmarkLauncherSpec  extends FunSpec with BeforeAndAfterEach  {
 describe("parsing options") {
   val launcher = new BenchmarkLauncher()
   describe("no args") {
     val parsedOptions = launcher.runTimeOptions(Array[String]())
     it("sets host to 127.0.0.1") {
       assert("127.0.0.1" == parsedOptions.host)
     }
     it("sets runs to 100") {
       assert(100 == parsedOptions.runs)
     }
     it("sets numrecords to 10000") {
       assert(10000 == parsedOptions.recordsToIngest)
     }
     it("does not run loader") {
       assert(false == parsedOptions.enableLauncher)
     }
   }
   describe("1 args") {
     var parsedOptions: RunTimeOptions = null
     val host = "8.8.8.8"
     parsedOptions = launcher.runTimeOptions(Array[String](host))
     it("sets host to passed in value") {
       assert(host == parsedOptions.host)
     }
     it("sets runs to 100") {
       assert(100 == parsedOptions.runs)
     }
     it("sets numrecords to 10000") {
       assert(10000 == parsedOptions.recordsToIngest)
     }
     it("does not run loader") {
       assert(false == parsedOptions.enableLauncher)
     }
   }
   describe("2 args") {
     val host = "8.8.8.8"
     val runs = "9000"
     val parsedOptions = launcher.runTimeOptions(Array[String](host, runs))
     it("sets host to passed in value") {
       assert(host == parsedOptions.host)
     }
     it("sets runs to 9000") {
       assert(runs.toInt == parsedOptions.runs)
     }
     it("sets numrecords to 10000") {
       assert(10000 == parsedOptions.recordsToIngest)
     }
     it("does not run loader") {
       assert(false == parsedOptions.enableLauncher)
     }
   }
    describe("3 args") {
     val host = "8.8.8.8"
     val runs = "9000"
      val recors = "100"
     val parsedOptions = launcher.runTimeOptions(Array[String](host, runs,recors))
     it("sets host to passed in value") {
       assert(host == parsedOptions.host)
     }
     it("sets runs to 9000") {
       assert(runs.toInt == parsedOptions.runs)
     }
     it("sets numrecords to 100") {
       assert(recors.toInt == parsedOptions.recordsToIngest)
     }
     it("does not run loader") {
       assert(false == parsedOptions.enableLauncher)
     }
   }
   describe("4 args") {
     val host = "8.8.8.8"
     val runs = "9000"
      val records = "100"
     val enabled= "enableLoader"
     val parsedOptions = launcher.runTimeOptions(Array[String](host, runs,records,enabled ))
     it("sets host to passed in value") {
       assert(host == parsedOptions.host)
     }
     it("sets runs to 9000") {
       assert(runs.toInt == parsedOptions.runs)
     }
     it("sets numrecords to 100") {
       assert(records.toInt == parsedOptions.recordsToIngest)
     }
     it("does not run loader") {
       assert(parsedOptions.enableLauncher)
     }
   }
 }
}
