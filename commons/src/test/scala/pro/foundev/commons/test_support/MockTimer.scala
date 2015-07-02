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

package pro.foundev.commons.test_support

import pro.foundev.commons.benchmarking.Timer

class MockTimer extends Timer {
  private var duration: Long = 0
  def setDuration(nanos: Long) = {
    duration = nanos
  }
  def profile[R](callback: () => R):R = {
    _lastProfile = duration
    callback()
  }

}
