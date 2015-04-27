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
package pro.foundev.java.Transformations;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.api.java.function.Function;
import play.libs.Json;
import scala.Tuple2;

import java.util.HashMap;

public class MapColumns implements Function<String[], Tuple2<Integer, JsonNode>> {

    @Override
    public Tuple2<Integer, JsonNode> call(String[] columnArray) throws Exception {
        Integer id = Integer.parseInt(columnArray[0]);
        JsonNode firstName = Json.toJson(columnArray[1]);
        JsonNode lastName  = Json.toJson(columnArray[2]);
        HashMap userAttributes = new HashMap<>();
        userAttributes.put("firstName", firstName);
        userAttributes.put("lastName", lastName);
        JsonNode body = Json.toJson(userAttributes);
        return new Tuple2<>(id, body);
    }
}
