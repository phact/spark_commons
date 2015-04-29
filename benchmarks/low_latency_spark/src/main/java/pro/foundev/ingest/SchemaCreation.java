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
package pro.foundev.ingest;

import com.datastax.driver.core.Session;

import static pro.foundev.Configuration.getKeyspace;
import static pro.foundev.Configuration.getTable;

public class SchemaCreation {
    private final Session session;

    public SchemaCreation(Session session){
        this.session = session;
    }

    public void createSchema(){
        session.execute("CREATE KEYSPACE IF NOT EXISTS "+ getKeyspace() + " with replication = " +
                "{ 'class': 'SimpleStrategy' , 'replication_factor' : 1 }" );
        session.execute("CREATE TABLE IF NOT EXISTS "  +
                getKeyspace() + "."
                + getTable() +
                " (id bigint, origin_state text, ip_address inet, urls list<text>," +
                " primary key(id))");
        session.execute("CREATE INDEX IF NOT EXISTS ON "  +
                getKeyspace() + "."
                + getTable() +
                " (origin_state)");
    }
}
