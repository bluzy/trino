/*
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
package io.trino.plugin.iceberg;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergParquetQuery extends AbstractTestQueryFramework {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return createIcebergQueryRunner();
    }

    @Test
    public void testDuplicatedFieldName() {
        try {
            assertUpdate("CREATE TABLE duplicated_field_name(id varchar, nested1 row(id varchar)) WITH (format = 'PARQUET')");
            assertUpdate("INSERT INTO duplicated_field_name VALUES ('1', ROW('2'))", 1);
            assertUpdate("INSERT INTO duplicated_field_name VALUES ('2', ROW('3'))", 1);
            assertQuery("SELECT id FROM duplicated_field_name WHERE id = '1' AND nested1.id = '2'", "VALUES 1");
        } finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS duplicated_field_name");
        }

    }

}
