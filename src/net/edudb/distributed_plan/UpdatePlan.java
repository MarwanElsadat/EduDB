/*
EduDB is made available under the OSI-approved MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package net.edudb.distributed_plan;

import net.edudb.condition.Condition;
import net.edudb.condition.NullCondition;
import net.edudb.data_type.DataType;
import net.edudb.data_type.DataTypeFactory;
import net.edudb.distributed_operator.UpdateOperator;
import net.edudb.distributed_operator.parameter.UpdateOperatorParameter;
import net.edudb.distributed_query.QueryNode;
import net.edudb.distributed_query.QueryTree;
import net.edudb.exception.InvalidTypeValueException;
import net.edudb.master.MasterWriter;
import net.edudb.metadata_buffer.MetadataBuffer;
import net.edudb.response.Response;
import net.edudb.statement.SQLStatement;
import net.edudb.statement.SQLUpdateStatement;
import net.edudb.translator.Translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

/**
 * A plan to update data in worker databases
 *
 * @author Fady Sameh
 */
public class UpdatePlan extends DistributedPlan {

    public QueryTree makePlan(SQLStatement sqlStatement) {

        SQLUpdateStatement statement = (SQLUpdateStatement) sqlStatement;
        String tableName = statement.getTableName();

        Hashtable<String, DataType> table = MetadataBuffer.getInstance().getTables().get(tableName);

        if (table == null) {
            MasterWriter.getInstance().write(new Response("Table '" + tableName + "' does not exist"));
            return null;
        }

        String whereClause = statement.getWhereClause();

        /**
         * if where clause is included we need to make sure that all column names
         * exist and that the data types are correct.
         *
         * moreover, in case of sharded tables, the where clause could mean that we
         * don't have to forward the command to all the workers, but only the ones
         * with minimum and maximum values that could satisfy the clause.
         */

        /**
         * The conditions that we will use to determine which shards should the
         * command be forwarded to
         */
        ArrayList<Condition> distributionConditions = new ArrayList<>();
        if (whereClause != null) {
            Translator translator = new Translator();
            String ra = translator.translate("select * from " + tableName + " " + whereClause);
            distributionConditions = translator.getDistributionCondition(table.get("metadata").toString(),
                    table.get("distribution_column").toString(), ra);
        } else {
            distributionConditions.add(new NullCondition());
        }

        if (distributionConditions == null)
            return null;

        DataTypeFactory dataTypeFactory = new DataTypeFactory();

        String distributionColumn = table.get("distribution_column").toString();

        String metadata = table.get("metadata").toString();

        String[] metadataArray = metadata.split(" ");

        String distributionColumnType = "";

        /**
         * Making sure that all assignments are correct
         */
        HashMap<String, String> metadataMap = new HashMap();
        for (int i = 0; i < metadataArray.length; i += 2) {
            metadataMap.put(metadataArray[i], metadataArray[i+1]);
            if (distributionColumn.equals(metadataArray[i]))
                distributionColumnType = metadataArray[i + 1];
        }

        HashMap<String, String> assignments = statement.getAssignemnts();

        for (String column: assignments.keySet()) {
            String columnType = metadataMap.get(column);
            if (column == null) {
                MasterWriter.getInstance().write(new Response("Column '" + column +"' does not exist"));
                return null;
            }

            try {
                dataTypeFactory.makeType(columnType, assignments.get(column));
            } catch (InvalidTypeValueException e) {
                MasterWriter.getInstance().write(new Response(e.getMessage()));
                return null;
            }
        }

        ArrayList<Hashtable<String, DataType>> shards = new ArrayList<>(); // shards we should forward the command to

        for (Hashtable<String, DataType> shard : MetadataBuffer.getInstance().getShards().values()) {
            if (!shard.get("table").toString().equals(tableName))
                continue;

            if (table.get("distribution_method").toString().equals("sharding")) {
                DataType shardMinValue = null;
                DataType shardMaxValue = null;
                try {
                    shardMinValue = dataTypeFactory.makeType(distributionColumnType,
                            shard.get("min_value").toString());
                    shardMaxValue = dataTypeFactory.makeType(distributionColumnType,
                            shard.get("max_value").toString());
                } catch (InvalidTypeValueException e) {
                    System.out.println(e.getMessage());
                    return null;
                }

                boolean validShard = false;

                for (Condition condition : distributionConditions)
                    if (condition.evaluate(shardMinValue, shardMaxValue)) {
                        validShard = true;
                        break;
                    }

                if (!validShard) continue;
            }

            shards.add(shard);
        }

        UpdateOperator operator = new UpdateOperator();
        UpdateOperatorParameter parameter = new UpdateOperatorParameter(statement, shards);
        operator.setParameter(parameter);

        QueryTree tree = new QueryTree((QueryNode)operator);

        return tree;

    }

}
