/*
EduDB is made available under the OSI-approved MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package net.edudb.distributed_executor;

import net.edudb.data_type.DataType;
import net.edudb.data_type.IntegerType;
import net.edudb.distributed_operator.DistributedOperator;
import net.edudb.distributed_operator.SelectOperator;
import net.edudb.distributed_operator.parameter.SelectOperatorParameter;
import net.edudb.master.MasterWriter;
import net.edudb.response.Response;
import net.edudb.statement.SQLSelectStatement;
import net.edudb.structure.Record;
import net.edudb.worker_manager.WorkerDAO;
import net.edudb.workers_manager.WorkersManager;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * Selects records from all necessary shards and concatenates the results
 *
 * @author Fady Sameh
 */
public class SelectExecutor implements OperatorExecutionChain {

    OperatorExecutionChain next;

    public void setNextElementInChain(OperatorExecutionChain chainElement) { this.next = chainElement; }

    public void execute(DistributedOperator operator) {
        if (operator instanceof SelectOperator) {

            SelectOperator select = (SelectOperator) operator;
            SelectOperatorParameter parameter = (SelectOperatorParameter)select.getParameter();

            SQLSelectStatement statement = parameter.getStatement();
            ArrayList<Hashtable<String, DataType>> shards = parameter.getShards();

            String shardId = shards.get(0).get("id").toString();

            Response[] responses = new Response[shards.size()];

            //added for replication
            ArrayList<Integer> str = new ArrayList<Integer>();
            ArrayList<Hashtable<String, DataType>> checkedShards = new ArrayList<>();
            int orgShards = 0;
            int replicas = 0;
            //responses.length
            for (int i = 0; i < responses.length; i++) {
                //MasterWriter.getInstance().write(new Response("for loop 1st"+i));

                Hashtable<String, DataType> shard = shards.get(i);


                //added for replication
                boolean sameMin = false;
                String cmp1 = "min";
                String cmp2 = "min";

                if (checkedShards.size() > 0) {
                    for (int j = 0; j < checkedShards.size(); j++) {
                        Hashtable<String, DataType> checkedShard = checkedShards.get(j);
                        MasterWriter.getInstance().write(new Response("min"+shard.get("min_value")));
                        MasterWriter.getInstance().write(new Response("checkedmin"+checkedShard.get("min_value")));

                        cmp1 +=(shard.get("min_value").toString());
                        cmp2 +=(checkedShard.get("min_value").toString());

                        if(cmp1.equals(cmp2) ){
                            sameMin = true;
                            replicas++;
                            MasterWriter.getInstance().write(new Response("replicaaaaaa"+sameMin));
                            break;
                        }else{
                            sameMin = false;
                           // str.add(i);
                            MasterWriter.getInstance().write(new Response("condition"+sameMin));
                        }

                        //sameMin = checkedShard.get("min_value").toString().equals(shard.get("min_value"));
                        //sameMax = checkedShard.get("max_value").toString().equals(shard.get("max_value"));
                    }
                }

                checkedShards.add(shard);
                //end of added part for replication
                    if(!sameMin) {
                        String workerAddress = shard.get("host").toString() + ":" + shard.get("port").toString();
                        WorkerDAO workerDAO = WorkersManager.getInstance().getWorkers().get(workerAddress);

                        if (workerDAO == null) {
                            MasterWriter.getInstance().write(new Response("Worker at '" + workerAddress + "' is not available"));
                            return;
                        }

                        String tableName = parameter.getTableName();
                        int id = ((IntegerType) shard.get("id")).getInteger();
                        String insertStatement = statement.toString();

                        if (id != 0) {
                            insertStatement = insertStatement.replace(tableName, tableName + id);
                        }

                        final int index = orgShards;
                        final String finalDeleteStatement = insertStatement;

                        new Thread(() -> responses[index] = workerDAO.insert(finalDeleteStatement)).start();

                        //added for shard replication
                        MasterWriter.getInstance().write(new Response("got thread" + i));
                        orgShards++;
                        str.add(i);

                    }
            }

            int index = 0;
            int responsesReceived = 0;
//edit responses.length

            while (responsesReceived != (responses.length - replicas)) {

                if(str.contains(index)) {

                    if (responses[index] == null) {
                        responsesReceived = 0;
                        MasterWriter.getInstance().write(new Response("reset responses"));

                    } else
                        ++responsesReceived;
//edit responses.length
                    index = (index + 1) % (responses.length);
                    MasterWriter.getInstance().write(new Response("index:" + index));
                }
            }




            /**
             *
             */
            if (shardId.equals("0")) {
                MasterWriter.getInstance().write(new Response("relation", responses[0].getRecords(), null));
            }
            else {
                ArrayList<Record> concatenatedResult = new ArrayList<>();

                for (int i = 0; i < responses.length; i++) {
                    if(str.contains(i)){
                        MasterWriter.getInstance().write(new Response("pre-passed"));
                        concatenatedResult.addAll(responses[i].getRecords());
                        //MasterWriter.getInstance().write(new Response("passed"));

                    }
                }

                MasterWriter.getInstance().write(new Response("relation", concatenatedResult, null));

            }
        }
        else {
            next.execute(operator);
        }
    }
}
