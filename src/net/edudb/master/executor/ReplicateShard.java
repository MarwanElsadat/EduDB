package net.edudb.master.executor;

import net.edudb.data_type.DataType;
import net.edudb.data_type.DataTypeFactory;
import net.edudb.data_type.IntegerType;
import net.edudb.data_type.VarCharType;
import net.edudb.engine.Utility;
import net.edudb.master.MasterWriter;
import net.edudb.meta_manager.MetaDAO;
import net.edudb.meta_manager.MetaManager;
import net.edudb.metadata_buffer.MetadataBuffer;
import net.edudb.response.Response;
import net.edudb.structure.Record;
import net.edudb.worker_manager.WorkerDAO;
import net.edudb.workers_manager.WorkersManager;

import java.util.Hashtable;
import java.util.regex.Matcher;

/**
        * Replicates shards
        *
        * @author Marwan karim
        */

public class ReplicateShard implements MasterExecutorChain{

    private MasterExecutorChain nextElement;

    //                                replicate            shard     (table, srcServer, srcPort,   id, dstServer, dstPort)
    private String cregex = "\\A(?:(?i)create)\\s+(?:(?i)cshard)\\s+\\((\\w+), (\\w+), (\\w+), (\\w+), (\\w+), (\\w+)\\)\\s*;?\\z";


    @Override
    public void setNextElementInChain(MasterExecutorChain chainElement) {
        this.nextElement = chainElement;
    }

    @Override
    public void execute(String string) {


            Matcher matcher = Utility.getMatcher(string, cregex);
            if (matcher.matches()) {
                String tableName = matcher.group(1);
                String workerHost = matcher.group(2);
                int workerPort = Integer.parseInt(matcher.group(3));
                int id = Integer.parseInt(matcher.group(4));
                String dstWorkerHost = matcher.group(5);
                int dstWorkerPort = Integer.parseInt(matcher.group(6));

                String sWorkerPort = matcher.group(3);

                //test inputs
                //MasterWriter.getInstance().write(new Response(tableName + workerHost + workerPort + id + dstWorkerHost + dstWorkerPort));

                // check if table exists
                Hashtable<String, DataType> table = MetadataBuffer.getInstance().getTables().get(tableName);

                if (table == null) {
                    MasterWriter.getInstance().write(new Response("Table '" + tableName + "' does not exist"));
                    return;
                }
                /** checking the src and destination port are registered and online */

                // check if source worker is registered on the port
                Record srcWorker = MetadataBuffer.getInstance().getWorkers().get(workerHost + ":" + workerPort);

                if (srcWorker == null) {
                    MasterWriter.getInstance().write(new Response(
                            "No worker registered at " + workerHost + ":" + workerPort
                    ));
                    return;
                }
                //check source worker is online
                WorkerDAO workerDAO = WorkersManager.getInstance().getWorkers().get(workerHost + ":" + workerPort);

                if (workerDAO == null) {
                    MasterWriter.getInstance().write(new Response("Worker at " + workerHost + ":" + workerPort + " is not currently online."));
                    return;
                }

                // check if destination worker is registered on the port
                Record dstWorker = MetadataBuffer.getInstance().getWorkers().get(workerHost + ":" + workerPort);

                if (dstWorker == null) {
                    MasterWriter.getInstance().write(new Response(
                            "No worker registered at " + dstWorkerHost + ":" + dstWorkerPort
                    ));
                    return;
                }
                //check destination worker is online
                WorkerDAO dstWorkerDAO = WorkersManager.getInstance().getWorkers().get(workerHost + ":" + workerPort);

                if (dstWorkerDAO == null) {
                    MasterWriter.getInstance().write(new Response("Worker at " + dstWorkerHost + ":" + dstWorkerPort + " is not currently online."));
                    return;
                }


                //check distribution method
                String distributionMethod = ((VarCharType) table.get("distribution_method")).getString();
                if (!distributionMethod.equals("sharding")) {
                    MasterWriter.getInstance().write(new Response("Distribution method for table '" + tableName
                            + "' is not set to sharding"));
                    return;
                }

                /** checking the original shard is at the source worker and has the same id */

                //check shard is at the src id and Address
                boolean addressCheck = false;
                boolean idCheck = false;
                boolean replicaExists = false;

                //get shard min and max
                String shardMin = null;
                String shardMax = null;
                String distributionColumn = ((VarCharType) table.get("distribution_column")).getString();
                MetaDAO metaDAO = MetaManager.getInstance();

                DataTypeFactory dataTypeFactory = new DataTypeFactory();


                for (Hashtable<String, DataType> shard : MetadataBuffer.getInstance().getShards().values()) {

                    if (shard.get("table_name").toString().equals(tableName)) {

                        //String address = shard.get("host").toString() + ":" + shard.get("port").toString();

                        String addressHost = shard.get("host").toString();
                        String addressPort = shard.get("port").toString();
                        String cID = shard.get("id").toString();


/*
                        if (addressHost == workerHost && addressPort == sWorkerPort) {
                            addressCheck = true;
                        }
*/

                        if (id == Integer.parseInt(cID)) {

                            idCheck = true;
                            //get min and max values
                            shardMin = shard.get("min_value").toString();
                            shardMax = shard.get("max_value").toString();

                            String s = addressHost+addressPort;
                            String u = workerHost + sWorkerPort;
                            if(s.equals(u)){
                                addressCheck = true;
    //                            MasterWriter.getInstance().write(new Response("address is correct"));
                            }

                            if(s.equals(dstWorkerHost+dstWorkerPort)){
                                replicaExists = true;
                            }

                        }





                    }
                }

                if (!idCheck) {
                    MasterWriter.getInstance().write(new Response(
                            "shard id doesn't exist at the table"
                    ));
                    return;
                }
                if (!addressCheck) {
                    MasterWriter.getInstance().write(new Response(
                            "shard doesn't exist at" + workerHost + ":" + workerPort
                    ));
                    return;
                }
                if (replicaExists) {
                    MasterWriter.getInstance().write(new Response(
                            "shard replica already exists at" + dstWorkerHost +":"+dstWorkerPort
                    ));
                    return;
                }

                // we store the shard as a new shard identical to normal shards
                int shardId = ((IntegerType)table.get("shard_number")).getInteger() + 1;

                metaDAO.writeShard(dstWorkerHost, dstWorkerPort, tableName, shardId, shardMin, shardMax);
                metaDAO.editTable(tableName, null, null, shardId);
                workerDAO.createTable(tableName + shardId, table.get("metadata").toString());



            }

    }
}
