package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.locationtech.jts.index.ArrayListVisitor;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;

import manager.StorageManager;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {
    
        private final List<RexNode> projects;
        private final RelDataType rowType;
        private final RelOptTable table;
        private final RexNode filter;
    
        public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
            super(cluster, traitSet, table);
            this.table = table;
            this.rowType = deriveRowType();
            this.filter = filter;
            this.projects = projects;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PIndexScan(getCluster(), traitSet, table, filter, projects);
        }
    
        @Override
        public RelOptTable getTable() {
            return table;
        }

        @Override
        public String toString() {
            return "PIndexScan";
        }

        public String getTableName() {
            return table.getQualifiedName().get(1);
        }

        @Override
        public List<Object[]> evaluate(StorageManager storage_manager) {
            String tableName = getTableName();
            System.out.println("Evaluating PIndexScan for table: " + tableName);

            /* Write your code here */


            // Extract the column name from this.table
            String [] columnNames = this.table.getRowType().getFieldNames().toArray(new String[0]);

            // Extract the filter condition from this.filter
            String typeOfFilter = this.filter.getKind().toString();
            RexCall rexCall = (RexCall) this.filter;     
            String stringColumnIndex = rexCall.getOperands().get(0).toString();
            stringColumnIndex = stringColumnIndex.substring(1);
            int columnIndex = Integer.parseInt(stringColumnIndex);
            // int value = Integer.parseInt(rexCall.getOperands().get(1).toString());
            RexLiteral value = (RexLiteral) rexCall.getOperands().get(1);
            String targetColumn = columnNames[columnIndex];
            System.out.println("Column index: " + columnIndex);
            System.out.println("Value: " + value);
            System.out.println("Target column: " + targetColumn);

            // now i have the column list and the column index and the kind of filter

            List<Object[]> allRecords = new ArrayList<>();
            int currId = 1;

            while(storage_manager.get_records_from_block(tableName, currId)!=null)
            {
                List<Object[]> records = storage_manager.get_records_from_block(tableName, currId);
                for(Object[] record: records)
                {
                    allRecords.add(record);
                }
                currId++;
            }

            HashMap<String,List<String>> table_column_type = storage_manager.table_column_type;
            int indexInTable = -1;
            for(int i=0;i<table_column_type.get(tableName).size();i++)
            {
                if(table_column_type.get(tableName).get(i).equals(targetColumn))
                {
                    indexInTable = i;
                    break;
                }
            }

            HashMap<String,List<RelDataType>> typeList = storage_manager.file_to_typelist;

            RelDataType type = typeList.get(tableName).get(indexInTable);

            System.out.println(type);
            List<Object[]> result = new ArrayList<>();

            String indexFileName = tableName + "_" + targetColumn + "_index";
            int blockId = storage_manager.search(tableName, targetColumn, value);
            if(blockId == -1)
                return result;


            byte [] data = storage_manager.get_data_block(indexFileName, blockId);

            System.out.println("Block id is "+ blockId);

            if(typeOfFilter.equals("EQUALS"))
            {
                if(type.getSqlTypeName().toString().equals("INTEGER"))
                {
                    int val = Integer.parseInt(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            key[0] = data[offset];
                            key[1] = data[offset+1];
                            key[2] = data[offset+2];
                            key[3] = data[offset+3];
                            offset+=4;
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt<val)
                                continue;
                            else if(keyInt>val)
                                blockId = 0;
                            else
                                blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(record[indexInTable].equals(val))
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if(type.getSqlTypeName().toString().equals("VARCHAR"))
                {
                    String nval = value.getValue().toString();
                    int startIndex = nval.indexOf("'") + 1;
                    int endIndex = nval.lastIndexOf("'");
                    String val = nval.substring(startIndex, endIndex);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] lengthOfKeyBytes = new byte[2];
                            lengthOfKeyBytes[0] = data[offset+2];
                            lengthOfKeyBytes[1] = data[offset+3];
                            offset+=4;
                            int lengthOfKey = (lengthOfKeyBytes[0] & 0xff) << 8 | (lengthOfKeyBytes[1] & 0xff);
                            byte [] key = new byte[lengthOfKey];
                            for(int j=0;j<lengthOfKey;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            String keyString = new String(key);
                            if(keyString.compareTo(val)<0)
                                continue;
                            else if(keyString.compareTo(val)>0)
                                blockId = 0;
                            else
                                blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(record[indexInTable].equals(val))
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("DOUBLE"))
                {
                    String temp = value.toString();
                    String [] strings = temp.split(":");
                    String valstring = strings[0];
                    double val = Double.parseDouble(valstring);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        // System.out.println("Block id is "+ blockId);
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        // System.out.println("Num keys is "+ numKeysInt);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[8];
                            for(int j=0;j<8;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            // long keyLong = (key[7] & 0xff) << 56 | (key[6] & 0xff) << 48 | (key[5] & 0xff) << 40 | (key[4] & 0xff) << 32 | (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);

                            long l = ((long) key[7] << 56) |
                                ((long) key[6] & 0xFF) << 48 |
                                ((long) key[5] & 0xFF) << 40 |
                                ((long) key[4] & 0xFF) << 32 |
                                ((long) key[3] & 0xFF) << 24 |
                                ((long) key[2] & 0xFF) << 16 |
                                ((long) key[1] & 0xFF) << 8 |
                                ((long) key[0] & 0xFF);
                            double keyLong = (Double) Double.longBitsToDouble(l);
                            if(keyLong<val)
                                continue;
                            else if(keyLong>val)
                                blockId = 0;
                            else
                                blockIdNeeded.add(currentBlockIdInt);
                        }
                        // System.out.println("Size of blockIdNeeded is "+ blockIdNeeded.size());
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(record[indexInTable].equals(val))
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("FLOAT"))
                {
                    float val = Float.parseFloat(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            for(int j=0;j<4;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt<val)
                                continue;
                            else if(keyInt>val)
                                blockId = 0;
                            else
                                blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(record[indexInTable].equals(val))
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if(type.getSqlTypeName().toString().equals("BOOLEAN"))
                {
                    boolean val = Boolean.parseBoolean(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[1];
                            key[0] = data[offset];
                            offset++;
                            boolean keyBoolean = key[0]==1;
                            if(keyBoolean==val)
                                blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(record[indexInTable].equals(val))
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else
                {
                    return null;
                }
            }
            else if(typeOfFilter.equals("GREATER_THAN"))
            {
                if(type.getSqlTypeName().toString().equals("INTEGER"))
                {
                    int val = Integer.parseInt(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            key[0] = data[offset];
                            key[1] = data[offset+1];
                            key[2] = data[offset+2];
                            key[3] = data[offset+3];
                            offset+=4;
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt<=val)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((int)record[indexInTable]>val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if(type.getSqlTypeName().toString().equals("VARCHAR"))
                {
                    String nval = value.getValue().toString();
                    int startIndex = nval.indexOf("'") + 1;
                    int endIndex = nval.lastIndexOf("'");
                    String val = nval.substring(startIndex, endIndex);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] lengthOfKeyBytes = new byte[2];
                            lengthOfKeyBytes[0] = data[offset+2];
                            lengthOfKeyBytes[1] = data[offset+3];
                            offset+=4;
                            int lengthOfKey = (lengthOfKeyBytes[0] & 0xff) << 8 | (lengthOfKeyBytes[1] & 0xff);
                            byte [] key = new byte[lengthOfKey];
                            for(int j=0;j<lengthOfKey;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            String keyString = new String(key);
                            if(keyString.compareTo(val)<=0)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(((String)record[indexInTable]).compareTo(val)>0)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("DOUBLE"))
                {
                    String temp = value.toString();
                    String [] strings = temp.split(":");
                    String valstring = strings[0];
                    double val = Double.parseDouble(valstring);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[8];
                            for(int j=0;j<8;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            // long keyLong = (key[7] & 0xff) << 56 | (key[6] & 0xff) << 48 | (key[5] & 0xff) << 40 | (key[4] & 0xff) << 32 | (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            long l = ((long) key[7] << 56) |
                                ((long) key[6] & 0xFF) << 48 |
                                ((long) key[5] & 0xFF) << 40 |
                                ((long) key[4] & 0xFF) << 32 |
                                ((long) key[3] & 0xFF) << 24 |
                                ((long) key[2] & 0xFF) << 16 |
                                ((long) key[1] & 0xFF) << 8 |
                                ((long) key[0] & 0xFF);
                            double keyLong = (Double) Double.longBitsToDouble(l);
                            if(keyLong<=val)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((double)record[indexInTable]>val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("FLOAT"))
                {
                    float val = Float.parseFloat(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            for(int j=0;j<4;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt<=val)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((float)record[indexInTable]>val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else
                {
                    return null;
                }
            }
            else if(typeOfFilter.equals("LESS_THAN"))
            {
                if(type.getSqlTypeName().toString().equals("INTEGER"))
                {
                    int val = Integer.parseInt(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            key[0] = data[offset];
                            key[1] = data[offset+1];
                            key[2] = data[offset+2];
                            key[3] = data[offset+3];
                            offset+=4;
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt>=val)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((int)record[indexInTable]<val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if(type.getSqlTypeName().toString().equals("VARCHAR"))
                {
                    String nval = value.getValue().toString();
                    int startIndex = nval.indexOf("'") + 1;
                    int endIndex = nval.lastIndexOf("'");
                    String val = nval.substring(startIndex, endIndex);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] lengthOfKeyBytes = new byte[2];
                            lengthOfKeyBytes[0] = data[offset+2];
                            lengthOfKeyBytes[1] = data[offset+3];
                            offset+=4;
                            int lengthOfKey = (lengthOfKeyBytes[0] & 0xff) << 8 | (lengthOfKeyBytes[1] & 0xff);
                            byte [] key = new byte[lengthOfKey];
                            for(int j=0;j<lengthOfKey;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            String keyString = new String(key);
                            if(keyString.compareTo(val)>=0)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(((String)record[indexInTable]).compareTo(val)<0)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("DOUBLE"))
                {
                    String temp = value.toString();
                    String [] strings = temp.split(":");
                    String valstring = strings[0];
                    double val = Double.parseDouble(valstring);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[8];
                            for(int j=0;j<8;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            // long keyLong = (key[7] & 0xff) << 56 | (key[6] & 0xff) << 48 | (key[5] & 0xff) << 40 | (key[4] & 0xff) << 32 | (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            long l = ((long) key[7] << 56) |
                                ((long) key[6] & 0xFF) << 48 |
                                ((long) key[5] & 0xFF) << 40 |
                                ((long) key[4] & 0xFF) << 32 |
                                ((long) key[3] & 0xFF) << 24 |
                                ((long) key[2] & 0xFF) << 16 |
                                ((long) key[1] & 0xFF) << 8 |
                                ((long) key[0] & 0xFF);
                            double keyLong = (Double) Double.longBitsToDouble(l);
                            if(keyLong>=val)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((double)record[indexInTable]<val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("FLOAT"))
                {
                    float val = Float.parseFloat(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            for(int j=0;j<4;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt>=val)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((float)record[indexInTable]<val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else
                {
                    return null;
                }
            }
            else if(typeOfFilter.equals("GREATER_THAN_OR_EQUAL"))
            {
                if(type.getSqlTypeName().toString().equals("INTEGER"))
                {
                    int val = Integer.parseInt(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            key[0] = data[offset];
                            key[1] = data[offset+1];
                            key[2] = data[offset+2];
                            key[3] = data[offset+3];
                            offset+=4;
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt<val)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    // remove duplicates from blockIdNeeded
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        // int blockIdNeededInt = blockIdNeeded.get(i);
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((int)record[indexInTable]>=val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if(type.getSqlTypeName().toString().equals("VARCHAR"))
                {
                    String nval = value.getValue().toString();
                    int startIndex = nval.indexOf("'") + 1;
                    int endIndex = nval.lastIndexOf("'");
                    String val = nval.substring(startIndex, endIndex);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] lengthOfKeyBytes = new byte[2];
                            lengthOfKeyBytes[0] = data[offset+2];
                            lengthOfKeyBytes[1] = data[offset+3];
                            offset+=4;
                            int lengthOfKey = (lengthOfKeyBytes[0] & 0xff) << 8 | (lengthOfKeyBytes[1] & 0xff);
                            byte [] key = new byte[lengthOfKey];
                            for(int j=0;j<lengthOfKey;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            String keyString = new String(key);
                            if(keyString.compareTo(val)<0)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(((String)record[indexInTable]).compareTo(val)>=0)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("DOUBLE"))
                {
                    String temp = value.toString();
                    String [] strings = temp.split(":");
                    String valstring = strings[0];
                    double val = Double.parseDouble(valstring);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[8];
                            for(int j=0;j<8;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            // long keyLong = (key[7] & 0xff) << 56 | (key[6] & 0xff) << 48 | (key[5] & 0xff) << 40 | (key[4] & 0xff) << 32 | (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            long l = ((long) key[7] << 56) |
                                ((long) key[6] & 0xFF) << 48 |
                                ((long) key[5] & 0xFF) << 40 |
                                ((long) key[4] & 0xFF) << 32 |
                                ((long) key[3] & 0xFF) << 24 |
                                ((long) key[2] & 0xFF) << 16 |
                                ((long) key[1] & 0xFF) << 8 |
                                ((long) key[0] & 0xFF);
                            double keyLong = (Double) Double.longBitsToDouble(l);
                            if(keyLong<val)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((double)record[indexInTable]>=val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("FLOAT"))
                {
                    float val = Float.parseFloat(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=4;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] nextNodeId = new byte[2];
                        nextNodeId[0] = data[offset];
                        nextNodeId[1] = data[offset+1];
                        offset+=4;
                        int nextNodeIdInt = (nextNodeId[0] & 0xff) << 8 | (nextNodeId[1] & 0xff);
                        blockId = nextNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            for(int j=0;j<4;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt<val)
                                continue;
                            blockIdNeeded.add(currentBlockIdInt);
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((float)record[indexInTable]>=val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else
                {
                    return null;
                }
            }
            else if(typeOfFilter.equals("LESS_THAN_OR_EQUAL"))
            {
                if(type.getSqlTypeName().toString().equals("INTEGER"))
                {
                    int val = Integer.parseInt(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            key[0] = data[offset];
                            key[1] = data[offset+1];
                            key[2] = data[offset+2];
                            key[3] = data[offset+3];
                            offset+=4;
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt>val)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((int)record[indexInTable]<=val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if(type.getSqlTypeName().toString().equals("VARCHAR"))
                {
                    String nval = value.getValue().toString();
                    int startIndex = nval.indexOf("'") + 1;
                    int endIndex = nval.lastIndexOf("'");
                    String val = nval.substring(startIndex, endIndex);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] lengthOfKeyBytes = new byte[2];
                            lengthOfKeyBytes[0] = data[offset+2];
                            lengthOfKeyBytes[1] = data[offset+3];
                            offset+=4;
                            int lengthOfKey = (lengthOfKeyBytes[0] & 0xff) << 8 | (lengthOfKeyBytes[1] & 0xff);
                            byte [] key = new byte[lengthOfKey];
                            for(int j=0;j<lengthOfKey;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            String keyString = new String(key);
                            if(keyString.compareTo(val)>0)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if(((String)record[indexInTable]).compareTo(val)<=0)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("DOUBLE"))
                {
                    String temp = value.toString();
                    String [] strings = temp.split(":");
                    String valstring = strings[0];
                    double val = Double.parseDouble(valstring);
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[8];
                            for(int j=0;j<8;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            // long keyLong = (key[7] & 0xff) << 56 | (key[6] & 0xff) << 48 | (key[5] & 0xff) << 40 | (key[4] & 0xff) << 32 | (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            long l = ((long) key[7] << 56) |
                                ((long) key[6] & 0xFF) << 48 |
                                ((long) key[5] & 0xFF) << 40 |
                                ((long) key[4] & 0xFF) << 32 |
                                ((long) key[3] & 0xFF) << 24 |
                                ((long) key[2] & 0xFF) << 16 |
                                ((long) key[1] & 0xFF) << 8 |
                                ((long) key[0] & 0xFF);
                            double keyLong = (Double) Double.longBitsToDouble(l);
                            if(keyLong>val)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((double)record[indexInTable]<=val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else if (type.getSqlTypeName().toString().equals("FLOAT"))
                {
                    float val = Float.parseFloat(value.toString());
                    List<Integer> blockIdNeeded = new ArrayList<>();
                    while(blockId!=0)
                    {
                        data = storage_manager.get_data_block(indexFileName, blockId);
                        // now extract the keys and block id from the data 
                        int offset = 0;
                        byte [] numKeys = new byte[2];
                        numKeys[0] = data[offset];
                        numKeys[1] = data[offset+1];
                        offset+=2;
                        int numKeysInt = (numKeys[0] & 0xff) << 8 | (numKeys[1] & 0xff);
                        byte [] prevNodeId = new byte[2];
                        prevNodeId[0] = data[offset];
                        prevNodeId[1] = data[offset+1];
                        offset+=6;
                        int prevNodeIdInt = (prevNodeId[0] & 0xff) << 8 | (prevNodeId[1] & 0xff);
                        blockId = prevNodeIdInt;
                        for(int i=0;i<numKeysInt;i++)
                        {
                            byte [] currentBlockId = new byte[2];
                            currentBlockId[0] = data[offset];
                            currentBlockId[1] = data[offset+1];
                            offset+=4;
                            int currentBlockIdInt = (currentBlockId[0] & 0xff) << 8 | (currentBlockId[1] & 0xff);
                            byte [] key = new byte[4];
                            for(int j=0;j<4;j++)
                            {
                                key[j] = data[offset];
                                offset++;
                            }
                            int keyInt = (key[3] & 0xff) << 24 | (key[2] & 0xff) << 16 | (key[1] & 0xff) << 8 | (key[0] & 0xff);
                            if(keyInt>val)
                            {
                                blockId = 0;
                            }
                            else
                            {
                                blockIdNeeded.add(currentBlockIdInt);
                            }
                        }
                    }
                    Set<Integer> set = new HashSet<>(blockIdNeeded);
                    blockIdNeeded.clear();
                    blockIdNeeded.addAll(set);
                    for(int blockIdNeededInt: blockIdNeeded)
                    {
                        List<Object[]> records = storage_manager.get_records_from_block(tableName, blockIdNeededInt);
                        for(Object[] record: records)
                        {
                            if((float)record[indexInTable]<=val)
                            {
                                result.add(record);
                            }
                        }
                    }
                    return result;
                }
                else
                {
                    return null;
                }
            }

        

            // if(type.getSqlTypeName().toString().equals("INTEGER"))
            // {
            //     int val = Integer.parseInt(value.toString());
            //     if(typeOfFilter.equals("EQUALS"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(record[indexInTable].equals(value))
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((int)record[indexInTable] > val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((int)record[indexInTable] < val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((int)record[indexInTable] >= val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((int)record[indexInTable] <= val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else
            //     {
            //         return null;
            //     }
                
            // }
            // else if(type.getSqlTypeName().toString().equals("VARCHAR"))
            // {
            //     String val = value.toString();
            //     if(typeOfFilter.equals("EQUALS"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(record[indexInTable].equals(val))
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(((String)record[indexInTable]).compareTo(val) > 0)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(((String)record[indexInTable]).compareTo(val) < 0)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(((String)record[indexInTable]).compareTo(val) >= 0)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(((String)record[indexInTable]).compareTo(val) <= 0)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else
            //     {
            //         return null;
            //     }
            // }
            // else if(type.getSqlTypeName().toString().equals("BOOLEAN"))
            // {
            //     boolean val = Boolean.parseBoolean(value.toString());
            //     if(typeOfFilter.equals("EQUALS"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(record[indexInTable].equals(val))
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else
            //     {
            //         return null;
            //     }
            // }
            // else if (type.getSqlTypeName().toString().equals("DOUBLE"))
            // {
            //     double val = Double.parseDouble(value.toString());
            //     if(typeOfFilter.equals("EQUALS"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(record[indexInTable].equals(val))
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((double)record[indexInTable] > val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((double)record[indexInTable] < val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((double)record[indexInTable] >= val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((double)record[indexInTable] <= val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else
            //     {
            //         return null;
            //     }
            // }
            // else if (type.getSqlTypeName().toString().equals("FLOAT"))
            // {
            //     float val = Float.parseFloat(value.toString());
            //     if(typeOfFilter.equals("EQUALS"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if(record[indexInTable].equals(val))
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((float)record[indexInTable] > val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((float)record[indexInTable] < val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("GREATER_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((float)record[indexInTable] >= val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else if(typeOfFilter.equals("LESS_THAN_OR_EQUAL"))
            //     {
            //         List<Object[]> result = new ArrayList<>();
            //         for(Object[] record: allRecords)
            //         {
            //             if((float)record[indexInTable] <= val)
            //             {
            //                 result.add(record);
            //             }
            //         }
            //         return result;
            //     }
            //     else
            //     {
            //         return null;
            //     }
            // }
            // else
            // {
            //     return null;
            // }
            return null;
        }
}