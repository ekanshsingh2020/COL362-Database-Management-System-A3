package manager;

import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;
import index.bplusTree.BPlusTreeIndexFile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;
    public HashMap<String,List<RelDataType>> file_to_typelist;
    public HashMap<String,List<String>> table_column_type;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
        file_to_typelist = new HashMap<>();
        table_column_type = new HashMap<>();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);
        List<RelDataType> modified_typeList = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }
                    set_type_column(table_name, modified_typeList, columns, typeList, columnNamesList);
                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise
        if(!check_file_exists(table_name)) {
            return null;
        }

        byte[] block = get_data_block(table_name, block_id);

        if (block == null || block.length == 0) {
            return null;
        }
        
        List<Object[]> records = new ArrayList<>();

        int offset = 0;
        short num_of_records = (short) ((block[offset+1] & 0xFF) | ((block[offset] & 0xFF) << 8));
        offset += 2;
        int last_offset = 4096;
        for(int i=0;i<num_of_records;i++)
        {
            // read the record from the block
            byte [] record_offset_bytes = new byte[2];
            record_offset_bytes[0] = block[offset];
            record_offset_bytes[1] = block[offset+1];
            int record_offset = (record_offset_bytes[1] & 0xFF) | ((record_offset_bytes[0] & 0xFF) << 8);
            offset += 2;
            int len_of_record = last_offset - record_offset;
            byte [] record = new byte[len_of_record];
            for(int j=record_offset;j<last_offset;j++)
            {
                record[j-record_offset] = block[j];
            }
            last_offset = record_offset;
            records.add(decode_record(record, table_name));
        }

        return records;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        List<RelDataType> typelist_for_table = file_to_typelist.get(table_name);
        List<String> columns = table_column_type.get(table_name);
        int column_index = columns.indexOf(column_name);
        RelDataType column_type = typelist_for_table.get(column_index);
        String index_file_name = table_name + "_" + column_name + "_index";

        if(column_type.getSqlTypeName().getName().equals("VARCHAR"))
        {
            Class <String> typeClass = String.class;
            BPlusTreeIndexFile<String> index = new BPlusTreeIndexFile<String>(order, typeClass);

            int idx=1; // first block is schema block
            while(get_records_from_block(table_name, idx)!=null)
            {
                List<Object[]> records = get_records_from_block(table_name, idx);
                for(int i=0;i<records.size();i++)
                {
                    String key = (String)(records.get(i)[column_index]);
                    index.insert(key, idx);
                }
                idx++;
            }
            int index_file_id = db.addFile(index);
            file_to_fileid.put(index_file_name, index_file_id);
            return true;
        }
        else if(column_type.getSqlTypeName().getName().equals("INTEGER"))
        {

            Class <Integer> typeClass = Integer.class;
            BPlusTreeIndexFile<Integer> index = new BPlusTreeIndexFile<Integer>(order, typeClass);

            int idx=1; // first block is schema block
            while(get_records_from_block(table_name, idx)!=null)
            {
                List<Object[]> records = get_records_from_block(table_name, idx);
                for(int i=0;i<records.size();i++)
                {
                    Integer key = (Integer)(records.get(i)[column_index]);
                    index.insert(key, idx);
                }
                idx++;
            }
            int index_file_id = db.addFile(index);
            file_to_fileid.put(index_file_name, index_file_id);
            return true;
        }
        else if(column_type.getSqlTypeName().getName().equals("BOOLEAN"))
        {
            Class <Boolean> typeClass = Boolean.class;
            BPlusTreeIndexFile<Boolean> index = new BPlusTreeIndexFile<Boolean>(order, typeClass);

            int idx=1; // first block is schema block
            while(get_records_from_block(table_name, idx)!=null)
            {
                List<Object[]> records = get_records_from_block(table_name, idx);
                for(int i=0;i<records.size();i++)
                {
                    Boolean key = (Boolean)(records.get(i)[column_index]);
                    index.insert(key, idx);
                }
                idx++;
            }
            int index_file_id = db.addFile(index);
            file_to_fileid.put(index_file_name, index_file_id);
            return true;
        }
        else if(column_type.getSqlTypeName().getName().equals("FLOAT"))
        {
            Class <Float> typeClass = Float.class;
            BPlusTreeIndexFile<Float> index = new BPlusTreeIndexFile<Float>(order, typeClass);

            int idx=1; // first block is schema block
            while(get_records_from_block(table_name, idx)!=null)
            {
                List<Object[]> records = get_records_from_block(table_name, idx);
                for(int i=0;i<records.size();i++)
                {
                    Float key = (Float)(records.get(i)[column_index]);
                    index.insert(key, idx);
                }
                idx++;
            }
            int index_file_id = db.addFile(index);
            file_to_fileid.put(index_file_name, index_file_id);
            return true;
        }
        else if(column_type.getSqlTypeName().getName().equals("DOUBLE"))
        {
            Class <Double> typeClass = Double.class;
            BPlusTreeIndexFile<Double> index = new BPlusTreeIndexFile<Double>(order, typeClass);

            int idx=1; // first block is schema block
            while(get_records_from_block(table_name, idx)!=null)
            {
                List<Object[]> records = get_records_from_block(table_name, idx);
                for(int i=0;i<records.size();i++)
                {
                    Double key = (Double)(records.get(i)[column_index]);
                    index.insert(key, idx);
                }
                idx++;
            }
            int index_file_id = db.addFile(index);
            file_to_fileid.put(index_file_name, index_file_id);
            return true;
        }
        else
        {
            return false;
        }
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        String index_file_name = table_name + "_" + column_name + "_index";
        if(check_index_exists(table_name, column_name))
        {
            int file_id = file_to_fileid.get(index_file_name);
            if(value.getType().getSqlTypeName().getName().equals("INTEGER"))
            {
                int key = Integer.parseInt(value.getValue().toString());
                return db.search_index(file_id, key);
            }
            else if(value.getType().getSqlTypeName().getName().equals("BOOLEAN"))
            {
                boolean key = Boolean.parseBoolean(value.getValue().toString());
                return db.search_index(file_id, key);
            }
            else if(value.getType().getSqlTypeName().getName().equals("FLOAT"))
            {
                float key = Float.parseFloat(value.getValue().toString());
                return db.search_index(file_id, key);
            }
            else if(value.getType().getSqlTypeName().getName().equals("DOUBLE"))
            {
                String temp = value.toString();
                String [] strings = temp.split(":");
                String val = strings[0];
                double key = Double.parseDouble(val);
                System.out.println("key is "+ key);
                return db.search_index(file_id, key);
            }
            else if(value.getType().getSqlTypeName().getName().equals("VARCHAR"))
            {
                String nval = value.getValue().toString();
                int startIndex = nval.indexOf("'") + 1;
                int endIndex = nval.lastIndexOf("'");
                String key = nval.substring(startIndex, endIndex);
                return db.search_index(file_id, key);
            }
            else
            {
                return -1;
            }
        }
        else
        {
            return -1;
        }
        
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }
    
    public void set_type_column(String table_name,List<RelDataType> modified_typeList, List<String> columns_modified, List<RelDataType> typeList, List<String> columns)
    {
        for(int i=0;i<typeList.size();i++)
        {
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) 
            {
                modified_typeList.add(typeList.get(i));
                columns_modified.add(columns.get(i));
            } 
        }
        for(int i=0;i<typeList.size();i++)
        {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) 
            {
                modified_typeList.add(typeList.get(i));
                columns_modified.add(columns.get(i));
            } 
        }
        file_to_typelist.put(table_name, modified_typeList);
        table_column_type.put(table_name, columns_modified);
    }

    public int parseIntegerFromBytes(byte [] bytes)
    {
        int result = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8) | ((bytes[2] & 0xFF) << 16) | ((bytes[3] & 0xFF) << 24);
        return result;
    }

    public float parseFloatFromBytes(byte [] bytes)
    {
        int result = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8) | ((bytes[2] & 0xFF) << 16) | ((bytes[3] & 0xFF) << 24);
        return Float.intBitsToFloat(result);
    }

    public double parseDoubleFromBytes(byte [] bytes)
    {
    
        long result = ((long) bytes[7] << 56) |
                ((long) bytes[6] & 0xFF) << 48 |
                ((long) bytes[5] & 0xFF) << 40 |
                ((long) bytes[4] & 0xFF) << 32 |
                ((long) bytes[3] & 0xFF) << 24 |
                ((long) bytes[2] & 0xFF) << 16 |
                ((long) bytes[1] & 0xFF) << 8 |
                ((long) bytes[0] & 0xFF);
        return Double.longBitsToDouble(result);
    }

    public boolean parseBooleanFromBytes(byte [] bytes)
    {
        return bytes[0] == 1;
    }

    public String parseStringFromBytes(byte [] bytes)
    {
        return new String(bytes);
    }

    public Object [] decode_record(byte [] seq, String table_name)
    {
        /* Write your code here */
        List<RelDataType>typelist_for_table = file_to_typelist.get(table_name);
        int num_columns = typelist_for_table.size();
        int variable_length_columns_size = 0;
        int fixed_length_bytes = 0;
        for(int i=0;i<num_columns;i++)
        {
            if(typelist_for_table.get(i).getSqlTypeName().getName().equals("VARCHAR"))
            {
                variable_length_columns_size++;
            }
            else
            {
                if(typelist_for_table.get(i).getSqlTypeName().getName().equals("INTEGER"))
                {
                    fixed_length_bytes += 4;
                }
                else if(typelist_for_table.get(i).getSqlTypeName().getName().equals("BOOLEAN"))
                {
                    fixed_length_bytes += 1;
                }
                else if(typelist_for_table.get(i).getSqlTypeName().getName().equals("FLOAT"))
                {
                    fixed_length_bytes += 4;
                }
                else if(typelist_for_table.get(i).getSqlTypeName().getName().equals("DOUBLE"))
                {
                    fixed_length_bytes += 8;
                }
            }
        }
        Object [] result = new Object[num_columns];
        int var_field_offset = 0;
        int fixed_length_offset = 4*variable_length_columns_size;
        int bitmap_offset = 4*variable_length_columns_size + fixed_length_bytes;
        int bit_index = 0;
        for(int i=0;i<num_columns;i++)
        {
            if(!typelist_for_table.get(i).getSqlTypeName().getName().equals("VARCHAR"))
            {
                if((seq[bitmap_offset] & (1 << (7 - bit_index))) == 0)
                {
                    if(typelist_for_table.get(i).getSqlTypeName().getName().equals("INTEGER"))
                    {
                        byte [] int_bytes = new byte[4];
                        for(int j=0;j<4;j++)
                        {
                            int_bytes[j] = seq[fixed_length_offset+j];
                        }
                        result[i] = parseIntegerFromBytes(int_bytes);
                        fixed_length_offset += 4;
                    }
                    else if(typelist_for_table.get(i).getSqlTypeName().getName().equals("BOOLEAN"))
                    {
                        byte [] bool_bytes = new byte[1];
                        bool_bytes[0] = seq[fixed_length_offset];
                        result[i] = parseBooleanFromBytes(bool_bytes);
                        fixed_length_offset += 1;
                    }
                    else if(typelist_for_table.get(i).getSqlTypeName().getName().equals("FLOAT"))
                    {
                        byte [] float_bytes = new byte[4];
                        for(int j=0;j<4;j++)
                        {
                            float_bytes[j] = seq[fixed_length_offset+j];
                        }
                        result[i] = parseFloatFromBytes(float_bytes);
                        fixed_length_offset += 4;
                    }
                    else if(typelist_for_table.get(i).getSqlTypeName().getName().equals("DOUBLE"))
                    {
                        byte [] double_bytes = new byte[8];
                        for(int j=0;j<8;j++)
                        {
                            double_bytes[j] = seq[fixed_length_offset+j];
                        }
                        result[i] = parseDoubleFromBytes(double_bytes);
                        fixed_length_offset += 8;
                    }
                }
                else
                {
                    result[i] = null;
                }
                bit_index++;
                if(bit_index == 8)
                {
                    bit_index = 0;
                    bitmap_offset++;
                }
            }
        }
        for(int i=0;i<num_columns;i++)
        {
            if(typelist_for_table.get(i).getSqlTypeName().getName().equals("VARCHAR"))
            {
                if((seq[bitmap_offset] & (1 << (7 - bit_index))) == 0)
                {
                    byte [] offset_bytes = new byte[2];
                    offset_bytes[0] = seq[var_field_offset];
                    offset_bytes[1] = seq[var_field_offset+1];
                    int offset = ((offset_bytes[0] & 0xFF) | ((offset_bytes[1] & 0xFF) << 8));
                    byte [] length_bytes = new byte[2];
                    length_bytes[0] = seq[var_field_offset+2];
                    length_bytes[1] = seq[var_field_offset+3];
                    int length = ((length_bytes[0] & 0xFF) | ((length_bytes[1] & 0xFF) << 8));
                    var_field_offset += 4;
                    byte [] var_field = new byte[length];
                    for(int j=0;j<length;j++)
                    {
                        var_field[j] = seq[offset+j];
                    }
                    result[i] = parseStringFromBytes(var_field);
                }  
                else
                {
                    result[i] = null;
                }
                bit_index++;
                if(bit_index == 8)
                {
                    bit_index = 0;
                    bitmap_offset++;
                }
            }
        }
        return result;
    }

}