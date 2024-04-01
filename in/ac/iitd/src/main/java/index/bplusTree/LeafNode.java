package index.bplusTree;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        byte [] data = this.get_data();
        
        int offset = 10;

        for(int i=0;i<numKeys;i++){
            byte [] keyLength = new byte[2];
            keyLength[0] = data[offset];
            keyLength[1] = data[offset+1];
            int lengthOfKey = (keyLength[0] << 8) | (keyLength[1] & 0xFF);
            offset += 2;
            byte [] key = new byte[lengthOfKey];
            for(int j=0;j<lengthOfKey;j++){
                key[j] = data[offset];
                offset++;
            }
            keys[i] = convertBytesToT(key, typeClass);
            offset +=2;
        }
        return keys;

    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int offset = 8;
        byte [] data = this.get_data();

        for(int i=0;i<numKeys;i++){
            byte [] blockId = new byte[2];
            blockId[0] = data[offset];
            blockId[1] = data[offset+1];
            block_ids[i] = (blockId[0] << 8) | (blockId[1] & 0xFF);
            offset += 2;
            int lengthOfKey = (data[offset] << 8) | (data[offset+1] & 0xFF);
            offset += lengthOfKey + 2;
        }
        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {


        /* Write your code here */
        int numKeys = getNumKeys();
        byte [] data = this.get_data();

        int offset = 8;
        List<List<Object>> key_blockId_keyLength = new ArrayList<>();
        for(int i=0;i<numKeys;i++){
            byte [] blockId = new byte[2];
            blockId[0] = data[offset];
            blockId[1] = data[offset+1];
            int blockIdInt = (blockId[0] << 8) | (blockId[1] & 0xFF);
            offset += 2;
            int lengthOfKey = (data[offset] << 8) | (data[offset+1] & 0xFF);
            offset += 2;
            byte [] keyBytes = new byte[lengthOfKey];
            for(int j=0;j<lengthOfKey;j++){
                keyBytes[j] = data[offset];
                offset++;
            }
            key_blockId_keyLength.add(new ArrayList<>());
            key_blockId_keyLength.get(i).add(convertBytesToT(keyBytes, typeClass));
            key_blockId_keyLength.get(i).add(blockIdInt);
            key_blockId_keyLength.get(i).add(lengthOfKey);
        }
        key_blockId_keyLength.add(new ArrayList<>());
        key_blockId_keyLength.get(numKeys).add(key);
        key_blockId_keyLength.get(numKeys).add(block_id);
        // if the typeclass is string, the find length of the string
        // if the typeclass is integer, the length of the key is 4
        int lengthOfKey = 0;
        if(typeClass == String.class){
            lengthOfKey = ((String)key).length();
        }
        else if(typeClass == Integer.class){
            lengthOfKey = 4;
        }
        else if(typeClass == Double.class){
            lengthOfKey = 8;
        }
        else if(typeClass == Float.class){
            lengthOfKey = 4;
        }
        else if(typeClass == Boolean.class){
            lengthOfKey = 1;
        }

        key_blockId_keyLength.get(numKeys).add(lengthOfKey);

        if(typeClass == String.class){
            key_blockId_keyLength.sort(Comparator.comparing(o -> o.get(0).toString()));
        }
        else if(typeClass == Integer.class){
            key_blockId_keyLength.sort(Comparator.comparing(o -> (int)o.get(0)));
        }
        else if(typeClass == Double.class){
            key_blockId_keyLength.sort(Comparator.comparing(o -> (double)o.get(0)));
        }
        else if(typeClass == Float.class){
            key_blockId_keyLength.sort(Comparator.comparing(o -> (float)o.get(0)));
        }
        else if(typeClass == Boolean.class){
            key_blockId_keyLength.sort(Comparator.comparing(o -> (boolean)o.get(0)));
        }

        numKeys++;

        byte [] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) (numKeys >> 8);
        numKeysBytes[1] = (byte) (numKeys & 0xFF);
        this.write_data(0, numKeysBytes);
        int newOffset = 8;
        for(int i=0;i<numKeys;i++){
            byte [] blockId = new byte[2];
            blockId[0] = (byte) ((int)key_blockId_keyLength.get(i).get(1) >> 8);
            blockId[1] = (byte) ((int)key_blockId_keyLength.get(i).get(1) & 0xFF);
            this.write_data(newOffset, blockId);
            newOffset += 2;
            byte [] keyLength = new byte[2];
            keyLength[0] = (byte) ((int)key_blockId_keyLength.get(i).get(2) >> 8);
            keyLength[1] = (byte) ((int)key_blockId_keyLength.get(i).get(2) & 0xFF);
            this.write_data(newOffset, keyLength);
            newOffset += 2;
            byte [] key_new = convertTToBytes((T)key_blockId_keyLength.get(i).get(0), typeClass);
            for(int j=0;j<key_new.length;j++){
                this.write_data(newOffset, new byte[]{key_new[j]});
                newOffset++;
            }
        }
        byte [] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (newOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) (newOffset & 0xFF);
        this.write_data(6, nextFreeOffsetBytes);
        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */
        int numKeys = getNumKeys();
        byte [] data = this.get_data();
        int offset = 8;
        for(int i=0;i<numKeys;i++){
            byte [] blockId = new byte[2];
            blockId[0] = data[offset];
            blockId[1] = data[offset+1];
            int blockIdInt = (blockId[0] << 8) | (blockId[1] & 0xFF);
            offset += 2;
            int lengthOfKey = (data[offset] << 8) | (data[offset+1] & 0xFF);
            offset += 2;
            byte [] keyBytes = new byte[lengthOfKey];
            for(int j=0;j<lengthOfKey;j++){
                keyBytes[j] = data[offset];
                offset++;
            }
            T keyInNode = convertBytesToT(keyBytes, typeClass);
            if(keyInNode.equals(key)){
                return blockIdInt;
            }
        }
        return -1;
    }

    public int getNextLeafId() {
        byte[] data = this.get_data();
        int nextLeafId = (data[4] << 8) | (data[5] & 0xFF);
        return nextLeafId;
    }

    public int getprevLeafId() {
        byte[] data = this.get_data();
        int prevLeafId = (data[2] << 8) | (data[3] & 0xFF);
        return prevLeafId;
    }

}
