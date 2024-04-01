package index.bplusTree;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        byte [] data = this.get_data();
        int offset = 6;
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

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        int numKeys = getNumKeys();
        byte [] data = this.get_data();
        int offset = 6;
        List<List<Object>>key_keyLength_rightchildId = new ArrayList<>();
        for(int i=0;i<numKeys;i++){
            byte [] keyLength = new byte[2];
            keyLength[0] = data[offset];
            keyLength[1] = data[offset+1];
            int lengthOfKey = (keyLength[0] << 8) | (keyLength[1] & 0xFF);
            offset += 2;
            byte [] tempKey = new byte[lengthOfKey];
            for(int j=0;j<lengthOfKey;j++){
                tempKey[j] = data[offset];
                offset++;
            }
            byte [] rightBlockId = new byte[2];
            rightBlockId[0] = data[offset];
            rightBlockId[1] = data[offset+1];
            int rightChildId = (rightBlockId[0] << 8) | (rightBlockId[1] & 0xFF);
            key_keyLength_rightchildId.add(new ArrayList<>());
            key_keyLength_rightchildId.get(i).add(convertBytesToT(tempKey, typeClass));
            key_keyLength_rightchildId.get(i).add(lengthOfKey);
            key_keyLength_rightchildId.get(i).add(rightChildId);
            offset +=2;
        }
        key_keyLength_rightchildId.add(new ArrayList<>());
        key_keyLength_rightchildId.get(numKeys).add(key);
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
        key_keyLength_rightchildId.get(numKeys).add(lengthOfKey);
        key_keyLength_rightchildId.get(numKeys).add(right_block_id);

        if(typeClass == String.class){
            key_keyLength_rightchildId.sort(Comparator.comparing(o -> o.get(0).toString()));
        }
        else if(typeClass == Integer.class){
            key_keyLength_rightchildId.sort(Comparator.comparing(o -> (int)o.get(0)));
        }
        else if(typeClass == Double.class){
            key_keyLength_rightchildId.sort(Comparator.comparing(o -> (double)o.get(0)));
        }
        else if(typeClass == Float.class){
            key_keyLength_rightchildId.sort(Comparator.comparing(o -> (float)o.get(0)));
        }
        else if(typeClass == Boolean.class){
            key_keyLength_rightchildId.sort(Comparator.comparing(o -> (boolean)o.get(0)));
        }

        numKeys++;

        byte [] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) (numKeys >> 8);
        numKeysBytes[1] = (byte) (numKeys & 0xFF);
        this.write_data(0, numKeysBytes);

        int newOffset = 6;

        for(int i=0;i<numKeys;i++){
            byte [] newKey = convertTToBytes((T)key_keyLength_rightchildId.get(i).get(0), typeClass);
            byte [] newKeyLength = new byte[2];
            newKeyLength[0] = (byte) ((int)key_keyLength_rightchildId.get(i).get(1) >> 8);
            newKeyLength[1] = (byte) ((int)key_keyLength_rightchildId.get(i).get(1) & 0xFF);
            byte [] newRightBlockId = new byte[2];
            newRightBlockId[0] = (byte) ((int)key_keyLength_rightchildId.get(i).get(2) >> 8);
            newRightBlockId[1] = (byte) ((int)key_keyLength_rightchildId.get(i).get(2) & 0xFF);
            this.write_data(newOffset, newKeyLength);
            newOffset += 2;
            for(int j=0;j<newKey.length;j++){
                this.write_data(newOffset, new byte[]{newKey[j]});
                newOffset++;
            }
            this.write_data(newOffset, newRightBlockId);
            newOffset += 2;
        }
        byte [] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (newOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) (newOffset & 0xFF);
        this.write_data(2, nextFreeOffsetBytes);
        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */
        return -1;
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        byte [] data = this.get_data();
        int offset = 4;

        for(int i=0;i<numKeys;i++)
        {
            byte [] blockId = new byte[2];
            blockId[0] = data[offset];
            blockId[1] = data[offset+1];
            children[i] = (blockId[0] << 8) | (blockId[1] & 0xFF);
            offset += 2;
            int lengthOfKey = (data[offset] << 8) | (data[offset+1] & 0xFF);
            offset += lengthOfKey + 2;
        }
        byte [] blockId = new byte[2];
        blockId[0] = data[offset];
        blockId[1] = data[offset+1];
        children[numKeys] = (blockId[0] << 8) | (blockId[1] & 0xFF);
        return children;

    }

}
