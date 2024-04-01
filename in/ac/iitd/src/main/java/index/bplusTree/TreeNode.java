package index.bplusTree;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */
        if(typeClass == Integer.class){
            return (T) (Integer) ((bytes[0] & 0xFF) | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24));
        }
        else if(typeClass == String.class){
            return (T) new String(bytes);
        }
        else if(typeClass == Double.class){
            long l = ((long) bytes[7] << 56) |
                ((long) bytes[6] & 0xFF) << 48 |
                ((long) bytes[5] & 0xFF) << 40 |
                ((long) bytes[4] & 0xFF) << 32 |
                ((long) bytes[3] & 0xFF) << 24 |
                ((long) bytes[2] & 0xFF) << 16 |
                ((long) bytes[1] & 0xFF) << 8 |
                ((long) bytes[0] & 0xFF);
            return (T) (Double) Double.longBitsToDouble(l);
        }
        else if(typeClass == Float.class){
            int i = ((bytes[0] & 0xFF) | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24));
            return (T) (Float) Float.intBitsToFloat(i);
        }
        else if(typeClass == Boolean.class){
            return (T) (Boolean) (bytes[0] == 1);
        }
        else
            return null;
    }

    default public byte[] convertTToBytes(T key, Class<T> typeClass){
        
        /* Write your code here */

        if(typeClass == Integer.class){
            int i = (Integer) key;
            byte[] bytes = new byte[4];
            bytes[0] = (byte) (i & 0xFF);
            bytes[1] = (byte) ((i >> 8) & 0xFF);
            bytes[2] = (byte) ((i >> 16) & 0xFF);
            bytes[3] = (byte) ((i >> 24) & 0xFF);
            return bytes;
        }
        else if(typeClass == String.class){
            return ((String) key).getBytes();
        }
        else if(typeClass == Double.class){
            long l = Double.doubleToLongBits((Double) key);
            byte[] bytes = new byte[8];
            bytes[0] = (byte) (l & 0xFF);
            bytes[1] = (byte) ((l >> 8) & 0xFF);
            bytes[2] = (byte) ((l >> 16) & 0xFF);
            bytes[3] = (byte) ((l >> 24) & 0xFF);
            bytes[4] = (byte) ((l >> 32) & 0xFF);
            bytes[5] = (byte) ((l >> 40) & 0xFF);
            bytes[6] = (byte) ((l >> 48) & 0xFF);
            bytes[7] = (byte) ((l >> 56) & 0xFF);
            return bytes;
        }
        else if(typeClass == Float.class){
            int i = Float.floatToIntBits((Float) key);
            byte[] bytes = new byte[4];
            bytes[0] = (byte) (i & 0xFF);
            bytes[1] = (byte) ((i >> 8) & 0xFF);
            bytes[2] = (byte) ((i >> 16) & 0xFF);
            bytes[3] = (byte) ((i >> 24) & 0xFF);
            return bytes;
        }
        else if(typeClass == Boolean.class){
            byte[] bytes = new byte[1];
            bytes[0] = (byte) ((Boolean) key ? 1 : 0);
            return bytes;
        }
        else
            return null;
    }
    
}