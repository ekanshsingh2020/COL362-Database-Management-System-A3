package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import com.google.protobuf.Internal;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */
        int leftLeafId = leafToInsertIn(key); // found the leaf to insert into


        if(leftLeafId == getRootId())
        {
            insertIfTheLeafIsRoot(key, block_id);
            return;
        }

        LeafNode<T> leftLeaf = (LeafNode<T>) blocks.get(leftLeafId);
        if(!isFull(leftLeafId))
        {
            leftLeaf.insert(key, block_id);
        }
        else
        {
            LeafNode <T> rightLeaf = new LeafNode<>(typeClass);
            LeafNode <T> tempLeaf = new LeafNode<>(typeClass);
            int order = getOrder();
            T[] keys = leftLeaf.getKeys();
            int[] blockIds = leftLeaf.getBlockIds();
            // traverse the leaf node and copy the keys and blockIds to tempLeaf
            for (int i = 0; i < (order-1); i++) {
                tempLeaf.insert(keys[i], blockIds[i]);
            }
            // insert the new key and block_id in tempLeaf
            tempLeaf.insert(key, block_id);

            T [] tempKeys = tempLeaf.getKeys();
            int [] tempBlockIds = tempLeaf.getBlockIds();
            // print the keys and blockIds in tempLeaf
           
            byte[] nextFreeOffsetBytes = new byte[2];
            nextFreeOffsetBytes[0] = 0;
            nextFreeOffsetBytes[1] = 8;

            byte[] numEntriesBytes = new byte[2];
            numEntriesBytes[0] = 0;
            numEntriesBytes[1] = 0;
            leftLeaf.write_data(0, numEntriesBytes);

            leftLeaf.write_data(6, nextFreeOffsetBytes);

            for (int i = 0; i < (order/2); i++) {
                leftLeaf.insert(tempKeys[i], tempBlockIds[i]);
            }

            for (int i = (order/2); i < order; i++) {
                rightLeaf.insert(tempKeys[i], tempBlockIds[i]);
            }

            int rightLeafId = blocks.size();

            byte [] rightLeafIdBytes = new byte[2];
            rightLeafIdBytes[0] = (byte) ((rightLeafId >> 8) & 0xFF);
            rightLeafIdBytes[1] = (byte) (rightLeafId & 0xFF);

            byte [] leftLeafIdBytes = new byte[2];
            leftLeafIdBytes[0] = (byte) ((leftLeafId >> 8) & 0xFF);
            leftLeafIdBytes[1] = (byte) (leftLeafId & 0xFF);

            byte [] leftLeadNextIdBytes = new byte[2];
            leftLeadNextIdBytes[0] = leftLeaf.get_data()[4];
            leftLeadNextIdBytes[1] = leftLeaf.get_data()[5];

            rightLeaf.write_data(4, leftLeadNextIdBytes);

            leftLeaf.write_data(4, rightLeafIdBytes);
            rightLeaf.write_data(2, leftLeafIdBytes);

            blocks.add(rightLeaf);

            List<Integer> pathTillNode = new ArrayList<>();
            int rootId = getRootId();
            int currId = rootId;
            while(!isLeaf(currId))
            {
                InternalNode<T> currNode = (InternalNode<T>) blocks.get(currId);
                int i = 0;
                T[] currKeys = currNode.getKeys();
                int [] children = currNode.getChildren();
                while(i < currKeys.length && compareT(key, currKeys[i]) > 0)
                    i++;
                int childId = children[i];
                pathTillNode.add(currId);
                currId = childId;              
            }
            pathTillNode.add(currId);
            insertInParent(tempKeys[order/2], rightLeafId, pathTillNode);
        }
        return;
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        int rootId = getRootId();
        int currId = rootId;
        int flag=0;
        System.out.println("Root id is "+rootId);
        while(!isLeaf(currId))
        {
            InternalNode<T> currNode = (InternalNode<T>) blocks.get(currId);
            int i = 0;
            T[] keys = currNode.getKeys();
            int [] children = currNode.getChildren();
            while(i < keys.length && compareT(key, keys[i]) > 0)
                i++;
            if(i<keys.length && compareT(key, keys[i]) == 0)
            {
                flag=1;
            }
            int childId = children[i];
            currId = childId;
        }
        LeafNode <T> leaf = (LeafNode<T>) blocks.get(currId);
        System.out.println("Leaf id is "+currId);
        
        T[] keys = leaf.getKeys();
        for (int i = 0; i < keys.length; i++) {
            if(compareT(key, keys[i]) == 0)
                return currId;
        }
        if(compareT(key, keys[keys.length-1])>0)
                return -1;
        if(flag==1)
        {
            while(true)
            {
                currId = leaf.getNextLeafId();
                leaf = (LeafNode<T>) blocks.get(currId);
                keys = leaf.getKeys();
                for (int i = 0; i < keys.length; i++) {
                    if(compareT(key, keys[i]) == 0)
                        return currId;
                }
            }
        }
        
        return currId;
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }


    public int compareT(T a, T b){
        if(typeClass == Integer.class){
            return ((Integer) a).compareTo((Integer) b);
        }
        else if(typeClass == String.class){
            return ((String) a).compareTo((String) b);
        }
        else if(typeClass == Double.class){
            return ((Double) a).compareTo((Double) b);
        }
        else if(typeClass == Float.class){
            return ((Float) a).compareTo((Float) b);
        }
        else if(typeClass == Boolean.class){
            return ((Boolean) a).compareTo((Boolean) b);
        }
        else
            return -500;
      
    }

    public int leafToInsertIn(T key)
    {
        int rootId = getRootId();
        int currId = rootId;
        while(!isLeaf(currId))
        {
            InternalNode<T> currNode = (InternalNode<T>) blocks.get(currId);
            int i = 0;
            T[] keys = currNode.getKeys();
            int [] children = currNode.getChildren();
            while(i < keys.length && compareT(key, keys[i]) > 0)
                i++;
            int childId = children[i];
            currId = childId;
        }
        return currId;
    }


    public void insertIfTheLeafIsRoot(T key, int block_id)
    {
        int leftLeafId = leafToInsertIn(key);
        LeafNode<T> leftLeaf = (LeafNode<T>) blocks.get(leftLeafId);
        if(!isFull(leftLeafId))
        {
            leftLeaf.insert(key, block_id);
        }
        else
        {
            LeafNode <T> rightLeaf = new LeafNode<>(typeClass);
            LeafNode <T> tempLeaf = new LeafNode<>(typeClass);
            int order = getOrder();
            T[] keys = leftLeaf.getKeys();
            int[] blockIds = leftLeaf.getBlockIds();
            // traverse the leaf node and copy the keys and blockIds to tempLeaf
            for (int i = 0; i < (order-1); i++) {
                tempLeaf.insert(keys[i], blockIds[i]);
            }
            // insert the new key and block_id in tempLeaf
            tempLeaf.insert(key, block_id);

            T [] tempKeys = tempLeaf.getKeys();
            int [] tempBlockIds = tempLeaf.getBlockIds();

            byte[] nextFreeOffsetBytes = new byte[2];
            nextFreeOffsetBytes[0] = 0;
            nextFreeOffsetBytes[1] = 8;

            byte[] numEntriesBytes = new byte[2];
            numEntriesBytes[0] = 0;
            numEntriesBytes[1] = 0;
            leftLeaf.write_data(0, numEntriesBytes);

            leftLeaf.write_data(6, nextFreeOffsetBytes);

            for (int i = 0; i < (order/2); i++) {
                leftLeaf.insert(tempKeys[i], tempBlockIds[i]);
            }

            for (int i = (order/2); i < order; i++) {
                rightLeaf.insert(tempKeys[i], tempBlockIds[i]);
            }

            int rightLeafId = blocks.size();

            byte [] rightLeafIdBytes = new byte[2];
            rightLeafIdBytes[0] = (byte) ((rightLeafId >> 8) & 0xFF);
            rightLeafIdBytes[1] = (byte) (rightLeafId & 0xFF);

            byte [] leftLeafIdBytes = new byte[2];
            leftLeafIdBytes[0] = (byte) ((leftLeafId >> 8) & 0xFF);
            leftLeafIdBytes[1] = (byte) (leftLeafId & 0xFF);

            leftLeaf.write_data(4, rightLeafIdBytes);
            rightLeaf.write_data(2, leftLeafIdBytes);

            blocks.add(rightLeaf);

            InternalNode<T> newRoot = new InternalNode<>(tempKeys[order/2],leftLeafId,rightLeafId,typeClass);
            int newRootId = blocks.size();
            blocks.add(newRoot);

            byte [] newRootIdBytes = new byte[2];
            newRootIdBytes[0] = (byte) ((newRootId >> 8) & 0xFF);
            newRootIdBytes[1] = (byte) (newRootId & 0xFF);

            // update the root id in metadata block
            BlockNode metadataBlock = blocks.get(0);
            metadataBlock.write_data(2, newRootIdBytes);
          
        }
    }

    public void insertInParent(T key, int rightChildId, List<Integer> pathTillNode)
    {
        int order = getOrder();
        // handle the root case
        if(pathTillNode.size() ==1)
        {
            int lastNode = pathTillNode.get(0);
          
            InternalNode <T> newRootNode = new InternalNode<>(key,lastNode,rightChildId,typeClass);
            
            int newRootNodeId = blocks.size();
            blocks.add(newRootNode);

            byte [] newRootNodeIdBytes = new byte[2];
            newRootNodeIdBytes[0] = (byte) ((newRootNodeId >> 8) & 0xFF);
            newRootNodeIdBytes[1] = (byte) (newRootNodeId & 0xFF);

            // update the root id in metadata block

            BlockNode metadataBlock = blocks.get(0);
            metadataBlock.write_data(2, newRootNodeIdBytes);
        }

        else
        {
            int currParentId = pathTillNode.get(pathTillNode.size()-2);
            pathTillNode.remove(pathTillNode.size()-1);
            InternalNode<T> currParent = (InternalNode<T>) blocks.get(currParentId);
            if(!isFull(currParentId))
            {
                currParent.insert(key, rightChildId);
            }
            else
            {
                byte [] data = currParent.get_data();
                T [] keys = currParent.getKeys(); // size is order-1
                int [] children = currParent.getChildren(); // size is order

                // print the keys and children in currParent
                
                int left_Child_Id = children[0];
                int right_Child_Id = children[1];
                T leftKey = keys[0];          
                
                InternalNode<T> tempInternalNode = new InternalNode<>(leftKey,left_Child_Id,right_Child_Id,typeClass);
                
                for (int i = 1; i < (order-1); i++) {
                    tempInternalNode.insert(keys[i], children[i+1]);
                }
                tempInternalNode.insert(key, rightChildId);
                
                byte [] nextFreeOffsetBytes = new byte[2];
                nextFreeOffsetBytes[0] = 0;
                nextFreeOffsetBytes[1] = 6;
                currParent.write_data(2, nextFreeOffsetBytes);

                byte [] numEntriesBytes = new byte[2];
                numEntriesBytes[0] = 0;
                numEntriesBytes[1] = 0;
                currParent.write_data(0, numEntriesBytes);

                int firstChildId = tempInternalNode.getChildren()[0];
                byte [] firstChildIdBytes = new byte[2];
                firstChildIdBytes[0] = (byte) ((firstChildId >> 8) & 0xFF);
                firstChildIdBytes[1] = (byte) (firstChildId & 0xFF);

                currParent.write_data(4, firstChildIdBytes);

                T [] tempKeys = tempInternalNode.getKeys(); // size is order
                int [] tempChildren = tempInternalNode.getChildren(); // size is order+1

                for (int i = 0; i < (order/2); i++) {
                    currParent.insert(tempKeys[i], tempChildren[i+1]);
                }
                
                InternalNode<T> rightInternalNode = new InternalNode<>(tempKeys[(order)/2 + 1],tempChildren[(order)/2 + 1],tempChildren[(order)/2 + 2],typeClass);

                for (int i = (order)/2 + 2; i < order; i++) {
                    rightInternalNode.insert(tempKeys[i], tempChildren[i+1]);
                }

                int rightInternalNodeId = blocks.size();
            
                blocks.add(rightInternalNode);

                T keyToInsertInParent = tempKeys[(order)/2];
                insertInParent(keyToInsertInParent,rightInternalNodeId, pathTillNode);
            }
        }    
    }

    public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
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
        else
            return null;
    }

}