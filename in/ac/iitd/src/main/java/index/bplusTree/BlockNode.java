package index.bplusTree;

import storage.AbstractBlock;

// Extends AbstractBlock, will be extended by InternalNode and LeafNode
public class BlockNode extends AbstractBlock {

    public BlockNode(byte[] data) {
        super(data);
    }

    public BlockNode() {
        super();
    }

    public int getNumKeys() {
        byte[] numKeysBytes = this.get_data(0, 2);
        return (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);
    }
}