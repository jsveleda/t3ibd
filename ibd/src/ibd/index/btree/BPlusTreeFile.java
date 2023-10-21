/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index.btree;

import ibd.persistent.AbstractExternalizablePage;
import ibd.persistent.Page;
import ibd.persistent.PageFile;
import ibd.persistent.PageSerialization;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class BPlusTreeFile extends BPlusTree implements PageSerialization {

    int rootID = -1;
    int firstLeafID = -1;

    /**
     * Page codes.
     */
    private static final int EMPTY_PAGE = 0;
    private static final int INTERNAL_NODE = 1;
    private static final int LEAF_NODE = 2;

    /**
     * The file storing the entries of this index.
     */
    protected PageFile<Node> file = null;

    /**
     * True if this index is already initialized.
     */
    protected boolean init = false;

    /**
     * Constructor
     *
     * @param m: the order (fanout) of the B+ tree
     * @param leafM: the number of values stores at the leaf nodes
     * @param keySchema: the schema of the keys.
     * @param valueSchema: the schema of the indexed values.
     *
     */
    public BPlusTreeFile(int m, int leafM, RowSchema keySchema, RowSchema valueSchema) {
        super(m, leafM, keySchema, valueSchema);
    }

    /**
     * Constructor
     *
     * @param m: the order (fanout) of the B+ tree
     * @param leafM: the number of values stores at the leaft nodes
     * @pagefile: the file storing the entries of this index.
     * @param keySchema: the schema of the keys.
     * @param valueSchema: the schema of the indexed values.
     */
    public BPlusTreeFile(int m, int leafM, PageFile pagefile, RowSchema keySchema, RowSchema valueSchema) throws InstantiationException, IllegalAccessException {
        super(m, leafM, keySchema, valueSchema);
        this.file = pagefile;

        pagefile.setPageSerialization(this);

        //parameter Class<Key> keyClass not used anymore
        //Key key = keyClass.newInstance();
        Key key = new Key(keySchema);
        int keySize = key.getSizeInBytes();

        //parameter Class<Value> valueClass not used anymore
        //Value value = valueClass.newInstance();
        Value value = new Value(valueSchema);
        int valueSize = value.getSizeInBytes();
        int pageSize = pagefile.getPageSize();

        //use the key size to determine the order of the internal nodes. This overrides the value passed to the constructor.
        InternalNode in = new InternalNode(this);
        this.m = in.findOutOptimalDegree(pageSize, keySize);

        //use the key and value size to determine the order of the leaf nodes. This overrides the value passed to the constructor.
        LeafNode ln = new LeafNode(this);
        this.leafM = ln.findOutOptimalDegree(pageSize, keySize, valueSize);

    }

    /*
    * Initialize the database file.
    * The created header is used only if the file is new. The already existing header is used otherwise.
    * the header contains the page size, the internal node order, the leaf node size and the ids of the root and the first leaf node
     */
    protected void init() {
        if (!init) {
            file.initialize(createHeader(file.getPageSize(), m, leafM, rootID, firstLeafID, keySchema, valueSchema));
            init = true;
        }
    }

    /**
     * Creates a header for this index structure which is an instance of
     * {@link TreeIndexHeader}. Subclasses may need to overwrite this method if
     * they need a more specialized header.
     *
     * @return a new header for this index structure
     */
    protected TreeIndexHeader createHeader(int pageSize, int dirCapacity, int leafCapacity, int rootID, int firstLeafID, RowSchema keySchema, RowSchema valueSchema) {
        return new TreeIndexHeader(pageSize, dirCapacity, leafCapacity, rootID, firstLeafID, keySchema, valueSchema);
    }

    public int getDirCapacity() {
        return m;
        //return ((TreeIndexHeader) file.getHeader()).getDirCapacity();
    }

    public int getLeafCapacity() {
        return leafM;
        //return ((TreeIndexHeader) file.getHeader()).getLeafCapacity();
    }

    @Override
    public RowSchema getKeySchema() {
        if (keySchema != null) {
            return keySchema;
        }
        return ((TreeIndexHeader) file.getHeader()).getKeySchema();
    }

    @Override
    public RowSchema getValueSchema() {
        if (valueSchema != null) {
            return valueSchema;
        }
        return ((TreeIndexHeader) file.getHeader()).getValueSchema();
    }

    public int getRootID() {
        return ((TreeIndexHeader) file.getHeader()).getRootID();
    }

    private void setRootID(int id) {
        this.rootID = id;
        ((TreeIndexHeader) file.getHeader()).setRootID(id);
    }

    public int getFirstLeafID() {
        return ((TreeIndexHeader) file.getHeader()).getFirstLeafID();
    }

    private void setFirstLeafID(int id) {
        this.firstLeafID = id;
        ((TreeIndexHeader) file.getHeader()).setFirstLeafID(id);
    }

    /**
     * This method starts at the root of the B+ tree and traverses down the tree
     * via key comparisons to the corresponding leaf node that holds 'key'
     * within its dictionary.
     *
     * @param key: the unique key that lies within the dictionary of a LeafNode
     * object
     * @return the LeafNode object that contains the key within its dictionary
     */
    private LeafNode findLeafNode(Key key) {

        // Initialize keys and index variable
        InternalNode root = (InternalNode) getNode(getRootID());
        Key[] keys = root.keys;
        int i;

        // Find next node on path to appropriate leaf node
        for (i = 0; i < root.degree - 1; i++) {
            if (key.compareTo(keys[i]) < 0) {
                break;
            }
        }

        /* Return node if it is a LeafNode object,
		   otherwise repeat the search function a level down */
        Node child = getNode(root.childPointersIDs[i]);
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else {
            return findLeafNode((InternalNode) child, key);
        }
    }

    private LeafNode findLeafNode(InternalNode node, Key key) {

        // Initialize keys and index variable
        Key[] keys = node.keys;
        int i;

        // Find next node on path to appropriate leaf node
        for (i = 0; i < node.degree - 1; i++) {
            if (key.compareTo(keys[i]) < 0) {
                break;
            }
        }

        /* Return node if it is a LeafNode object,
		   otherwise repeat the search function a level down */
        Node childNode = getNode(node.childPointersIDs[i]);
        if (childNode instanceof LeafNode) {
            return (LeafNode) childNode;
        } else {
            return findLeafNode((InternalNode) childNode, key);
        }
    }

    /**
     * This is a simple method that returns the midpoint (or lower bound
     * depending on the context of the method invocation) of the max degree m of
     * the B+ tree.
     *
     * @return (int) midpoint/lower bound
     */
    private int getMidpoint(int m_) {
        return (int) Math.ceil((m_ + 1) / 2.0) - 1;
    }

    /**
     * Copy all entries of a source node into the start of a target node
     *
     * @param target: the target node
     * @param source: the source node
     */
    public void prependAllEntries(InternalNode targetNode, InternalNode sourceNode) {

        //only here the number of keys is the same as the number of points.
        //reason: a key was addded before this method is called
        int nkeys = targetNode.degree;
        for (int i = nkeys - 1; i >= 0; i--) {
            targetNode.keys[i + sourceNode.degree - 1] = targetNode.keys[i];
        }
        for (int i = targetNode.degree - 1; i >= 0; i--) {
            targetNode.childPointers[i + sourceNode.degree] = targetNode.childPointers[i];
            targetNode.childPointersIDs[i + sourceNode.degree] = targetNode.childPointersIDs[i];
        }

        int i = 0;
        for (i = 0; i < sourceNode.degree - 1; i++) {
            Node child = getNode(sourceNode.childPointersIDs[i]);
            targetNode.setPointer(i, child);
            writeNode(child);
            targetNode.setKey(i, sourceNode.keys[i]);
        }
        Node child = getNode(sourceNode.childPointersIDs[i]);
        targetNode.setPointer(i, child);
        writeNode(child);
        targetNode.degree += sourceNode.degree;

    }

    /**
     * Copy all entries of a source node into the end of a target node
     *
     * @param target: the target node
     * @param source: the source node
     */
    private void appendAllEntries(InternalNode targetNode, InternalNode sourceNode) {
        Node child = getNode(sourceNode.childPointersIDs[0]);
        targetNode.appendChildPointer(child);
        writeNode(child);
        for (int i = 1; i < sourceNode.degree; i++) {
            targetNode.appendKey(sourceNode.keys[i - 1]);
            child = getNode(sourceNode.childPointersIDs[i]);
            targetNode.appendChildPointer(child);
            writeNode(child);
        }
    }

    /**
     * Determines if the B+ tree is empty or not.
     *
     * @return a boolean indicating if the B+ tree is empty or not
     */
    private boolean isEmpty() {
        return getFirstLeafID() == -1;
    }

    /**
     * When an insertion into the B+ tree causes an overfull node, this method
     * is called to remedy the issue, i.e. to split the overfull node. This
     * method calls the sub-methods of splitKeys() and splitChildPointers() in
     * order to split the overfull node.
     *
     * @param in: an overfull InternalNode that is to be split
     */
    private void splitInternalNode(InternalNode in) {

        // Acquire parent
        InternalNode parent = null;//in.parent;
        if (parent == null) {
            parent = (InternalNode) getNode(in.getParentID());
        }

        // Split keys and pointers in half and leave the first half in the original node
        int midpoint = getMidpoint(getDirCapacity());
        Key newParentKey = in.keys[midpoint];
        Key[] halfKeys = splitKeys(in.keys, midpoint);
        Integer[] halfPointers = in.splitChildPointersID(midpoint);

        // Change degree of original InternalNode in
        //in.degree = Utils.linearNullSearch(in.childPointers);
        // Create new sibling internal node and add half of keys and pointers
        InternalNode sibling = new InternalNode(getDirCapacity(), halfKeys, halfPointers, this);
        writeNode(sibling);
        for (Integer pointer : halfPointers) {
            if (pointer != null) {
                Node p = getNode(pointer);
                p.setParentNode(sibling);
                writeNode(p);
            }
        }

        // Make internal nodes siblings of one another
        sibling.rightSiblingID = in.rightSiblingID;

        if (sibling.rightSiblingID != -1) {
            sibling.rightSibling = (InternalNode) getNode(sibling.rightSiblingID);
            sibling.rightSibling.leftSibling = sibling;
            sibling.rightSibling.leftSiblingID = sibling.getPageID();
            writeNode(sibling.rightSibling);
        }
        in.rightSibling = sibling;
        in.rightSiblingID = sibling.getPageID();
        sibling.leftSibling = in;
        sibling.leftSiblingID = in.getPageID();

        if (parent == null) {

            // Create new root node and add midpoint key and pointers
            Key[] keys = new Key[getDirCapacity()];
            keys[0] = newParentKey;
            InternalNode newRoot = new InternalNode(getDirCapacity(), keys, this);
            newRoot.appendChildPointer(in);
            newRoot.appendChildPointer(sibling);
            writeNode(newRoot);
            in.setParentID(newRoot.getPageID());
            sibling.setParentID(newRoot.getPageID());
            setRootID(newRoot.getPageID());

        } else {

            // Add key to parent
            parent.addKey(newParentKey);

            // Set up pointer to new sibling
            int pointerIndex = parent.findIndexOfPointer(in) + 1;
            parent.insertChildPointer(sibling, pointerIndex);
            writeNode(parent);
        }
        writeNode(sibling);
        writeNode(in);
    }

    /**
     * This method modifies a list of keys by removing half of the keys and
     * returning them in a separate array. This method is used when splitting an
     * InternalNode object.
     *
     * @param keys: a list of keys
     * @param split: the index where the split is to occur
     * @return array of removed keys
     */
    private Key[] splitKeys(Key[] keys, int split) {

        Key[] halfKeys = new Key[getDirCapacity()];

        // Remove split-indexed value from keys
        keys[split] = null;

        // Copy half of the values into halfKeys while updating original keys
        for (int i = split + 1; i < keys.length; i++) {
            halfKeys[i - split - 1] = keys[i];
            keys[i] = null;
        }

        return halfKeys;
    }

    /*~~~~~~~~~~~~~~~~ API: DELETE, INSERT, SEARCH, UPDATE ~~~~~~~~~~~~~~~~*/
    @Override
    public Value delete(Key key) {
        init();

        if (isEmpty()) {

            /* Flow of execution goes here when B+ tree has no dictionary pairs */
            //System.err.println("Invalid Delete: The B+ tree is currently empty.");
            return null;

        } else {

            LeafNode firstLeaf = (LeafNode) getNode(getFirstLeafID());
            //if (getRootID()!=-1)
            InternalNode root = (InternalNode) getNode(getRootID());

            // Find leaf node that holds the dictionary key
            LeafNode ln = (root == null) ? firstLeaf : findLeafNode(key);

            // Get leaf node and attempt to find index of key to delete
            int dpIndex = Utils.binarySearch(ln.dictionary, ln.numPairs, key, this);

            if (dpIndex < 0) {

                /* Flow of execution goes here when key is absent in B+ tree */
                //System.err.println("Invalid Delete: Key unable to be found: " + key);
                return null;

            } else {

                //saves value to return later
                Value valueToDelete = ln.dictionary[dpIndex].value;

                // Successfully delete the dictionary pair
                ln.delete(dpIndex);

                // Check for deficiencies
                if (ln.isDeficient()) {

                    LeafNode sibling;
                    //InternalNode parent = ln.getParentNode();
                    InternalNode parent = (InternalNode) getNode(ln.getParentID());

                    if (ln.leftSiblingID != -1) {
                        ln.leftSibling = (LeafNode) getNode(ln.leftSiblingID);
                    }

                    if (ln.rightSiblingID != -1) {
                        ln.rightSibling = (LeafNode) getNode(ln.rightSiblingID);
                    }

                    // Borrow: First, check the left sibling, then the right sibling
                    if (ln.leftSibling != null
                            && ln.leftSibling.getParentID() == ln.getParentID()
                            && ln.leftSibling.isLendable()) {

                        sibling = ln.leftSibling;
                        
                        //System.out.println("BORROW FROM LEFT");
                        writeNode(ln);
                        return valueToDelete;


                    } else if (ln.rightSibling != null
                            && ln.rightSibling.getParentID() == ln.getParentID()
                            && ln.rightSibling.isLendable()) {

                        sibling = ln.rightSibling;
                        //System.out.println("BORROW FROM RIGHT");

                        writeNode(ln);
                        return valueToDelete;

                    } // Merge: First, check the left sibling, then the right sibling
                    else if (ln.leftSibling != null
                            && ln.leftSibling.getParentID() == ln.getParentID()
                            && ln.leftSibling.isMergeable()) {

                        sibling = ln.leftSibling;
                        
                        //System.out.println("MERGE WITH LEFT");
                        writeNode(ln);
                        return valueToDelete;


                    } else if (ln.rightSibling != null
                            && ln.rightSibling.getParentID() == ln.getParentID()
                            && ln.rightSibling.isMergeable()) {

                        sibling = ln.rightSibling;

                        //System.out.println("MERGE WITH RIGHT");
                        writeNode(ln);
                        return valueToDelete;
                        
                        //printTree();
                    } else {
                        writeNode(ln);
                    }

                } else if (root == null && firstLeaf.numPairs == 0) {

                    /* Flow of execution goes here when the deleted dictionary
					   pair was the only pair within the tree */
                    // Set first leaf as null to indicate B+ tree is empty
                    //this.firstLeaf = null;
                    setFirstLeafID(-1);
                    deleteNode(firstLeaf);

                } else {
                    writeNode(ln);

                }
                return valueToDelete;
            }
        }
    }

    @Override
    public boolean insert(Key key, Value value) {
        init();
        if (isEmpty()) {

            /* Flow of execution goes here only when first insert takes place */
            // Create leaf node as first node in B plus tree (root is null)
            LeafNode ln = new LeafNode(getLeafCapacity(), new DictionaryPair(key, value, this), this);

            // Set as first leaf node (can be used later for in-order leaf traversal)
            //this.firstLeaf = ln;
            writeNode(ln);
            setFirstLeafID(ln.getPageID());

        } else {

            // Find leaf node to insert into
            LeafNode firstLeaf = (LeafNode) getNode(getFirstLeafID());
            // Find leaf node to insert into
            LeafNode ln = (getRootID() == -1) ? firstLeaf
                    : findLeafNode(key);

            // Perform binary search to find index of key within dictionary
            DictionaryPair[] dps = ln.dictionary;
            int index = Utils.binarySearch(dps, ln.numPairs, key, this);

            //key is already indexed.
            if (index >= 0) {
                return false;
            }

            // Insert into leaf node fails if node becomes overfull
            if (!ln.insert(new DictionaryPair(key, value, this))) {

                // Sort all the dictionary pairs with the included pair to be inserted
                ln.sortDictionary(key, value);

                // Split the sorted pairs into two halves
                int midpoint = getMidpoint(getLeafCapacity());
                DictionaryPair[] halfDict = ln.splitDictionary(midpoint);
                writeNode(ln);

                if (ln.getParentID() >= 0) {
                    ln.setParentNode((InternalNode) getNode(ln.getParentID()));
                }

                if (ln.getParentNode() == null) {

                    /* Flow of execution goes here when there is 1 node in tree */
                    // Create internal node to serve as parent, use dictionary midpoint key
                    Key[] parent_keys = new Key[getDirCapacity()];
                    parent_keys[0] = halfDict[0].key;
                    InternalNode parent = new InternalNode(getDirCapacity(), parent_keys, this);

                    parent.appendChildPointer(ln);
                    writeNode(parent);
                    ln.setParentID(parent.getPageID());
                    writeNode(ln);

                } else {

                    /* Flow of execution goes here when parent exists */
                    // Add new key to parent for proper indexing
                    Key newParentKey = halfDict[0].key;
                    ln.getParentNode().addKey(newParentKey);
                }

                // Create new LeafNode that holds the other half
                LeafNode newLeafNode = new LeafNode(getLeafCapacity(), halfDict, ln.getParentNode());
                writeNode(newLeafNode);

                if (ln.getParentNode() == null) {
                    ln.setParentNode((InternalNode) getNode(ln.getParentID()));
                }

                // Update child pointers of parent node
                int pointerIndex = ln.getParentNode().findIndexOfPointer(ln) + 1;
                ln.getParentNode().insertChildPointer(newLeafNode, pointerIndex);
                writeNode(ln.getParentNode());

                // Make leaf nodes siblings of one another
                newLeafNode.rightSiblingID = ln.rightSiblingID;
                if (newLeafNode.rightSiblingID != -1) {
                    newLeafNode.rightSibling = (LeafNode) getNode(newLeafNode.rightSiblingID);
                    newLeafNode.rightSibling.leftSibling = newLeafNode;
                    newLeafNode.rightSibling.leftSiblingID = newLeafNode.getPageID();
                    writeNode(newLeafNode.rightSibling);
                }
                ln.rightSibling = newLeafNode;
                ln.rightSiblingID = newLeafNode.getPageID();
                newLeafNode.leftSibling = ln;
                newLeafNode.leftSiblingID = ln.getPageID();

                writeNode(newLeafNode);
                writeNode(ln);

                if (getRootID() == -1) {

                    // Set the root of B+ tree to be the parent
                    setRootID(ln.getParentID());

                } else {

                    /* If parent is overfull, repeat the process up the tree,
			   		   until no deficiencies are found */
                    //printTree();
                    InternalNode in = ln.getParentNode();
                    while (in != null) {
                        if (in.isOverfull()) {
                            splitInternalNode(in);
                        } else {
                            break;
                        }
                        if (in.getParentID() != -1) {
                            in = (InternalNode) getNode(in.getParentID());
                        } else {
                            in = null;
                        }
                    }
                }
            } else {
                writeNode(ln);
            }
        }
        return true;
    }

    @Override
    public Value search(Key key) {

        init();

        LeafNode firstLeaf = (LeafNode) getNode(getFirstLeafID());

        // If B+ tree is completely empty, return null
        if (isEmpty()) {
            return null;
        }

        // Find leaf node that holds the dictionary key
        LeafNode ln = (getRootID() == -1) ? firstLeaf
                : findLeafNode(key);

        // Perform binary search to find index of key within dictionary
        DictionaryPair[] dps = ln.dictionary;
        int index = Utils.binarySearch(dps, ln.numPairs, key, this);

        // If index negative, the key doesn't exist in B+ tree
        if (index < 0) {
            return null;
        } else {
            return dps[index].value;
        }
    }

    @Override
    public ArrayList<Value> search(Key lowerBound, Key upperBound) {

        init();

        LeafNode firstLeaf = (LeafNode) getNode(getFirstLeafID());

        // Instantiate Double array to hold values
        ArrayList<Value> values = new ArrayList<>();

        // Iterate through the doubly linked list of leaves
        LeafNode currNode = firstLeaf;
        while (currNode != null) {
            // Iterate through the dictionary of each node
            DictionaryPair dps[] = currNode.dictionary;
            for (DictionaryPair dp : dps) {

                /* Stop searching the dictionary once a null value is encountered
				   as this indicates the end of non-null values */
                if (dp == null) {
                    break;
                }

                // Include value if its key fits within the provided range
                if (lowerBound.compareTo(dp.key) <= 0 && dp.key.compareTo(upperBound) <= 0) {
                    values.add(dp.value);
                }
            }

            /* Update the current node to be the right sibling,
			   leaf traversal is from left to right */
            currNode = currNode.rightSibling;

        }

        return values;
    }

    @Override
    public ArrayList<DictionaryPair> searchAll() {

        init();

        LeafNode firstLeaf = (LeafNode) getNode(getFirstLeafID());

        // Instantiate array to hold values
        ArrayList<DictionaryPair> values = new ArrayList<>();

        // Iterate through the doubly linked list of leaves
        LeafNode currNode = firstLeaf;
        while (currNode != null) {
            // Iterate through the dictionary of each node
            DictionaryPair dps[] = currNode.dictionary;
            for (DictionaryPair dp : dps) {

                /* Stop searching the dictionary once a null value is encountered
				   as this the indicates the end of non-null values */
                if (dp == null) {
                    break;
                }

                values.add(dp);
            }

            /* Update the current node to be the right sibling,
			   leaf traversal is from left to right */
            currNode = (LeafNode) getNode(currNode.rightSiblingID);

        }

        return values;
    }

    @Override
    public ArrayList<Value> partialSearch(Key key) {

        init();

        // Instantiate array to hold values
        ArrayList<Value> values = new ArrayList();

        LeafNode firstLeaf = (LeafNode) getNode(getFirstLeafID());

        InternalNode root = (InternalNode) getNode(getRootID());

        // If B+ tree is completely empty, return null
        if (isEmpty()) {
            return values;
        }

        // Find leaf node that holds the dictionary key
        LeafNode ln = (root == null) ? firstLeaf : findLeafNode(key);

        // Perform binary search to find index of key within dictionary
        DictionaryPair[] dps = ln.dictionary;
        int index = Utils.binarySearch(dps, ln.numPairs, key, this);

        // in this cse, a negative index may simply mean that the key has less levels than the indexed keys. 
        if (index < 0) {
            index = 0;
        }
        {
            for (int i = index; i < dps.length; i++) {
                if (dps[i] != null) {
                    if (key.match(dps[i].key)) {
                        values.add(dps[i].value);
                    } else {
                        return values;
                    }
                }
            }

            // Iterate through the doubly linked list of leaves
            //LeafNode currNode = ln.rightSibling;
            LeafNode currNode = (LeafNode) getNode(ln.rightSiblingID);
            while (currNode != null) {
                // Iterate through the dictionary of each node
                DictionaryPair dps_[] = currNode.dictionary;
                for (DictionaryPair dp : dps_) {

                    /* Stop searching the dictionary once a null value is encountered
				   as this the indicates the end of non-null values */
                    if (dp == null) {
                        break;
                    }

                    // Include value if its key fits within the provided range
                    if (key.match(dp.key)) {
                        values.add(dp.value);
                    } else {
                        return values;
                    }
                }

                /* Update the current node to be the right sibling,
			   leaf traversal is from left to right */
                //currNode = currNode.rightSibling;
                currNode = (LeafNode) getNode(currNode.rightSiblingID);

            }

        }

        return values;
    }

    @Override
    public Value update(Key key, Value value) {

        init();

        LeafNode firstLeaf = (LeafNode) getNode(getFirstLeafID());
        InternalNode root = (InternalNode) getNode(getRootID());

        // If B+ tree is completely empty, return null
        if (isEmpty()) {
            return null;
        }

        // Find leaf node that holds the dictionary key
        LeafNode ln = (root == null) ? firstLeaf : findLeafNode(key);

        // Perform binary search to find index of key within dictionary
        DictionaryPair[] dps = ln.dictionary;
        int index = Utils.binarySearch(dps, ln.numPairs, key, this);

        // If index negative, the key doesn't exist in B+ tree
        if (index < 0) {
            return null;
        } else {
            dps[index].value = value;
            writeNode(ln);
            return dps[index].value;
        }
    }

    /**
     * Closes the backing storage.
     */
    public void close() {
        file.close();
    }

    public void printNode(LeafNode node) {
        if (node == null) {
            return;
        }
        String parent = "";
        if (node.getParentNode() != null) {
            parent = node.getParentNode().toString();
        }
        System.out.println("LF:" + node.toString() + " => " + parent);
    }

    public void printNode(InternalNode node) {
        if (node == null) {
            return;
        }
        String parent = "";
        if (node.getParentNode() != null) {
            parent = node.getParentNode().toString();
        }
        System.out.println("IN:" + node.toString() + " => " + parent);
        for (int x = 0; x < node.childPointers.length; x++) {
            if (node.childPointers != null) {
                if (node.childPointers[x] instanceof InternalNode) {
                    printNode((InternalNode) node.childPointers[x]);
                } else {
                    printNode((LeafNode) node.childPointers[x]);
                }
            }
        }

    }

    /**
     * Returns the node with the specified id.
     *
     * @param nodeID the page id of the node to be returned
     * @return the node with the specified id
     */
    public Node getNode(int nodeID) {
        if (nodeID < 0) {
            return null;
        }
        Node n = file.readPage(nodeID);
        n.setPageID(nodeID);
        return n;
    }

    /**
     * Write a node to the backing storage.
     *
     * @param node Node to write
     */
    protected void writeNode(Node node) {
        file.writePage(node);
    }

    /**
     * Delete a node from the backing storage.
     *
     * @param node Node to delete
     */
    protected void deleteNode(Node node) {
        file.deletePage(node.getPageID());
    }

    /**
     * Puts an Internal/leaf Node page into a outputstream stream.
     *
     * @param oos the outputstream
     * @param page the page that contains the node
     */
    @Override
    public void writePage(DataOutputStream oos, Page page) throws IOException {
        if (page == null) {
            oos.writeInt(EMPTY_PAGE);
        } else {
            if (page instanceof LeafNode) {
                oos.writeInt(LEAF_NODE);
            } else if (page instanceof InternalNode) {
                oos.writeInt(INTERNAL_NODE);
            }

            ((AbstractExternalizablePage) page).writeExternal(oos);
        }

    }

    /**
     * Reads from an inputstream an Internal/Leaf Node page.
     *
     * @param ois the inputstream return the page read from the inputstream
     */
    @Override
    public Page readPage(DataInputStream ois) throws IOException {
        final int type = ois.readInt();
        if (type == EMPTY_PAGE) {
            return null;
        }
//      else if(type != FILLED_PAGE) {
//        throw new IllegalArgumentException("Unknown type: " + type);
//      }
        try {
            AbstractExternalizablePage page = null;
            if (type == INTERNAL_NODE) {
                page = new InternalNode(this);
            } else if (type == LEAF_NODE) {
                page = new LeafNode(this);
            }
            page.readExternal(ois);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error instanciating an index page", e);

        }

    }

    /*
    Flushes the file contents to disk
     */
    public void flush() {
        init();
        file.flush();
    }

}
