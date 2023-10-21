/*
 * This file is part of ELKI:
 * Environment for Developing KDD-Applications Supported by Index-Structures
 *
 * Copyright (C) 2022
 * ELKI Development Team
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package ibd.index.btree;

import java.nio.ByteBuffer;

import ibd.persistent.PageHeader;

/**
 * Encapsulates the header information of a tree-like index structure. This
 * information is needed for persistent storage.
 *
 * @author Elke Achtert - Adapted by Sergio Mergen
 * @since 0.1
 */
public class TreeIndexHeader extends PageHeader {

    /**
     * The size of this header in Bytes, which is 6 Integer attibutes: 
     * dirCapacity, leafCapacity, rootID, firstLeafID, number of key field columns and number of value field columns
     */
    private static int SIZE = 6 * Integer.BYTES;

    /**
     * The capacity of a directory node (= 1 + maximum number of entries in a
     * directory node).
     */
    int dirCapacity;

    /**
     * The capacity of a leaf node (= 1 + maximum number of entries in a leaf
     * node).
     */
    int leafCapacity;

    /**
     * The schema of the key fields
     */
    RowSchema keySchema;
    
    /**
     * The schema of the value fields
     */
    RowSchema valueSchema;

    /**
     * The page ID of the root node
     */
    private int rootID = 0;

    /**
     * The page ID of the left most leaf node
     */
    private int firstLeafID = 0;

    /**
     * Empty constructor for serialization.
     */
    public TreeIndexHeader(int pageSize) {
        super(pageSize);
    }

    /**
     * Creates a new header with the specified parameters.
     *
     * @param pageSize the size of a page in bytes
     * @param dirCapacity the maximum number of entries in a directory node
     * @param leafCapacity the maximum number of entries in a leaf node
     * @param rootID the page id of the root node
     * @param firstLeafID the page id of the left most leaf node
     * @param keySchema the schema of the key fields
     * @param valueSchema the schema of the value fields 
     */
    public TreeIndexHeader(int pageSize, int dirCapacity, int leafCapacity, int rootID, int firstLeafID, RowSchema keySchema, RowSchema valueSchema) {
        super(pageSize);
        this.dirCapacity = dirCapacity;
        this.leafCapacity = leafCapacity;
        this.rootID = rootID;
        this.firstLeafID = firstLeafID;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    /**
     * Reads the header attributes from the specified file. 
     */
    @Override
    public void readHeader(ByteBuffer buffer) {
        super.readHeader(buffer);
        this.dirCapacity = buffer.getInt();
        this.leafCapacity = buffer.getInt();
        //this.dirMinimum = buffer.getInt();
        //this.leafMinimum = buffer.getInt();

        this.rootID = buffer.getInt();
        this.firstLeafID = buffer.getInt();
        keySchema = new RowSchema(buffer.getInt());
        for (int i = 0; i < keySchema.getSize(); i++) {
            keySchema.addDataType(buffer.getChar());
        }
        valueSchema = new RowSchema(buffer.getInt());
        for (int i = 0; i < valueSchema.getSize(); i++) {
            valueSchema.addDataType(buffer.getChar());
        }

    }

    /**
     * Writes this header attributes to the specified file. 
     */
    @Override
    public void writeHeader(ByteBuffer buffer) {
        super.writeHeader(buffer);
        buffer.putInt(this.dirCapacity); //1
        buffer.putInt(this.leafCapacity); //2
        buffer.putInt(this.rootID); //3
        buffer.putInt(this.firstLeafID);//4
        buffer.putInt(keySchema.getSize());//5
        for (int i = 0; i < keySchema.getSize(); i++) {
            buffer.putChar(keySchema.get(i));
        }
        buffer.putInt(valueSchema.getSize());//6
        for (int i = 0; i < valueSchema.getSize(); i++) {
            buffer.putChar(valueSchema.get(i));
        }
        //buffer.flip();
    }

    /**
     * Returns the capacity of a directory node (= 1 + maximum number of entries
     * in a directory node).
     */
    public int getDirCapacity() {
        return dirCapacity;
    }

    public void setDirCapacity(int dirCapacity) {
        this.dirCapacity = dirCapacity;
    }

    /**
     * Returns the capacity of a leaf node (= 1 + maximum number of entries in a
     * leaf node).
     */
    public int getLeafCapacity() {
        return leafCapacity;
    }

    public void setLeafCapacity(int leafCapacity) {
        this.leafCapacity = leafCapacity;
    }

    public int getRootID() {
        return rootID;
    }

    public int getFirstLeafID() {
        return firstLeafID;
    }

    public void setRootID(int rootID) {
        this.rootID = rootID;
    }

    public void setFirstLeafID(int firstLeafID) {
        this.firstLeafID = firstLeafID;
    }

    public RowSchema getKeySchema() {
        return keySchema;
    }
    
    public RowSchema getValueSchema() {
        return valueSchema;
    }

    /**
     * Returns the size of the header considering all attributes that needs saving. Note, this is only the base size and probably
     * <em>not</em> the overall size of this header, as there may be empty pages
     * to be maintained.
     */
    @Override
    public int size() {
        //each key or value columns takes a single character to represent its data type. 
        return super.size() + SIZE + keySchema.getSize()* Character.BYTES + valueSchema.getSize()*Character.BYTES;
    }

}
