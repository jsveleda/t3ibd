/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index.btree;

import ibd.table.record.Record;

/**
 * Defines the schema of a row, i.e., the datatype for each field of the row
 * @author Sergio
 */
public class RowSchema {

    Character types[];
    int current = 0;

    public RowSchema(int size) {
        types = new Character[size];
    }

    public void addDataType(char c) {
        switch (c) {
            case 'I':
                addInt();
                return;
            case 'L':
                addLong();
                return;
            case 'S':
                addString();
        }
    }

    public void addInt() {
        types[current] = 'I';
        current++;
    }

    public void addLong() {
        types[current] = 'L';
        current++;
    }

    public void addString() {
        types[current] = 'S';
        current++;
    }

    public int getSize() {
        return types.length;
    }

    public char get(int index) {
        return types[index];
    }

    /*
    * Returns how many bytes each object takes, given its type
    * he current function assumes the string type is fixed as char(100)
    */
    public int getDataSizeInBytes(int type, Object obj) {
        switch (types[type]) {
            case 'I':
                return Integer.BYTES;
            case 'L':
                return Long.BYTES;
            case 'S':
                return Record.RECORD_SIZE;
        }
        return 0;
    }

}
