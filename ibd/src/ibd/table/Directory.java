/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import java.util.Hashtable;

/**
 *
 * @author Sergio
 */
public class Directory {

    static Hashtable<String, Table> tables = new Hashtable<String, Table>();
    static Hashtable<String, Table> tables2 = new Hashtable<String, Table>();

    public static Table getTable(String folder, String name, int pageSize, boolean override) throws Exception {
        String key = folder + "\\" + name;
        Table t = tables2.get(key);
        if (t != null && !override) {
            return t;
        }
        //t = new HeapTable(folder, name, pageSize, override);
        t = new BTreeTable(folder, name, pageSize, override);
        //t = new ChainedBlocksTable1(folder, name, pageSize, override);
        t.tableKey = key;
        tables2.put(key, t);
        return t;

    }

    public static Table getTable(String key, int pageSize, boolean override) throws Exception {
        String folder = getTableFolder(key);
        String file = getTableFile(key);
        return Directory.getTable(folder, file, pageSize, override);
    }

    public static String getTableFolder(String key) {

        return key.substring(0, key.lastIndexOf("\\"));

    }

    public static String getTableFile(String key) {

        return key.substring(key.lastIndexOf("\\"), key.length());

    }

}
