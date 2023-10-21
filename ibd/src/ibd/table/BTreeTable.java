/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import ibd.index.btree.BPlusTreeFile;
import ibd.index.btree.DictionaryPair;
import ibd.index.btree.Key;
import ibd.index.btree.RowSchema;
import ibd.index.btree.Value;
import ibd.persistent.PersistentPageFile;
import ibd.persistent.cache.Cache;
import ibd.persistent.cache.LRUCache;
import ibd.table.record.Record;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



public class BTreeTable extends Table implements Iterable {

    BPlusTreeFile tree = null;
    RowSchema keyPrototype;
    RowSchema valuePrototype;
    
    PersistentPageFile p;

    public BTreeTable(String folder, String name, int pageSize, boolean override) throws Exception {
        
        //the schema is fixed. The key is a long and the value is the key plus a string column
        keyPrototype = new RowSchema(1);
        keyPrototype.addLong();
        
        valuePrototype = new RowSchema(2);
        valuePrototype.addLong();
        valuePrototype.addString();
        
        //defines the paged file that the BTree will use
        p = new PersistentPageFile(pageSize, Paths.get(folder + "\\" + name), override);
        //LRUCache lru = new LRUCache(5000000, p);
        
        //defines the buffer management to be used, if any.
        Cache lru = defineBufferManagement(p);
        
        //creates a BTree instance using the defined buffer manager, if any
        if (lru!=null)
            tree = new BPlusTreeFile(5, 7, lru, keyPrototype, valuePrototype);
        else tree = new BPlusTreeFile(5, 7, p, keyPrototype, valuePrototype);
    }

    public Cache defineBufferManagement(PersistentPageFile file) {
        //return new MidPointCache(5000000, file);
        //return new ibd.persistent.cache.LRUCache(5000000, file);
        return null;
    }

    @Override
    public Record getRecord(Long primaryKey) throws Exception {

        Key key = new Key(keyPrototype);
        key.setKeys(new Long[]{primaryKey});
        Value value = tree.search(key);

        if (value == null) {
            return null;
        }

        Record rec = new Record(primaryKey);
        rec.setContent((String) value.get(1));

        return rec;
    }

    @Override
    public List<? extends Record> getRecords(Long primaryKey, int comparisonType) throws Exception {

        //comparison type not  working yet
        List<DictionaryPair> values = tree.searchAll();
        List<Record> records = new ArrayList();
        for (DictionaryPair value : values) {
            Value v = value.getValue();
            Long pk = (Long) v.get(0);
            String content = (String) v.get(1);
            Record rec = new Record(pk);
            rec.setContent(content);
            records.add(rec);
        }
        return records;
    }

    @Override
    public Record addRecord(Long primaryKey, String content) throws Exception {

        Key key = new Key(tree.getKeySchema());
        key.setKeys(new Long[]{primaryKey});
        
        Value value = new Value(tree.getValueSchema());
        
        value.set(0, primaryKey);
        value.set(1, content);
        boolean ok = tree.insert(key, value);
        
        if (!ok) return null;

        Record rec = new Record(primaryKey);
        rec.setContent(content);

        return rec;

    }

    @Override
    public Record updateRecord(Long primaryKey, String content) throws Exception {

        
        Key key = new Key(tree.getKeySchema());
        key.setKeys(new Long[]{primaryKey});
        
        Value value = new Value(tree.getValueSchema());
        value.set(0, primaryKey);
        value.set(1, content);
        
        value = tree.update(key, value);

        if (value == null) {
            return null;
        }

        Record rec = new Record(primaryKey);
        rec.setContent((String) value.get(1));

        return rec;

    }

    @Override
    public Record removeRecord(Long primaryKey) throws Exception {

        Key key = new Key(tree.getKeySchema());
        key.setKeys(new Long[]{primaryKey});
        
        Value value = tree.delete(key);
        
        if (value==null) return null;

        Record rec = new Record(primaryKey);
        rec.setContent((String) value.get(1));
        
        return rec;

    }

    @Override
    public void flushDB() throws Exception {

        tree.flush();

    }

    @Override
    public int getRecordsAmount() throws Exception {
        //not implemented yet
        return 0;
    }

    @Override
    public Iterator<Record> iterator() {
        List<DictionaryPair> values = tree.searchAll();
        List<Record> records = new ArrayList();
        for (DictionaryPair value : values) {
            Value v = value.getValue();
            Long pk = (Long) v.get(0);
            String content = (String) v.get(1);
            Record rec = new Record(pk);
            rec.setContent(content);
            records.add(rec);
        }
        return records.iterator();
    }

    @Override
    public void printStats() throws Exception {
        System.out.println("largest used page id:"+p.getNextPageID());
    }

}
