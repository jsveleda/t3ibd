/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import ibd.table.record.Record;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Sergio
 */
public class MainTrabalho {

    /*
    auxiliary class that reproduces the contents of a page.
    Each page has enough room for ten records.
    */
    class Block {
        Long pks[] = new Long[10];
    }
    
    
    /*
    Distributes the pks in such a way that the pages become full
    Returns a list of blocks that reproduce the contents of the actual tree pages
    */
    private ArrayList<Block> createFullPages(Table table, List<Long> pks) throws Exception{
        ArrayList<Long> list1 = new ArrayList();
        ArrayList<Long> list2 = new ArrayList();
        
        System.out.println("creating full pages");
        /*the lists are formed in such a way that blocks of ten sorted elements 
        are evenly distributed between the two lists (five elements each)*/
        for (int i = 0; i < pks.size(); i += 10) {
            list1.add(pks.get(i));
            list1.add(pks.get(i+1));
            
            list2.add(pks.get(i + 2));
            list2.add(pks.get(i + 3));
            list2.add(pks.get(i + 4));
            list2.add(pks.get(i + 5));
            list2.add(pks.get(i + 6));
            
            list1.add(pks.get(i+7));
            list1.add(pks.get(i+8));
            list1.add(pks.get(i+9));
            

        }
        
        //when elements from this list are added, the full pages split
        for (int i = 0; i < list1.size(); i++) {
            table.addRecord(list1.get(i), "person with id = "+list1.get(i));
            
        }

        
        //when elements from this list are added, the half full pages become full
        for (int i = 0; i < list2.size(); i++) {
            table.addRecord(list2.get(i), "person with id = "+list2.get(i));
        }
        
        /*the pks are put in blocks of ten elements each.
        This reproduces the actual page layout after the elements are added to the tree:
        The first block corresponds to the first page, the second block corresponds to the second page, and so on...
        */
        ArrayList<Block> blocks = new ArrayList<>();
        for (int i = 0; i < pks.size() - 10; i += 10) {
            Block b = new Block();
            for (int index = 0; index < 10; index++) {
                b.pks[index] = pks.get(i + index);
            }
            blocks.add(b);
        }
        return blocks;
    }

    /*
    removes one record from a table and updates lists that controls which pks were added and deleted
    */
    private void removeRecord(Table table, Long pk, List<Long> addedPks, List<Long> deletedPks) throws Exception{
        table.removeRecord(pk);
        addedPks.remove(pk);
        deletedPks.add(pk);
    }
    
    /*
    removes many records from a table and updates lists that controls which pks were added and deleted
    */
    private void removeManyRecords(Table table, Block b, List<Long> addedPks, List<Long> deletedPks) throws Exception{
        for (int i = 1; i <= 8; i++) {
            removeRecord(table, b.pks[i], addedPks, deletedPks);
        }
    }
    
    /*
    Verifies if the added pks are indeed in the tree
    */
    private boolean validateAddedPks(Table table, List<Long> addedPks) throws Exception{
    boolean ok = true;
        for (int i = 0; i < addedPks.size(); i++) {
            Long pk = addedPks.get(i);
            Record rec = table.getRecord(pk);
            if (rec==null){
                System.out.println("Error. Should find pk = "+pk);
                ok = false;
            }
        }
        return ok;
    }
    
    /*
    Verifies if the deleted pks were indeed removed from the tree
    */
    private boolean validateDeletedPks(Table table, List<Long> deletedPks) throws Exception{
    boolean ok = true;
        for (int i = 0; i < deletedPks.size(); i++) {
            Long pk = deletedPks.get(i);
            Record rec = table.getRecord(pk);
            if (rec!=null){
                System.out.println("Error. Should not find pk = "+pk);
                ok = false;
            }
        }
    return ok;
    }
    
    /*
    verifies if the 'merging with the left sibling node' strategy was properly implemented
    */
    public void testLeftSiblingMergeStrategy() throws Exception {
        Table table = Directory.getTable("c:\\teste\\ibd", "aluno", 4096, true);

        System.out.println("TEST MERGING WITH LEFT SIBLING");
        
        //contain all added and deleted pks. Used to validate the contents of the tree
        ArrayList<Long> addedPks = new ArrayList<>();
        ArrayList<Long> deletedPks = new ArrayList<>();
        
        //creates a consecutive array of primary keys
        List<Long> pks = new ArrayList();
        for (long i = 0; i < 10000; i++) {
            pks.add(i);
            addedPks.add(i);
        }
        
        //creates records with the given primary keys. 
        //the records are arranged in a way to force the pages to become full
        ArrayList<Block> blocks = createFullPages(table, pks);

        //process pages from right to left, removing many records from each, until they become deficient
        //A deficient page can be merged with the left sibling
        //only works with left sibling merge, since the pages at the right are still full
        System.out.println("removing many records from all blocks");
        for (int i = 0; i < blocks.size()-1; i += 2) {
            Block b = blocks.get(i);
            removeManyRecords(table, b, addedPks, deletedPks);
            Block b1 = blocks.get(i+1);
            removeManyRecords(table, b1, addedPks, deletedPks);
        }

        
        //creates a new list of pks whose values are larger than the larger indexed value
        //it means new pages will be required
        //if the merge was successfull, the deleted pages can be used to prevent the file from growing
        pks = new ArrayList();
        for (long i = 10000; i < 15000; i++) {
            pks.add(i);
        }
        
        System.out.println("inserting new elements");
        for (int i = 0; i < pks.size();i++){
            Long pk = pks.get(i);
            table.addRecord(pk, "person with id = "+pk);
            addedPks.add(pk);
        }

        table.flushDB();
        
        //Validates the contents of the tree
        if (validateAddedPks(table, addedPks)){
            System.out.println("added PKs OK");
        }
        if (validateDeletedPks(table, deletedPks)){
            System.out.println("deleted PKs OK");
        }
        
        //prints the number of pages created
        table.printStats();
        
        ArrayList<Long> addedPks2 = new ArrayList<>();
        Iterator<Record>  it = table.iterator();
        while (it.hasNext()){
            Record rec = it.next();
            addedPks2.add(rec.getPrimaryKey());
        }
        if (addedPks.size()!=addedPks2.size())
            System.out.println("erro: "+addedPks.size()+" - "+addedPks2.size());
    }
    
    /*
    verifies if the 'merging with the right sibling node' strategy was properly implemented
    */
    public void testRightSiblingMergeStrategy() throws Exception {
        Table table = Directory.getTable("c:\\teste\\ibd", "aluno", 4096, true);

        System.out.println("TEST MERGING WITH RIGHT SIBLING");
        
        //contain all added and deleted pks. Used to validate the contents of the tree
        ArrayList<Long> addedPks = new ArrayList<>();
        ArrayList<Long> deletedPks = new ArrayList<>();
        
        //creates a consecutive array of primary keys
        List<Long> pks = new ArrayList();
        for (long i = 0; i < 10000; i++) {
            pks.add(i);
            addedPks.add(i);
        }
        
        //creates records with the given primary keys. 
        //the records are arranged in a way to force the pages to become full
        ArrayList<Block> blocks = createFullPages(table, pks);

        //process pages from right to left. 
        //the processed pages have records removed, until they become deficient
        //A deficient page can be merged with the right sibling
        //only works with right sibling merge, since the pages at the left are still full
        System.out.println("removing many records from all blocks");
        for (int i = blocks.size()-1; i>=0;i-=1) {
            Block b = blocks.get(i);
            removeManyRecords(table, b, addedPks, deletedPks);
        }

        
        //creates a new list of pks whose values are larger than the larger indexed value
        //it means new pages will be required
        //if the merge was successfull, the deleted pages can be used to prevent the file from growing
        pks = new ArrayList();
        for (long i = 10000; i < 15000; i++) {
            pks.add(i);
        }
        
        System.out.println("inserting new elements");
        for (int i = 0; i < pks.size();i++){
            Long pk = pks.get(i);
            table.addRecord(pk, "person with id = "+pk);
            addedPks.add(pk);
        }

        table.flushDB();
        
        //Validates the contents of the tree
        if (validateAddedPks(table, addedPks)){
            System.out.println("added PKs OK");
        }
        if (validateDeletedPks(table, deletedPks)){
            System.out.println("deleted PKs OK");
        }
        
        //prints the number of pages created
        table.printStats();
        
        ArrayList<Long> addedPks2 = new ArrayList<>();
        Iterator<Record>  it = table.iterator();
        while (it.hasNext()){
            Record rec = it.next();
            addedPks2.add(rec.getPrimaryKey());
        }
        if (addedPks.size()!=addedPks2.size())
            System.out.println("erro: "+addedPks.size()+" - "+addedPks2.size());
    }



    /*
    verifies if the 'borrowing from a sibling leaf node' strategy was properly implemented
    */
    public void testBorrowFromSiblingStrategy() throws Exception {
        Table table = Directory.getTable("c:\\teste\\ibd", "aluno", 4096, true);

        System.out.println("TEST BORROW FROM SIBLING");
        
        //contain all added and deleted pks. Used to validate the contents of the tree
        ArrayList<Long> addedPks = new ArrayList<>();
        ArrayList<Long> deletedPks = new ArrayList<>();
        
        //creates a consecutive array of even primary keys (0,2,4,6,...).
        //the odd values will be inserted later (1,3,5,7,9)
        List<Long> pks = new ArrayList();
        for (long i = 0; i < 10000; i++) {
            if (i%2==0){
                pks.add(i);
                addedPks.add(i);
            }
        }
        
        //creates records with the even primary keys.
        //the records are arranged in a way to force the pages to become full
        ArrayList<Block> blocks = createFullPages(table, pks);

        System.out.println("removing many records from  elements from each interleaved pages");
        //process pages from left to right, interleaving them. 
        //the processed pages have records removed, until they become deficient
        //a deficient page can borrow records from either the left or the right siblings.
        for (int i = 0; i < blocks.size(); i += 2) {
            Block b = blocks.get(i);
            removeManyRecords(table, b, addedPks, deletedPks);
        }

        //process pages not visited during the previous step.
        //if no borrowing strategy was used, those pages will be full
        //adds records with unused pks (odd values) inside those pages
        //if those pages did not lend records to deficient siblings, a split will occur, and the file will grow (bad)
        System.out.println("inserts new elements into full blocks");
        for (int i = 1; i < blocks.size(); i += 2) {
            Block b = blocks.get(i);
            //since the pages contain even elements only, we add 1 to obtain an odd value, which is not indexed
            Long pk = b.pks[0]+1;
            table.addRecord(pk, "person with id = "+pk);
            addedPks.add(pk);
        }

        table.flushDB();
        
        //Validates the contents of the tree
        if (validateAddedPks(table, addedPks)){
            System.out.println("added PKs OK");
        }
        if (validateDeletedPks(table, deletedPks)){
            System.out.println("deleted PKs OK");
        }
    
        //prints the number of pages created
        table.printStats();
        
        ArrayList<Long> addedPks2 = new ArrayList<>();
        Iterator<Record>  it = table.iterator();
        while (it.hasNext()){
            Record rec = it.next();
            addedPks2.add(rec.getPrimaryKey());
        }
        if (addedPks.size()!=addedPks2.size())
            System.out.println("erro: "+addedPks.size()+" - "+addedPks2.size());
        
    }

    
    
    public static void main(String[] args) throws Exception {

        MainTrabalho main = new MainTrabalho();

        //main.testBorrowFromSiblingStrategy();
        //main.testLeftSiblingMergeStrategy();
        main.testRightSiblingMergeStrategy();
        

    }

}
