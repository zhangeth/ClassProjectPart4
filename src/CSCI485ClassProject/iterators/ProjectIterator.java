package CSCI485ClassProject.iterators;


import CSCI485ClassProject.*;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.*;

import CSCI485ClassProject.utils.ComparisonUtils;
import CSCI485ClassProject.utils.IndexesUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class ProjectIterator extends Iterator {
    private final Database db;
    private Transaction tx;
    private RecordsImpl recordsImpl;
    private String attrName;
    private String tableName;
    private List<String> duplicateAttrPath;
    private boolean isDuplicateFree;
    private boolean isInitialized = false;
    private boolean isUsingIterator = false;
    private Iterator iterator;
    private Cursor cursor;

    // for duplicates, if checking for duplicates is enabled
    DirectorySubspace dupSubspace;

    private void initDupeSubspace()
    {}

    public ProjectIterator(){
        db = FDBHelper.initialization();
        recordsImpl = new RecordsImpl();
    }
    public ProjectIterator(String attrName, boolean isDuplicateFree, Database db)
    {
        this.db = db;
        this.attrName = attrName;
        this.isDuplicateFree = isDuplicateFree;
        recordsImpl = new RecordsImpl();
        tx = FDBHelper.openTransaction(db);
    }

    // public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx)
    public ProjectIterator(String tableName, String attrName, boolean isDuplicateFree, Database db)
    {
        this(attrName, isDuplicateFree, db);
        this.tableName = tableName;
        // make cursor on attrName, don't have to worry about index, just simple Cursor, and each record returned, get rid of other attrs
        cursor = recordsImpl.openCursor(tableName, Cursor.Mode.READ);
        // want to make subdirectory of attribute name, duplicate, add to duplicate table, and check it records are traversed
        if (isDuplicateFree)
        {
            // first, just hardcode subdirectory
            duplicateAttrPath = new ArrayList<>();
            // get TableMetadata
            TableMetadata tbm = recordsImpl.getTableMetadataByTableName(tx, tableName);
            RecordsTransformer rt = new RecordsTransformer(tableName, tbm);
            // make duplicate store under main data subdirectory
            for (String s : rt.getTableRecordPath())
                duplicateAttrPath.add(s);
            String dirStr = attrName + "Duplicates";
            duplicateAttrPath.add(dirStr);
            // needs own tx to write to fdb
            Transaction createTx = FDBHelper.openTransaction(db);
            dupSubspace = FDBHelper.createOrOpenSubspace(createTx, duplicateAttrPath);
            //FDBHelper.commitTransaction(createTx);
            System.out.println("successfully created dupe subspace");
            if (FDBHelper.doesSubdirectoryExists(createTx, duplicateAttrPath))
                System.out.println("exists");
        }
    }
    public ProjectIterator(Iterator iterator, String attrName, boolean isDuplicateFree, Database db)
    {
        this(attrName, isDuplicateFree, db);
        // use iterator instead of tablename
        //this.iterator =

    }

    // idea: use Cursor to iterate over records, make "subrecord" of record, and return that
    public Record next() {
        Record r = null;
        if (!isInitialized)
        {
            r = recordsImpl.getFirst(cursor);
            isInitialized = true;
        }
        else
            r = recordsImpl.getNext(cursor);

        while (r != null)
        {
            Object val = r.getValueForGivenAttrName(attrName);
            if (val != null)
            {
                // want to add value as key in duplicate subspace, if dupes are enabled
                if (isDuplicateFree)
                {
                    Tuple keyTuple = new Tuple();
                    keyTuple = keyTuple.addObject(val);
                    System.out.println("val: " + val.toString());
                    Transaction tx1 = FDBHelper.openTransaction(db);
                    // check if exists in subspace first
                    if (FDBHelper.getCertainKeyValuePairInSubdirectory(dupSubspace, tx1, keyTuple, duplicateAttrPath) == null)
                    {
                        // make dupe entry
                        Tuple valueTuple = new Tuple();
                        FDBKVPair kvPair = new FDBKVPair(duplicateAttrPath, keyTuple, valueTuple);

                        FDBHelper.setFDBKVPair(dupSubspace, tx1, kvPair);
                        FDBHelper.commitTransaction(tx1);
                    }
                    else {
                        tx1.close();
                        r = recordsImpl.getNext(cursor);
                        continue;
                    }
                }
                Record ans = new Record();
                ans.setAttrNameAndValue(attrName, val);
                return ans;
            }
            r = recordsImpl.getNext(cursor);
        }
        return null;
    }
    public List<String> getDuplicateAttrPath()
    {
        return duplicateAttrPath;
    }

    public void commit()
    {
        FDBHelper.commitTransaction(tx);
    }

    public void abort()
    {
        FDBHelper.abortTransaction(tx);
    }

}
