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
    private Transaction dupeTx;
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
    {
        // first, make unique subdir
        duplicateAttrPath = new ArrayList<>(); duplicateAttrPath.add(this.toString());
        String dirStr = attrName + "Duplicates";
        duplicateAttrPath.add(dirStr);
        dupSubspace = FDBHelper.createOrOpenSubspace(dupeTx, duplicateAttrPath);
        //FDBHelper.commitTransaction(createTx);
        System.out.println("successfully created dupe subspace");
        if (FDBHelper.doesSubdirectoryExists(dupeTx, duplicateAttrPath))
            System.out.println("exists");
    }

    public ProjectIterator(){
        db = FDBHelper.initialization();
        recordsImpl = new RecordsImpl();
    }
    public ProjectIterator(String attrName, boolean isDuplicateFree, Database db)
    {
        this.db = db;
        this.attrName = attrName;
        this.isDuplicateFree = isDuplicateFree;
        if (isDuplicateFree)
        {
            dupeTx = FDBHelper.openTransaction(db);
            initDupeSubspace();
        }
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

    }
    public ProjectIterator(Iterator iterator, String attrName, boolean isDuplicateFree, Database db)
    {
        this(attrName, isDuplicateFree, db);
        this.iterator = iterator;
        isUsingIterator = true;
    }

    // idea: use Cursor to iterate over records, make "subrecord" of record, and return that
    // if using cursor, do the same but call iterator.next
    public Record next() {
        Record r = null;
        if (!isInitialized && !isUsingIterator)
        {
            r = (isUsingIterator) ? iterator.next() : recordsImpl.getFirst(cursor);
            isInitialized = true;
        }
        else
        {
            r = (isUsingIterator) ? iterator.next() : recordsImpl.getNext(cursor);
        }

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

                    // check if exists in subspace first
                    if (FDBHelper.getCertainKeyValuePairInSubdirectory(dupSubspace, dupeTx, keyTuple, duplicateAttrPath) == null)
                    {
                        // make dupe entry
                        Tuple valueTuple = new Tuple();
                        FDBKVPair kvPair = new FDBKVPair(duplicateAttrPath, keyTuple, valueTuple);

                        FDBHelper.setFDBKVPair(dupSubspace, dupeTx, kvPair);
                        //FDBHelper.commitTransaction(tx1);
                    }
                    else {
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
    public Transaction getDupeTransaction()
    {
        return dupeTx;
    }

    public void commit()
    {
        if (isDuplicateFree)
            FDBHelper.abortTransaction(dupeTx);
        FDBHelper.commitTransaction(tx);
    }

    public void abort()
    {
        if (isDuplicateFree)
            FDBHelper.abortTransaction(dupeTx);
        FDBHelper.abortTransaction(tx);
    }

}
