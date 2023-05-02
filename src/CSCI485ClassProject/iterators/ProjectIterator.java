package CSCI485ClassProject.iterators;


import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.*;

import CSCI485ClassProject.utils.ComparisonUtils;
import CSCI485ClassProject.utils.IndexesUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

public class ProjectIterator extends Iterator {
    private final Database db;
    private Transaction tx;
    private RecordsImpl recordsImpl;
    private String attrName;
    private boolean isDuplicateFree;
    private boolean isInitialized = false;
    private Cursor cursor;
    public ProjectIterator(){
        db = FDBHelper.initialization();
        recordsImpl = new RecordsImpl();
    }
    // public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx)
    public ProjectIterator(String tableName, String attrName, boolean isDuplicateFree, Database db)
    {
        this.db = db;
        recordsImpl = new RecordsImpl();
        tx = FDBHelper.openTransaction(db);
        this.attrName = attrName;
        this.isDuplicateFree = isDuplicateFree;
        // make cursor on attrName, don't have to worry about index, just simple Cursor, and each record returned, get rid of other attrs
        cursor = recordsImpl.openCursor(tableName, Cursor.Mode.READ);
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
                Record ans = new Record();
                ans.setAttrNameAndValue(attrName, val);
                return ans;
            }
            r = recordsImpl.getNext(cursor);
        }
        return null;
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
