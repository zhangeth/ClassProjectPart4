package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;

import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.utils.IndexesUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.List;

public class SelectIterator extends Iterator {

    private final Database db;
    private Transaction tx;
    Cursor leftCursor;
    // used only for two Attr predicates
    Cursor rightCursor;
    ComparisonPredicate cp;
    Iterator.Mode mode;
    RecordsImpl recordsImpl;
    boolean isUsingIndex;
    boolean hasReadLeftFirst = false;
    boolean hasReadRightFirst = false;


    public SelectIterator()
    {
        db = FDBHelper.initialization();
        recordsImpl = new RecordsImpl();
    }

    // select iterator is always read, I believe
    public SelectIterator(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex, Database db)
    {
        this.db = db;
        recordsImpl = new RecordsImpl();
        // make transaction
        tx = FDBHelper.openTransaction(db);
        this.cp = predicate;
        this.mode = mode;
        this.isUsingIndex = isUsingIndex;

        // make appropriate cursor, according to predicate type
        if (predicate.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR)
        {
            String attrName = predicate.getLeftHandSideAttrName();

            leftCursor = recordsImpl.openCursor(tableName, attrName, predicate.getRightHandSideValue(), predicate.getOperator(), Cursor.Mode.READ, isUsingIndex);
                //recordsImpl.get
            // make cursor on the attribute, using index or not, next will just
        }
        else if (predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS)
        {

        }

    }
    public Record next()
    {
        // use only left cursor if one predicate
        if (cp.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR)
        {
            if (hasReadLeftFirst)
                return recordsImpl.getNext(leftCursor);
            else
                return recordsImpl.getFirst(leftCursor);
        }
        else if (cp.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR)
        {

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
