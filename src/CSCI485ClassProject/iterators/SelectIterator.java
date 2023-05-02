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
            System.out.println("made leftCur");
        }
        else if (predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS)
        {
            // make generic cursor to traverse by pks
            leftCursor = recordsImpl.openCursor(tableName, Cursor.Mode.READ);
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
            {
                hasReadLeftFirst = true;
                return recordsImpl.getFirst(leftCursor);
            }

        }
        else if (cp.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS)
        {
            Record r = null;
            if (!hasReadLeftFirst)
            {
                System.out.println("Entered two block");
                hasReadLeftFirst = true;
                r = recordsImpl.getFirst(leftCursor);
            }
            else {
                r = recordsImpl.getNext(leftCursor);
            }
            while (r != null)
            {
                // compare values and such
                AttributeType leftType = cp.getLeftHandSideAttrType();
                AttributeType rightType = cp.getRightHandSideAttrType();
                if (leftType != rightType)
                {
                    System.out.println("Types don't match");
                    return null;
                }
                // get values
                Object leftVal = r.getValueForGivenAttrName(cp.getLeftHandSideAttrName());
                Object rightVal = r.getValueForGivenAttrName(cp.getRightHandSideAttrName());
                System.out.println("leftVal: " + leftVal);
                System.out.println("rightVal pre Algebra: " + rightVal);
                if (leftType == AttributeType.INT)
                {
                    // apply algebraic
                    if (cp.getRightHandSideOperator() == AlgebraicOperator.PRODUCT)
                    {
                        System.out.println("entered product check");
                        long val1 = ComparisonUtils.convertObjectToLong(rightVal);
                        long val2 = ComparisonUtils.convertObjectToLong(cp.getRightHandSideValue());

                        rightVal = (Object)(val1 * val2);
                        System.out.println("rightVal: " + rightVal);
                    }
                    if (ComparisonUtils.compareTwoINT(leftVal, rightVal, cp.getOperator()))
                    {
                        System.out.println("returning");
                        return r;
                    }
                }
                r = recordsImpl.getNext(leftCursor);
            }
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
