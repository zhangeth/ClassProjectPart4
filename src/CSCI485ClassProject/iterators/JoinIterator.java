package CSCI485ClassProject.iterators;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.Record;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class JoinIterator extends Iterator {
    private final Database db;
    private Transaction tx;
    private Transaction outerTx;

    private Iterator outerIterator;
    private Iterator innerIterator;
    private ComparisonPredicate predicate;
    private Set<String> attrNames;
    private RecordsImpl recordsImpl;
    private List<String> outerPath;
    private DirectorySubspace outerSubspace;

    private int currentOuterIdx;
    private int outerSize;

    private void initOuterSubspace(String attrName)
    {
        // first, make unique subdir
        outerPath = new ArrayList<>(); outerPath.add(this.toString());
        String dirStr = attrName + "Duplicates";
        outerPath.add(dirStr);
        outerSubspace = FDBHelper.createOrOpenSubspace(outerTx, outerPath);
        //FDBHelper.commitTransaction(createTx);
        System.out.println("successfully created dupe subspace");
        if (FDBHelper.doesSubdirectoryExists(outerTx, outerPath))
            System.out.println("exists");
    }
    public JoinIterator()
    {
        db = FDBHelper.initialization();
        recordsImpl = new RecordsImpl();
        tx = FDBHelper.openTransaction(db);
    }
    public JoinIterator(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames, Database db)
    {
        this.db = db;
        this.outerIterator = outerIterator;
        this.innerIterator = innerIterator;
        this.predicate = predicate;
        this.attrNames = attrNames;
        recordsImpl = new RecordsImpl();
        tx = FDBHelper.openTransaction(db);
        outerTx = FDBHelper.openTransaction(db);
        initOuterSubspace(predicate.getLeftHandSideAttrName());
        loopThroughOuter();
        currentOuterIdx = 0;
        // make subspace for value storage

    }
    private void loopThroughOuter()
    {
        String attrName = predicate.getLeftHandSideAttrName();
        outerSize = 0;
        Record r = outerIterator.next();
        while (r != null)
        {
            outerSize++;
            // commit value of the predicate to the thing
            Tuple keyTuple = new Tuple();
            keyTuple = keyTuple.addObject(r.getValueForGivenAttrName(attrName));

            Tuple valueTuple = new Tuple();
            FDBKVPair kvPair = new FDBKVPair(outerPath, keyTuple, valueTuple);
            FDBHelper.setFDBKVPair(outerSubspace, outerTx, kvPair);

            r = outerIterator.next();
        }
    }
    public Record next()
    {
        // now when calling next on inner Iterator, can loop through outer and compare
        Record rightRecord = innerIterator.next();

        while (rightRecord != null)
        {
            Object rightVal = rightRecord.getValueForGivenAttrName(predicate.getRightHandSideAttrName());
            // check type of right Record for applying algebraic, and apply it
            if (predicate.getRightHandSideAttrType() == AttributeType.INT)
            {
                long val1 = ComparisonUtils.convertObjectToLong(rightVal);
                if (predicate.getRightHandSideOperator() == AlgebraicOperator.PRODUCT)
                {
                    long val2 = ComparisonUtils.convertObjectToLong(predicate.getRightHandSideValue());
                    rightVal = (Object)(val1 * val2);
                }

            }
            else if (predicate.getRightHandSideAttrType() == AttributeType.DOUBLE)
            {
                double val1 = (double) rightVal;
                double val2 = (double) predicate.getRightHandSideValue();
                if (predicate.getRightHandSideOperator() == AlgebraicOperator.PRODUCT) {
                    rightVal = (Object) (val1 * val2);
                }
            }
            // want to use index of outerIdx to keep track of what records have been made
            List<FDBKVPair> pairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, outerTx, outerPath);
            // loop through values in outer subspace
            for (; currentOuterIdx < outerSize; currentOuterIdx++)
            {
                Object leftVal = pairs.get(currentOuterIdx).getKey().get(0);
                if (ComparisonUtils.compareTwoObjects(leftVal, rightVal, predicate))
                {
                    // return record, with the correct processing
                    currentOuterIdx++;
                    return rightRecord;
                    // won't there be repeats?
                }
            }
            // if reached end, reset outerIdx
            currentOuterIdx = 0;
            rightRecord = innerIterator.next();
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
