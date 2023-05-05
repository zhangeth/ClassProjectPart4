package CSCI485ClassProject.iterators;

import CSCI485ClassProject.RecordsTransformer;
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

import java.util.*;

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
    private String outerTableName;
    private String innerTableName;
    private Record currRecord;

    private int currentOuterIdx;
    private int outerSize;

    private void initOuterSubspace(String attrName)
    {
        // first, make unique subdir
        outerPath = new ArrayList<>(); outerPath.add(this.toString());
        String dirStr = "outer";
        outerPath.add(dirStr);
        outerSubspace = FDBHelper.createOrOpenSubspace(outerTx, outerPath);
        //FDBHelper.commitTransaction(createTx);
        System.out.println("successfully created outer subspace");
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
        if (innerIterator instanceof SelectIterator)
        {
            // cast to selectIterators for now
            SelectIterator si = (SelectIterator)innerIterator;
            innerTableName = si.getTableName();
            System.out.println("printing innername" + innerTableName);
        }
        if (outerIterator instanceof  SelectIterator)
        {
            SelectIterator si2 = (SelectIterator) outerIterator;
            outerTableName = si2.getTableName();
        }

    }
    private void loopThroughOuter()
    {
        String attrName = predicate.getLeftHandSideAttrName();
        outerSize = 0;
        Record r = outerIterator.next();
        System.out.println("looping for attr: " + predicate.getLeftHandSideAttrName());
        for (Map.Entry e :  r.getMapAttrNameToValue().entrySet())
        {
            System.out.println("key: " + e.getKey() + "val: " + e.getValue());
        }

        while (r != null)
        {
            outerSize++;
            // commit value of the predicate to the thing
            Tuple keyTuple = new Tuple();
            keyTuple = keyTuple.addObject(r.getValueForGivenAttrName(attrName));
            // want to add pk as value, so can fetch when matched
            Tuple valueTuple = new Tuple();
            // could alternate thingies, and when there's a duplicate, then you know
            for (Map.Entry<String, Record.Value> entry : r.getMapAttrNameToValue().entrySet()) {
                valueTuple = valueTuple.add(entry.getKey());
                valueTuple = valueTuple.addObject(entry.getValue().getValue());
            }

            //valueTuple = valueTuple.addObject(r);
            FDBKVPair kvPair = new FDBKVPair(outerPath, keyTuple, valueTuple);
            FDBHelper.setFDBKVPair(outerSubspace, outerTx, kvPair);

            r = outerIterator.next();
        }
    }

    private Object applyAlgebraic(Object rightVal)
    {
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
        return rightVal;
    }

    public Record next()
    {
        List<FDBKVPair> pairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, outerTx, outerPath);
        Record rightRecord;
        // now when calling next on inner Iterator, can loop through outer and compare, inner is dept
        if (currentOuterIdx >= pairs.size() || currRecord == null) {
            rightRecord = innerIterator.next();
            currRecord = rightRecord;
        }
        else {
            rightRecord = currRecord;
        }

        while (rightRecord != null)
        {
            Object rightVal = rightRecord.getValueForGivenAttrName(predicate.getRightHandSideAttrName());
            // check type of right Record for applying algebraic, and apply it
            rightVal = applyAlgebraic(rightVal);
            // want to use index of outerIdx to keep track of what records have been made

            for (int idx = 0; idx < pairs.size(); idx++)
            {
                FDBKVPair p = pairs.get(idx);
                Object leftVal = p.getKey().get(0);
                //System.out.println("leftVal: " + leftVal);
                if (ComparisonUtils.compareTwoObjects(leftVal, rightVal, predicate))
                {
                    Record res = new Record();
                    List<Object> valueObjs = p.getValue().getItems();
                    // add the value alternating thingies
                    for (int i = 0; i < valueObjs.size() - 1; i++)
                    {
                        res.setAttrNameAndValue((String)valueObjs.get(i), valueObjs.get(++i));
                    }

                    HashMap<String, Record.Value> currMap = res.getMapAttrNameToValue();
                    // now add right Record
                    for (Map.Entry<String, Record.Value> entry : rightRecord.getMapAttrNameToValue().entrySet()) {
                        String key = entry.getKey();
                        if (currMap.containsKey(key))
                        {
                            res.updateJoinedRecord(key, outerTableName);
                            String str = innerTableName + "." + key;
                            res.setAttrNameAndValue(str, entry.getValue().getValue());
                        }
                        else {
                            res.setAttrNameAndValue(key, entry.getValue().getValue());
                        }
                    }

                    currentOuterIdx = idx;
                    System.out.println("Matched employee: " + res.getValueForGivenAttrName("SSN") + " with: " + res.getValueForGivenAttrName("Employee.DNO"));
                    return res;
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
