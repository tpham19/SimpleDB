package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> groupByCount;
    private Field fieldOfGroupBy;
    private String fieldNameOfGroupBy;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.toString().equals("count")) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupByCount = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            // For no grouping
            fieldOfGroupBy = new IntField(-1);
            fieldNameOfGroupBy = null;
        } else {
            fieldOfGroupBy = tup.getField(gbfield);
            fieldNameOfGroupBy = tup.getTupleDesc().getFieldName(gbfield);
        }
        if (groupByCount.containsKey(fieldOfGroupBy)) {
            groupByCount.put(fieldOfGroupBy, groupByCount.get(fieldOfGroupBy) + 1);
        } else {
            groupByCount.put(fieldOfGroupBy, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc resultingTupleDescriptor;
        if (gbfield == NO_GROUPING) {
            resultingTupleDescriptor = new TupleDesc(new Type[]{ Type.INT_TYPE }, new String[]{ what.toString() });
        } else {
            resultingTupleDescriptor = new TupleDesc(new Type[]{ gbfieldtype, Type.INT_TYPE }, new String[]{ fieldNameOfGroupBy, what.toString() });
        }
        ArrayList<Tuple> resultingTuples = new ArrayList<Tuple>();
        for (Field groupByField : groupByCount.keySet()) {
            Tuple resultingTuple = new Tuple(resultingTupleDescriptor);
            int aggregateValue = groupByCount.get(groupByField);
            if (gbfield == NO_GROUPING) {
                resultingTuple.setField(0, new IntField(aggregateValue));
            } else {
                resultingTuple.setField(0, groupByField);
                resultingTuple.setField(1, new IntField(aggregateValue));
            }
            resultingTuples.add(resultingTuple);
        }
        return new TupleIterator(resultingTupleDescriptor, resultingTuples);
    }

}
