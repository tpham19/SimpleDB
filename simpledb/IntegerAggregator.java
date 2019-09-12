package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> groupByValue;
    private Map<Field, Integer> groupByCount;
    private Field fieldOfGroupBy;
    private String fieldNameOfGroupBy;


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupByValue = new HashMap<Field, Integer>();
        groupByCount = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
        IntField fieldOfAggregate = (IntField) tup.getField(afield);
        // Update existing entries
        if (groupByCount.containsKey(fieldOfGroupBy)) {
            groupByCount.put(fieldOfGroupBy, groupByCount.get(fieldOfGroupBy) + 1);
            int valueOfGroupBy = groupByValue.get(fieldOfGroupBy);
            int valueOfAggregate = fieldOfAggregate.getValue();
            switch (what) {
                case MIN:
                    groupByValue.put(fieldOfGroupBy, Math.min(valueOfAggregate, valueOfGroupBy));
                    break;
                case MAX:
                    groupByValue.put(fieldOfGroupBy, Math.max(valueOfAggregate, valueOfGroupBy));
                    break;
                case SUM:
                    groupByValue.put(fieldOfGroupBy, valueOfAggregate + valueOfGroupBy);
                    break;
                case AVG:
                    // Sum the value to then calculate the average
                    groupByValue.put(fieldOfGroupBy, valueOfAggregate + valueOfGroupBy);
                    break;
                case COUNT:
                    // Count has been updated
                    break;
                default:
                    break;
            }            
        } else {
            // Store new entries
            groupByValue.put(fieldOfGroupBy, fieldOfAggregate.getValue());
            groupByCount.put(fieldOfGroupBy, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc resultingTupleDescriptor;
        if (gbfield == NO_GROUPING) {
            resultingTupleDescriptor = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { what.toString() });
        } else {
            resultingTupleDescriptor = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE }, new String[] { fieldNameOfGroupBy, what.toString() });
        }
        ArrayList<Tuple> resultingTuples = new ArrayList<Tuple>();
        for (Field groupByField : groupByCount.keySet()) {
            Tuple resultingTuple = new Tuple(resultingTupleDescriptor);
            int aggregateValue = getValueOfAggregate(what, groupByField);
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

    public int getValueOfAggregate(Op what, Field groupByField) {
        switch (what) {
            case MIN:
                return groupByValue.get(groupByField);
            case MAX:
                return groupByValue.get(groupByField);
            case SUM:
                return groupByValue.get(groupByField);
            case AVG:
                return groupByValue.get(groupByField) / groupByCount.get(groupByField);
            case COUNT:
                return groupByCount.get(groupByField);
            default:
                // Exit case for invalid aggregator
                return Integer.MIN_VALUE;
        }         
    }

}
