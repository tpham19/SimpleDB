package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple child1Tuple;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.child1Tuple = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException, 
            TransactionAbortedException {
        // some code goes here
        child1Tuple = null;
        child1.open();
        child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child1Tuple = null;
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (true) {
            if (child1Tuple == null) {
                if (child1.hasNext()) {
                    child1Tuple = child1.next();
                    // Move the child2 iterator back to the start
                    child2.rewind();
                } else {
                    // The child1 iterator has no more tuples to iterate
                    return null;
                }
            }
            while (child2.hasNext()) {
                Tuple child2Tuple = child2.next();
                if (p.filter(child1Tuple, child2Tuple)) {
                    Tuple result = new Tuple(this.getTupleDesc());
                    TupleDesc child1TupleDesc = child1.getTupleDesc();
                    for (int i = 0; i < result.getTupleDesc().numFields(); i++) {
                        if (i < child1TupleDesc.numFields()) {
                            result.setField(i, child1Tuple.getField(i));
                        } else {
                            result.setField(i, child2Tuple.getField(i - child1TupleDesc.numFields()));
                        }
                    }
                    return result;
                }
            }
            // Reset the child1Tuple tuple pointer
            child1Tuple = null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child1 != children[0]) {
           this.child1 = children[0]; 
        }
        if (this.child2 != children[1]) {
           this.child2 = children[1]; 
        }
    }
    

}
