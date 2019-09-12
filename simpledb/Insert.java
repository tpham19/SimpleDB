package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private boolean beenCalled;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        TupleDesc tableTupleDescriptor = Database.getCatalog().getTupleDesc(tableId);
        if (!child.getTupleDesc().equals(tableTupleDescriptor)) {
            throw new DbException("The tuple descriptor of child differs from the table into which we are to insert.");
        }
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.beenCalled = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "NumberOfInsertedRecords" });
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
        beenCalled = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // Check if the insert operator has been called
        if (beenCalled) {
            return null;
        }
        int numberOfInsertedRecords = 0;
        while (child.hasNext()) {
            Tuple tupleFromChild = child.next();
            try {
                Database.getBufferPool().insertTuple(this.t, this.tableId, tupleFromChild);
            } catch (Exception e) {
                e.printStackTrace();
                throw new DbException("The page is full or the given tuple descriptor is a mismatch.");
            }
            numberOfInsertedRecords++;
        }
        beenCalled = true;
        Tuple oneFieldTuple = new Tuple(this.getTupleDesc());
        oneFieldTuple.setField(0, new IntField(numberOfInsertedRecords));
        return oneFieldTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }
}
