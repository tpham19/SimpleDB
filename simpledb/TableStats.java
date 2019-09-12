package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;
    private int ioCostPerPage;
    private DbFile file;
    private TupleDesc tupleDescriptor;
    private int totalNumberOfTuples;
    private Map<String, IntHistogram> fieldNameToIntHistogram;
    private Map<String, StringHistogram> fieldNameToStringHistogram;
    private Map<String, Integer> fieldNameToMin;
    private Map<String, Integer> fieldNameToMax;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDescriptor = Database.getCatalog().getTupleDesc(tableid);
        this.totalNumberOfTuples = 0;
        this.fieldNameToIntHistogram = new HashMap<String, IntHistogram>();
        this.fieldNameToStringHistogram = new HashMap<String, StringHistogram>();
        this.fieldNameToMin = new HashMap<String, Integer>();
        this.fieldNameToMax = new HashMap<String, Integer>();
        TransactionId tid = new TransactionId();
        DbFileIterator iterator = file.iterator(tid);
        // Store min and max values for each integer field
        this.setMinAndMaxValues(iterator);
        for (int i = 0; i < tupleDescriptor.numFields(); i++) {
            String fieldName = tupleDescriptor.getFieldName(i);
            if (tupleDescriptor.getFieldType(i).equals(Type.INT_TYPE)) {
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, fieldNameToMin.get(fieldName), fieldNameToMax.get(fieldName));
                fieldNameToIntHistogram.put(fieldName, intHistogram);
            } else {
                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                fieldNameToStringHistogram.put(fieldName, stringHistogram);
            }
        }
        // Add the values to the histograms
        this.addValuesToHistograms(iterator);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        HeapFile heapfile = (HeapFile) file;
        return (double) heapfile.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (totalNumberOfTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        String fieldName = tupleDescriptor.getFieldName(field);
        // Check if the field is an integer type
        if (constant.getType().equals(Type.INT_TYPE)) {
            // Cast to IntField
            IntField castedConstant = (IntField) constant;
            int integerValue = castedConstant.getValue();
            IntHistogram intHistogram = fieldNameToIntHistogram.get(fieldName);
            return intHistogram.estimateSelectivity(op, integerValue);
        } else {
            // Cast to StringField
            StringField castedConstant = (StringField) constant;
            String stringValue = castedConstant.getValue();
            StringHistogram stringHistogram = fieldNameToStringHistogram.get(fieldName);
            return stringHistogram.estimateSelectivity(op, stringValue);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return totalNumberOfTuples;
    }

    private void setMinAndMaxValues(DbFileIterator iterator) {
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Tuple currentTuple = iterator.next();
                totalNumberOfTuples++;
                for (int i = 0; i < tupleDescriptor.numFields(); i++) {
                    String fieldName = tupleDescriptor.getFieldName(i);
                    if (tupleDescriptor.getFieldType(i).equals(Type.INT_TYPE)) {
                        Field field = currentTuple.getField(i);
                        int value = ((IntField) field).getValue();
                        if (!fieldNameToMin.containsKey(fieldName)) {
                            fieldNameToMin.put(fieldName, value);
                        } else {
                            fieldNameToMin.put(fieldName, Math.min(value, fieldNameToMin.get(fieldName)));
                        }
                        if (!fieldNameToMax.containsKey(fieldName)) {
                            fieldNameToMax.put(fieldName, value);
                        } else {
                            fieldNameToMax.put(fieldName, Math.max(value, fieldNameToMax.get(fieldName)));
                        }
                    }
                }
            }
            iterator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addValuesToHistograms(DbFileIterator iterator) {
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Tuple currentTuple = iterator.next();
                for (int i = 0; i < tupleDescriptor.numFields(); i++) {
                    String fieldName = tupleDescriptor.getFieldName(i);
                    if (tupleDescriptor.getFieldType(i).equals(Type.INT_TYPE)) {
                        Field field = currentTuple.getField(i);
                        int integerValue = ((IntField) field).getValue();
                        fieldNameToIntHistogram.get(fieldName).addValue(integerValue);
                    } else {
                        Field field = currentTuple.getField(i);
                        String stringValue = ((StringField) field).getValue();
                        fieldNameToStringHistogram.get(fieldName).addValue(stringValue);
                    }
                }
            }
            iterator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
