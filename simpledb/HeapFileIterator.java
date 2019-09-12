package simpledb;
import java.util.*;


public class HeapFileIterator implements DbFileIterator {

	private TransactionId tid;
	private HeapFile heapFile;
	private Iterator<Tuple> tupleIterator;
	// Keeps track of current page number
	private int pageNumber;

	public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
		this.tid = tid;
		this.heapFile = heapFile;
	}

	/**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
        throws DbException, TransactionAbortedException {
        // Open at first page
        pageNumber = 0;
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.getId(), pageNumber), Permissions.READ_ONLY);
    	// Get tuple iterator for the first page
    	tupleIterator = page.iterator();
    }

    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        // Check if iterator isn't open
        if (tupleIterator == null) {
        	return false;
        }
        // Look for next page if there are no more tuples on the current page
        while (!tupleIterator.hasNext()) {
        	pageNumber++;
        	// Check if reached end of file
        	if (heapFile.numPages() == pageNumber) {
        		return false;
        	}
        	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.getId(), pageNumber), Permissions.READ_ONLY);
        	// Store tuple iterator for new page
        	tupleIterator = page.iterator();
        }
        return true;
	}

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!this.hasNext()) {
        	throw new NoSuchElementException();
        }
        return tupleIterator.next();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
    	this.close();
    	this.open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    	tupleIterator = null;
    }
}