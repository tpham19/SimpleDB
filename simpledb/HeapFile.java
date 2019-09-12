package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            if (pid.getPageNumber() < 0 || this.numPages() < pid.getPageNumber()) {
                throw new IllegalArgumentException();
            }
            // Check to see if a new, empty page needs to be added to the heap file
            if (this.numPages() == pid.getPageNumber()) {
                Page heapPage = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), new byte[BufferPool.getPageSize()]);
                this.writePage(heapPage);
                return heapPage;
            }
            RandomAccessFile rad = new RandomAccessFile(f, "rw");
            int pointer = BufferPool.getPageSize() * pid.getPageNumber();
             // Go to corresponding page
            rad.seek(pointer);
            // Read page data
            byte[] data = new byte[BufferPool.getPageSize()];
            rad.read(data);
            rad.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile rad = new RandomAccessFile(f, "rw");
        // Go to corresponding page location
        rad.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
        // Write page data
        rad.write(page.getPageData());
        rad.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        for (int i = 0; i < this.numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() > 0) {
                heapPage.insertTuple(t);
                modifiedPages.add(heapPage);
                return modifiedPages;
            }
        }
        HeapPageId heapPageId = new HeapPageId(this.getId(), this.numPages());
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        modifiedPages.add(heapPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        modifiedPages.add(heapPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

}

