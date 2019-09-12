package simpledb;

import java.util.*;

public class LockManager {

	// Map to a set to avoid duplicates
	private Map<TransactionId, Set<PageId>> transactionToPages;
	private Map<PageId, Set<TransactionId>> pageToTransactions;
	private Map<PageId, Permissions> pageToPermissions;
	private Map<TransactionId, PageId> transactionToPage; // Transaction acquiring a lock

	public LockManager() {
		transactionToPages = new HashMap<TransactionId, Set<PageId>>();
		pageToTransactions = new HashMap<PageId, Set<TransactionId>>();
		pageToPermissions = new HashMap<PageId, Permissions>();
		transactionToPage = new HashMap<TransactionId, PageId>();
	}

	public synchronized boolean transactionContainsLock(TransactionId tid, PageId pid) {
		return (transactionToPages.containsKey(tid) && transactionToPages.get(tid).contains(pid) && 
				pageToTransactions.containsKey(pid) && pageToTransactions.get(pid).contains(tid));
	}

	public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		if (!transactionToPage.containsKey(tid)) {
			transactionToPage.put(tid, pid);
			this.checkForDeadlock(tid, pid);
		}
		if (!this.canAcquireLock(tid, pid, perm)) {
			return false;
		}
		if (!pageToTransactions.containsKey(pid)) {
			pageToTransactions.put(pid, new HashSet<TransactionId>());
		}
		if (!transactionToPages.containsKey(tid)) {
			transactionToPages.put(tid, new HashSet<PageId>());
		}
		transactionToPages.get(tid).add(pid);
		pageToTransactions.get(pid).add(tid);
		// Assigns READ_ONLY or READ_WRITE permissions
		pageToPermissions.put(pid, perm);
		// No longer trying to acquire lock
		transactionToPage.remove(tid);
		return true;
	}

	public synchronized boolean canAcquireLock(TransactionId tid, PageId pid, Permissions perm) {
		// Check if there are no locks on the page
		if (!pageToTransactions.containsKey(pid)) {
			return true;
		}
		// Check if trying to acquire another share lock
		if (perm.equals(Permissions.READ_ONLY)) {
			// Check if another share lock exists
			if (pageToPermissions.get(pid).equals(Permissions.READ_ONLY)) {
				return true;
			} else {
				// Can acquire shared lock if the transaction owns an exclusive lock
				return pageToTransactions.get(pid).contains(tid);
			}
		}
		// Check if trying to upgrade to an exclusive lock
		if (perm.equals(Permissions.READ_WRITE) && pageToPermissions.get(pid).equals(Permissions.READ_ONLY)) {
			boolean hasOnlyOneTransaction = pageToTransactions.containsKey(pid) && (pageToTransactions.get(pid).size() == 1);
			// Check if the transaction is the only transaction holding a shared lock on the given page
			return pageToTransactions.get(pid).contains(tid) && hasOnlyOneTransaction;
		} 
		// Check if the transaction can ask for an exclusive lock again
		return pageToTransactions.get(pid).contains(tid);	
	}

	public synchronized void releaseLock(TransactionId tid, PageId pid) {
		// Check if the given transaction has a lock on the given page
		if (!pageToTransactions.containsKey(pid) || !pageToTransactions.get(pid).contains(tid)) {
			return;
		}
		if (!transactionToPages.containsKey(tid) || !transactionToPages.get(tid).contains(pid)) {
			return;
		}
		transactionToPages.get(tid).remove(pid);
		pageToTransactions.get(pid).remove(tid);
		if (transactionToPages.get(tid).isEmpty()) {
			transactionToPages.remove(tid);
		}
		if (pageToTransactions.get(pid).isEmpty()) {
			pageToTransactions.remove(pid);
			pageToPermissions.remove(pid);
		}
	}

	public synchronized Set<PageId> getPageIdsOfTransaction(TransactionId tid) {
		// Get pageIds of given transaction
		Set<PageId> pageIds = new HashSet<PageId>();
		for (PageId pageId : transactionToPages.get(tid)) {
			pageIds.add(pageId);
		}
		return pageIds;
	}

	public synchronized void releaseLocksOfTransaction(TransactionId tid) {
		if (!transactionToPages.containsKey(tid)) {
			return;
		}
		Set<PageId> pageIds = this.getPageIdsOfTransaction(tid);
		for (PageId pageId : pageIds) {
			this.releaseLock(tid, pageId);
		}
		transactionToPage.remove(tid);
	}


	public synchronized void checkForDeadlock(TransactionId startingTransactionId, PageId pageId) throws TransactionAbortedException {
		// Check if a lock is contained on the given page
		if (pageToTransactions.containsKey(pageId)) {
			for (TransactionId transactionId : pageToTransactions.get(pageId)) {
				// Check the other transactions
				if (!startingTransactionId.equals(transactionId)) {
					Set<TransactionId> transactionsVisited = new HashSet<TransactionId>();
					this.checkForDeadlock(transactionsVisited, transactionId);
				}
			}
		}
	}

	public synchronized void checkForDeadlock(Set<TransactionId> transactionsVisited, TransactionId currentTransactionId) throws TransactionAbortedException {
		// Check if there is a cycle in terms of dependencies
		if (transactionsVisited.contains(currentTransactionId)) {
			throw new TransactionAbortedException();
		}
		transactionsVisited.add(currentTransactionId);
		Set<TransactionId> waitingTransactionIds = this.getWaitingTransactionIds(currentTransactionId);
		for (TransactionId transactionId : waitingTransactionIds) {
			// Check the other transactions
			if (!currentTransactionId.equals(transactionId)) {
				this.checkForDeadlock(transactionsVisited, transactionId);
			}
		}
	}

	public synchronized Set<TransactionId> getWaitingTransactionIds(TransactionId tid) {
		Set<TransactionId> waitingTransactionIds = new HashSet<TransactionId>();
		// Check if the given transaction is not trying to acquire a lock 
		if (!transactionToPage.containsKey(tid)) {
			return waitingTransactionIds;
		}
		// Get the page to which the given transaction is trying to aquire a lock on
		PageId pageId = transactionToPage.get(tid);
		if (!pageToTransactions.containsKey(pageId)) {
			return waitingTransactionIds;
		}
		Set<TransactionId> transactionIds = pageToTransactions.get(pageId);
		// Store transactionIds to which holds a lock on pageId
		for (TransactionId transactionId : transactionIds) {
			waitingTransactionIds.add(transactionId);
		} 
		return waitingTransactionIds;
	}
}