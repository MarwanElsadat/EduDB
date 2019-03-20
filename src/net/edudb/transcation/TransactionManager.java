/*
EduDB is made available under the OSI-approved MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package net.edudb.transcation;

/**
 * A singleton that handles the execution of transactions.
 * 
 * @author Ahmed Abdul Badie
 *
 */
public class TransactionManager {

	private static TransactionManager instance = new TransactionManager();

	private TransactionManager() {
	}

	public static TransactionManager getInstance() {
		return instance;
	}

	/**
	 * Executes a concurrent transaction.
	 * 
	 * @param transaction
	 *            The concurrent transaction to execute.
	 */
	public String execute(ConcurrentTransaction transaction) {
		Thread thread = new Thread(transaction);
		thread.start();
		return "";
	}

	/**
	 * Executes a synchronized transaction.
	 * 
	 * @param transaction
	 *            The synchronized transaction to execute.
	 */
	public String execute(SynchronizedTransaction transaction) {
		return transaction.run();
	}
}
