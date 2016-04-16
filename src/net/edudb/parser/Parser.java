/*
EduDB is made available under the OSI-approved MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package net.edudb.parser;

import adipe.translate.TranslationException;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import net.edudb.console.DatabaseConsole;
import net.edudb.db_operator.DBOperator;
import net.edudb.engine.DBTransactionManager;
import net.edudb.plan.PlanFactory;
import net.edudb.server.ServerWriter;
import net.edudb.statement.SQLSelectStatement;
import net.edudb.statement.SQLStatement;
import net.edudb.statement.SQLStatementFactory;
import net.edudb.statement.SQLStatementType;
import net.edudb.structure.table.TableManager;
import net.edudb.transcation.ConcurrentTransaction;
import net.edudb.transcation.SynchronizedTransaction;
import net.edudb.transcation.TransactionManager;

public class Parser {
	/**
	 * @uml.property name="sqlparser"
	 * @uml.associationEnd multiplicity="(1 1)"
	 */
	TGSqlParser sqlparser;
	/**
	 * @uml.property name="planFactory"
	 * @uml.associationEnd multiplicity="(1 1)"
	 */
	PlanFactory planFactory;

	public Parser() {
		sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		planFactory = new PlanFactory();
	}

	public void parseSQL(String strSQL) throws TranslationException {
		sqlparser.setSqltext(strSQL);
		int ret = sqlparser.parse();
		if (ret == 0) {
			SQLStatementFactory statementFactory = new SQLStatementFactory();
			SQLStatement statement = statementFactory.getSQLStatement(sqlparser.sqlstatements.get(0));
			DBOperator plan = planFactory.makePlan(statement);
			if (plan == null) {
				return;
			}

			switch (statement.statementType()) {
			case SQLCreateTableStatement:
			case SQLInsertStatement: {
//				ConcurrentTransaction transaction = new ConcurrentTransaction(plan);
				SynchronizedTransaction transaction = new SynchronizedTransaction(plan);
				TransactionManager.getInstance().execute(transaction);
				break;
			}
			default: {
				DBTransactionManager.getInstance().execute(plan);
				if (statement.statementType() != SQLStatementType.SQLSelectStatement) {
					ServerWriter.getInstance().writeln("Parser (parseSQL): " + plan.execute());
				}
				break;
			}

			}
		} else {
			ServerWriter.getInstance().writeln(sqlparser.getErrormessage());
		}
	}
}