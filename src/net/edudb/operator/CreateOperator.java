/*
EduDB is made available under the OSI-approved MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package net.edudb.operator;

import java.io.IOException;

import net.edudb.block.*;
import net.edudb.operator.Operator;
import net.edudb.page.*;
import net.edudb.statement.SQLCreateTableStatement;
import net.edudb.statistics.Schema;
import net.edudb.table.Table;
import net.edudb.table.*;

/**
 * Created by mohamed on 4/1/14.
 */
public class CreateOperator implements Operator {

	/**
	 * @uml.property name="statement"
	 * @uml.associationEnd multiplicity="(1 1)"
	 */
	private SQLCreateTableStatement statement;

	public CreateOperator(SQLCreateTableStatement statement) {
		this.statement = statement;
	}

	@Override
	public DBResult execute() {
		
		// add table to schema
		String line = statement.getTableName();
		line += " " + statement.getColumnListString();
		Schema.AddTable(line);
		
		Tabular table = new Table(statement.getTableName());
		TableAbstractFactory tableFactory = new TableWriterFactory();
		TableWriter tableWriter = tableFactory.getWriter(TableFileType.Binary);
		
		Pageable page = new Page();
		BlockAbstractFactory blockFactory = new BlockWriterFactory();
		BlockWriter blockWriter = blockFactory.getWriter(BlockFileType.Binary);
		
		try {
			blockWriter.write(page);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		table.addPageName(page.getName());
		
		try {
			tableWriter.write(table);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public DBParameter[] getChildren() {
		return new DBParameter[0];
	}

	@Override
	public void giveParameter(DBParameter par) {

	}

	@Override
	public void runStep(DBPage page) {

	}

	@Override
	public DBPage getPage() {
		return null;
	}

	@Override
	public void print() {

	}

	@Override
	public int numOfParameters() {
		return 0;
	}
}
