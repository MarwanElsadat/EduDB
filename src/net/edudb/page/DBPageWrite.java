/*
EduDB is made available under the OSI-approved MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/


package net.edudb.page;

import net.edudb.db_operator.DBParameter;
import net.edudb.db_operator.DBOperator;
import net.edudb.engine.DBBufferManager;
import net.edudb.transcation.Step;

public class DBPageWrite extends Step{
    private DBOperator operator;

    public DBPageWrite(DBOperator operator) {
        this.operator = operator;
    }


    public void giveParameter(DBParameter par){
        operator.giveParameter(par);
    }

    @Override
    public void execute() {
        DBPage page = operator.getPage();
//        BufferManager bufferManager = TransactionManager.getBufferManager();
        DBBufferManager.getInstance().write(page.getPageId(), page);
        page.print();
    }
}