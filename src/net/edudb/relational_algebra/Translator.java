/*
EduDB is made available under the OSI-approved MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package net.edudb.relational_algebra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import adipe.translate.Queries;
import adipe.translate.TranslationException;
import net.edudb.ebtree.EBNode;
import net.edudb.ebtree.EBTree;
import net.edudb.query.PostOrderTreeExecutor;
import net.edudb.query.QueryTree;
import net.edudb.query.QueryTreeExecutor;
import net.edudb.relation.Relation;
import net.edudb.relation.RelationIterator;
import net.edudb.statistics.Schema;
import ra.Term;

public class Translator {
	public String translate(String sqlString) {
		try {
			Term term = Queries.getRaOf(adipe.translate.ra.Schema.create(Schema.getInstance().getSchema()), sqlString);
			return term.toString();
		} catch (RuntimeException | TranslationException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private RAMatcherChain getChain() {
		RAMatcherChain project = new ProjectMatcher();
		RAMatcherChain filter = new FilterMatcher();
		RAMatcherChain relation = new RelationMatcher();

		project.setNextInChain(filter);
		filter.setNextInChain(relation);
		relation.setNextInChain(new NullMatcher());
		return project;
	}
	
	private ArrayList<EBNode> constructTreeNodes(String relationAlgebra, RAMatcherChain chain) {
		ArrayList<EBNode> nodes = new ArrayList<>();
		while (relationAlgebra.length() != 0) {
			RAMatcherResult result = chain.parse(relationAlgebra);
			if (result != null) {
				nodes.add(result.getNode());
				relationAlgebra = result.getString();
			} else {
				System.out.println("No Match");
				break;
			}
		}
		return nodes;
	}

	public QueryTree processRelationalAlgebra(String relationAlgebra) {
		ArrayList<EBNode> nodes = constructTreeNodes(relationAlgebra, getChain());
		Collections.reverse(nodes);
		EBTree tree = new QueryTree();
		tree.constructTree(nodes);

		return (QueryTree) tree;
	}

	/**
	 * 
	 * @param string
	 *            String to be matched
	 * @param regex
	 *            Regular expression to be matched against
	 * @return
	 */
	public static Matcher matcher(String string, String regex) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(string);
		return matcher;
	}

	public static void main(String[] args) {
		Translator t = new Translator();

		String ra = t.translate("select a,b from test where b<>4 and a<=7");

		System.out.println(ra);

		QueryTree qt = t.processRelationalAlgebra(ra);

		QueryTreeExecutor qte = new PostOrderTreeExecutor();

		Relation r = qte.execute(qt);

		RelationIterator ri = r.getIterator();
		while (ri.hasNext()) {
			System.out.println(ri.next());
		}
	}
}
