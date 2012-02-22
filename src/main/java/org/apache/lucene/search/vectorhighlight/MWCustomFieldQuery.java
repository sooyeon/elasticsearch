package org.apache.lucene.search.vectorhighlight;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.elasticsearch.plugin.caesar.query.MultiPhraseQuery;
import org.elasticsearch.plugin.caesar.query.SpanWildcardQuery;

public class MWCustomFieldQuery extends CustomFieldQuery
{

    public MWCustomFieldQuery(Query query, IndexReader reader, FastVectorHighlighter highlighter)
            throws IOException
    {
        super(query, reader, highlighter);
    }

    public MWCustomFieldQuery(Query query, IndexReader reader, boolean phraseHighlight, boolean fieldMatch) throws IOException {
        super(query, reader, phraseHighlight, fieldMatch);
    }

    @Override
    void flatten(Query sourceQuery, IndexReader reader, Collection<Query> flatQueries)
            throws IOException
    {
        if (sourceQuery instanceof MultiPhraseQuery) {
            flatten(((MultiPhraseQuery) sourceQuery).rewrite(reader), reader, flatQueries);
        } else if (sourceQuery instanceof SpanWildcardQuery) {
            for (Object o : ((SpanWildcardQuery) sourceQuery).getTerms()) {
                TermQuery termQuery = new TermQuery((Term) o);
                if (!flatQueries.contains(termQuery)) {
                    flatQueries.add(termQuery);
                }
            }
        } else if (sourceQuery instanceof SpanNearQuery) {
            for (Query spanQuery : ((SpanNearQuery) sourceQuery).getClauses())
                flatten(spanQuery, reader, flatQueries);
        } else {
            super.flatten(sourceQuery, reader, flatQueries);
        }
    }
}
