/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.highlight;

import static com.google.common.collect.Maps.newHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.vectorhighlight.CustomFieldQuery;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.FragListBuilder;
import org.apache.lucene.search.vectorhighlight.MWCustomFieldQuery;
import org.apache.lucene.search.vectorhighlight.MWSimpleHTMLFormatter;
import org.apache.lucene.search.vectorhighlight.TermFragListBuilder;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.document.SingleFieldSelector;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.meltwater.caesar.highlight.MultiFieldTermPositionVectorTokenSource;
import com.meltwater.caesar.highlight.PositionGapFragmentsBuilder;

/**
 *
 */
public class HighlightPhase implements FetchSubPhase {
    private final ESLogger logger = Loggers.getLogger(HighlightPhase.class);

    public static class Encoders {
        public static Encoder DEFAULT = new DefaultEncoder();
        public static Encoder HTML = new SimpleHTMLEncoder();
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        // we use a cache to cache heavy things, mainly the rewrite in FieldQuery for FVH
        HighlighterEntry cache = (HighlighterEntry) hitContext.cache().get("highlight");
        if (cache == null) {
            cache = new HighlighterEntry();
            hitContext.cache().put("highlight", cache);
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper(hitContext.hit().type());

        Map<String, HighlightField> highlightFields = newHashMap();
        Set<CISString> hitwords = new HashSet<CISString>();
        for (SearchContextHighlight.Field field : context.highlight().fields()) {
            Encoder encoder;
            if (field.encoder().equals("html")) {
                encoder = Encoders.HTML;
            } else {
                encoder = Encoders.DEFAULT;
            }
            FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(field.field());
            if (mapper == null) {
                MapperService.SmartNameFieldMappers fullMapper = context.mapperService().smartName(field.field());
                if (fullMapper == null || !fullMapper.hasDocMapper()) {
                    //Save skipping missing fields
                    continue;
                }
                if (!fullMapper.docMapper().type().equals(hitContext.hit().type())) {
                    continue;
                }
                mapper = fullMapper.mapper();
                if (mapper == null) {
                    continue;
                }
            }

            // if we can do highlighting using Term Vectors, use FastVectorHighlighter, otherwise, use the
            // slower plain highlighter
            if (mapper.termVector() != Field.TermVector.WITH_POSITIONS_OFFSETS) {
                MapperHighlightEntry entry = cache.mappers.get(mapper);
                if (entry == null) {
                    // Don't use the context.query() since it might be rewritten, and we need to pass the non rewritten queries to
                    // let the highlighter handle MultiTerm ones

                    // QueryScorer uses WeightedSpanTermExtractor to extract terms, but we can't really plug into
                    // it, so, we hack here (and really only support top level queries)
                    Query query = context.parsedQuery().query();
                    while (true) {
                        boolean extracted = false;
                        if (query instanceof FunctionScoreQuery) {
                            query = ((FunctionScoreQuery) query).getSubQuery();
                            extracted = true;
                        } else if (query instanceof FiltersFunctionScoreQuery) {
                            query = ((FiltersFunctionScoreQuery) query).getSubQuery();
                            extracted = true;
                        } else if (query instanceof ConstantScoreQuery) {
                            ConstantScoreQuery q = (ConstantScoreQuery) query;
                            if (q.getQuery() != null) {
                                query = q.getQuery();
                                extracted = true;
                            }
                        }
                        if (!extracted) {
                            break;
                        }
                    }

                    QueryScorer queryScorer = new QueryScorer(query, field.requireFieldMatch() ? mapper.names().indexName() : null);
                    queryScorer.setExpandMultiTermQuery(true);
                    Fragmenter fragmenter;
                    if (field.numberOfFragments() == 0) {
                        fragmenter = new NullFragmenter();
                    } else {
                        fragmenter = new SimpleSpanFragmenter(queryScorer, field.fragmentCharSize());
                    }
                    MWSimpleHTMLFormatter formatter = new MWSimpleHTMLFormatter(field.preTags()[0], field.postTags()[0]);


                    entry = new MapperHighlightEntry();
                    entry.highlighter = new Highlighter(formatter, encoder, queryScorer);
                    entry.highlighter.setTextFragmenter(fragmenter);
                    entry.formatter = formatter;

                    cache.mappers.put(mapper, entry);
                }

                List<Object> textsToHighlight;
                if (mapper.stored()) {
                    try {
                        Document doc = hitContext.reader().document(hitContext.docId(), new SingleFieldSelector(mapper.names().indexName()));
                        textsToHighlight = new ArrayList<Object>(doc.getFields().size());
                        for (Fieldable docField : doc.getFields()) {
                            if (docField.stringValue() != null) {
                                textsToHighlight.add(docField.stringValue());
                            }
                        }
                    } catch (Exception e) {
                        throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                    }
                } else {
                    SearchLookup lookup = context.lookup();
                    lookup.setNextReader(hitContext.reader());
                    lookup.setNextDocId(hitContext.docId());
                    textsToHighlight = lookup.source().extractRawValues(mapper.names().sourcePath());
                }

                // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                ArrayList<TextFragment> fragsList = new ArrayList<TextFragment>();
                try {
                    for (Object textToHighlight : textsToHighlight) {
                        String text = textToHighlight.toString();
                        Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().type()).mappers().indexAnalyzer();
                        TokenStream tokenStream = analyzer.reusableTokenStream(mapper.names().indexName(), new FastStringReader(text));
                        TextFragment[] bestTextFragments = entry.highlighter.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                        for (TextFragment bestTextFragment : bestTextFragments) {
                            if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                                fragsList.add(bestTextFragment);
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                }
                if (field.scoreOrdered()) {
                    Collections.sort(fragsList, new Comparator<TextFragment>() {
                        public int compare(TextFragment o1, TextFragment o2) {
                            return Math.round(o2.getScore() - o1.getScore());
                        }
                    });
                }
                String[] fragments = null;
                // number_of_fragments is set to 0 but we have a multivalued field
                if (field.numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
                    fragments = new String[1];
                    for (int i = 0; i < fragsList.size(); i++) {
                        fragments[0] = (fragments[0] != null ? (fragments[0] + " ") : "") + fragsList.get(i).toString();
                    }
                } else {
                    // refine numberOfFragments if needed
                    numberOfFragments = fragsList.size() < numberOfFragments ? fragsList.size() : numberOfFragments;
                    fragments = new String[numberOfFragments];
                    for (int i = 0; i < fragments.length; i++) {
                        fragments[i] = fragsList.get(i).toString();
                    }
                }

                if (fragments != null && fragments.length > 0) {
                    for (String term : entry.formatter.getTerms())
                        hitwords.add(new CISString(term));
                    if (highlight(field.field())) {
                        HighlightField highlightField = new HighlightField(field.field(), fragments);
                        highlightFields.put(highlightField.name(), highlightField);
                    }
                }

            } else {
                try {
                    MapperHighlightEntry entry = cache.mappers.get(mapper);
                    FieldQuery fieldQuery = null;
                    if (entry == null) {
                        entry = new MapperHighlightEntry();
                        entry.fragListBuilder = new TermFragListBuilder();
                        entry.fragmentsBuilder = new PositionGapFragmentsBuilder();
                        if (cache.fvh == null) {
                            // parameters to FVH are not requires since:
                            // first two booleans are not relevant since they are set on the CustomFieldQuery (phrase and fieldMatch)
                            // fragment builders are used explicitly
                            cache.fvh = new FastVectorHighlighter();
                        }
                        CustomFieldQuery.highlightFilters.set(field.highlightFilter());
                        if (field.requireFieldMatch().booleanValue()) {
                            if (cache.fieldMatchFieldQuery == null) {
                                // we use top level reader to rewrite the query against all readers, with use caching it across hits (and across readers...)
                                cache.fieldMatchFieldQuery = new MWCustomFieldQuery(context.parsedQuery().query(), hitContext.topLevelReader(), true, field.requireFieldMatch());
                            }
                            fieldQuery = cache.fieldMatchFieldQuery;
                        } else {
                            if (cache.noFieldMatchFieldQuery == null) {
                                // we use top level reader to rewrite the query against all readers, with use caching it across hits (and across readers...)
                                cache.noFieldMatchFieldQuery = new MWCustomFieldQuery(context.parsedQuery().query(), hitContext.topLevelReader(), true, field.requireFieldMatch());
                            }
                            fieldQuery = cache.noFieldMatchFieldQuery;
                        }
                        cache.mappers.put(mapper, entry);
                    }

                    String[] fragments;

                    // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                    int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                    // we highlight against the low level reader and docId, because if we load source, we want to reuse it if possible
                    fragments = cache.fvh.getBestFragments(fieldQuery, hitContext.reader(), hitContext.docId(), mapper.names().indexName(), field.fragmentCharSize(), numberOfFragments,
                            entry.fragListBuilder, entry.fragmentsBuilder, field.preTags(), field.postTags(), encoder);

                    if (fragments != null && fragments.length > 0) {
                        for (String term : entry.fragmentsBuilder.getTerms())
                            hitwords.add(new CISString(term));
                        HighlightField highlightField = new HighlightField(field.field(), fragments);
                        highlightFields.put(highlightField.name(), highlightField);
                    }
                } catch (Exception e) {
                    throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                }
            }
        }

        hitContext.hit().highlightFields(highlightFields);
        try {
            addCustomHitDetails(hitContext, hitwords);

            /*if (logger.isDebugEnabled()) {
                logger.debug("Original Query: " + context.parsedQuery().query().toString(), (Object[]) null);
                logger.debug("Rewritten Query: " + context.parsedQuery().query().rewrite(hitContext.reader()).toString(), (Object[]) null);
            }*/
        } catch (CorruptIndexException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch metadata field(s)", e);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch metadata field(s)", e);
        }
    }

    static class MapperHighlightEntry {
        public FragListBuilder fragListBuilder;
        public PositionGapFragmentsBuilder fragmentsBuilder;

        public Highlighter highlighter;
        public MWSimpleHTMLFormatter formatter;
    }

    static class HighlighterEntry {
        public FastVectorHighlighter fvh;
        public FieldQuery noFieldMatchFieldQuery;
        public FieldQuery fieldMatchFieldQuery;
        public Map<FieldMapper, MapperHighlightEntry> mappers = Maps.newHashMap();
    }

    private class CISString {
        public String value;
        public CISString(String value) { this.value = value; }
        @Override public String toString() { return value; }
        @Override public int hashCode() { return value.toLowerCase().hashCode(); }
        @Override public boolean equals(Object o) { return value.toLowerCase().equals(o.toString().toLowerCase()); }
    }
    /**
     * HACK! This is not the best place for this but necessary until
     * Elasticsearch implements customizable fetchphase modules
     * <sooyeon@meltwater.com>
     * 
     * @param hitContext
     * @param cache
     * @throws IOException 
     * @throws CorruptIndexException 
     */
    private void addCustomHitDetails(HitContext hitContext, Collection<CISString> hitwords) throws CorruptIndexException, IOException
    {
        // fetch title
        if (!hitContext.hit().getFields().containsKey("title")) {
            String languageCode = hitContext.hit().getFields().get("language").getValue();
            Document doc = hitContext.reader().document(hitContext.docId());
            String fieldName = "title." + languageCode;
            if (doc.getFieldable(fieldName) != null)
                hitContext.hit().getFields().put("title", new InternalSearchHitField("title", Arrays.asList((Object) doc.get(fieldName))));
        }

        // reconstruct ingress
        TermPositionVector vector = (TermPositionVector) hitContext.reader().getTermFreqVector(hitContext.docId(),
                "ingress.snippet");
        TokenStream stream = MultiFieldTermPositionVectorTokenSource.getTokenStream(vector, false);
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
        StringBuilder sb = new StringBuilder();
        while (stream.incrementToken())
            sb.append(termAtt.toString()).append(' ');
        hitContext.hit().getFields().put("ingress", new InternalSearchHitField("ingress", Arrays.asList((Object) sb.toString().trim())));

        // list hitwords
        if (!hitwords.isEmpty()) {
            List<Object> terms = new ArrayList<Object>();
            for (CISString hitword : hitwords)
                terms.add(hitword.value);
            hitContext.hit().getFields().put("hitwords", new InternalSearchHitField("hitwords", terms));
        }
    }
    
    public static boolean highlight(String fieldName)
    {
        if (fieldName.contains("title"))
            return true;
        else if (fieldName.contains("ingress"))
            return true;
        else if (fieldName.contains("content"))
            return true;
        return false;
    }
}
