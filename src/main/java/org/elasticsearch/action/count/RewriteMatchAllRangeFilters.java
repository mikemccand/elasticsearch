/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.action.count;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.RangeFilterParser;
import org.elasticsearch.index.query.RangeQueryParser;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import com.fasterxml.jackson.core.JsonParseException;

// TODO
//   - can we also rewrite to MatchNone?
//   - store MinMax somewhere for the index (recomputing it is not cheap)
//   - only do this for read-only indices

/** Takes the raw (BytesReference) source for a search request, recursively finds any range filters/queries, checks whether that
 *  filter/query matches all documents in the current index.  If so, the filter/query is replaced with MatchAll.  This returns the rewritten
 *  request (BytesReference). */

public class RewriteMatchAllRangeFilters {
    // nocommit how to access the RangeFilterParser singleton that was already instantiated...
    private final static RangeFilterParser RANGE_FILTER_PARSER = new RangeFilterParser();
    private final static RangeQueryParser RANGE_QUERY_PARSER = new RangeQueryParser();

    /** Holds min/max long value for a given index. */
    static class MinMaxLongs {

        /** null if no docs, or no docs that have the requested field */
        final Long minLong;
        final Long maxLong;

        public MinMaxLongs(Long minLong, Long maxLong) {
            this.minLong = minLong;
            this.maxLong = maxLong;
        }

        @Override
        public String toString() {
            return "min=" + minLong + " max=" + maxLong;
        }
    }

    private static MinMaxLongs getIndexMinMaxLongs(IndexReader reader, String fieldName) throws IOException {
        Long minLong = null;
        Long maxLong = null;
        for(LeafReaderContext ctx : reader.leaves()) {
            Terms terms = ctx.reader().fields().terms(fieldName);
            if (terms != null) {
                long min = NumericUtils.getMinLong(terms);
                if (minLong == null || min < minLong.longValue()) {
                    minLong = min;
                }
                long max = NumericUtils.getMaxLong(terms);
                if (maxLong == null || max > maxLong.longValue()) {
                    maxLong = max;
                }
            }
        }

        return new MinMaxLongs(minLong, maxLong);
    }

    /** Copies current structure from the parser. */
    private static BytesReference copy(XContentParser parser) throws IOException {
        // nocommit is there a better way to "copy" a structure out?
        XContent xc = parser.contentType().xContent();
        BytesStreamOutput os = new BytesStreamOutput();
        XContentGenerator gen = xc.createGenerator(os);
        gen.copyCurrentStructure(parser);
        gen.close();
        return os.bytes();
    }

    /** Just like rewriteOneRangeFilter, but Query instead */
    private static void rewriteOneRangeQuery(IndexSearcher searcher, IndexQueryParserService parserService, XContentParser parser, XContentGenerator gen) throws IOException {

        QueryParseContext parseContext = parserService.getParseContext();

        XContent xc = parser.contentType().xContent();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[range] query malformed, missing start_object");
        }

        // This is a range query: copy the source so we can both parse it and possibly keep it:
        BytesReference rangeQuerySource = copy(parser);

        XContentParser parser2 = xc.createParser(rangeQuerySource);
                            
        parseContext.parser(parser2);

        // Skip the start object:
        token = parser2.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[range] query malformed, missing start_object");
        }

        Query query = RANGE_QUERY_PARSER.parse(parseContext);

        Long minValue;
        Long maxValue;
        String fieldName;
        String rangeFieldName;
        // nocommit must properly handle includeLower/Upper:
        if (query instanceof NumericRangeQuery) {
            // nocommit must verify this is a long field, or generalize to other types...
            NumericRangeQuery<Long> numQuery = (NumericRangeQuery<Long>) query;
            minValue = numQuery.getMin();
            maxValue = numQuery.getMax();
            fieldName = numQuery.getField();
        } else {
            // Some kind of other range filter?
            minValue = null;
            maxValue = null;
            fieldName = null;
        }

        boolean matchesAll;

        if (fieldName != null) {
            if (minValue == null && maxValue == null) {
                // silly
                matchesAll = true;
            } else {
                MinMaxLongs minMaxLongs = getIndexMinMaxLongs(searcher.getIndexReader(), fieldName);
                matchesAll = (minMaxLongs.minLong == null || minValue == null || minValue < minMaxLongs.minLong) &&
                    (minMaxLongs.maxLong == null || maxValue == null || maxValue > minMaxLongs.maxLong);
            }
        } else {
            matchesAll = false;
        }
        // nocommit matchesNone too?

        if (matchesAll) {
            // OK, rewrite to MatchAll:
            gen.writeStartObject();
            gen.writeFieldName("match_all");
            gen.writeStartObject();
            gen.writeEndObject();
            gen.writeEndObject();
        } else {
            // Does not match all; leave original:
            gen.writeStartObject();
            gen.writeFieldName(RangeQueryParser.NAME);
            gen.writeStartObject();
            // nocommit just copy at this point?
            XContentParser parser3 = xc.createParser(rangeQuerySource);
            parser3.nextToken();
            parser3.nextToken();
            //rewriteCurrentStructure(searcher, parserService, parser3, gen);
            gen.copyCurrentStructure(parser3);
            gen.writeEndObject();
            gen.writeEndObject();
        }
    }

    private static void rewriteOneRangeFilter(IndexSearcher searcher, IndexQueryParserService parserService, XContentParser parser, XContentGenerator gen) throws IOException {

        QueryParseContext parseContext = parserService.getParseContext();

        XContent xc = parser.contentType().xContent();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[range] filter malformed, missing start_object");
        }

        // This is a range filter: copy the source so we can both parse it and possibly keep it:
        BytesReference rangeFilterSource = copy(parser);

        XContentParser parser2 = xc.createParser(rangeFilterSource);

        parseContext.parser(parser2);

        // Skip the start object:
        token = parser2.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[range] filter malformed, missing start_object");
        }

        Filter filter = RANGE_FILTER_PARSER.parse(parseContext);

        Long minValue;
        Long maxValue;
        String fieldName;
        String rangeFieldName;
        // nocommit must properly handle includeLower/Upper:
        if (filter instanceof NumericRangeFieldDataFilter) {
            // nocommit this isn't right?  this could be a doc values only field, so we can't use Terms.getMin/Max?
            // nocommit must verify this is a long field, or generalize to other types...
            NumericRangeFieldDataFilter<Long> numFilter = (NumericRangeFieldDataFilter<Long>) filter;
            minValue = numFilter.getLowerVal();
            maxValue = numFilter.getUpperVal();
            fieldName = numFilter.getField();
        } else if (filter instanceof NumericRangeFilter) {
            // nocommit must verify this is a long field, or generalize to other types...
            NumericRangeFilter<Long> numFilter = (NumericRangeFilter<Long>) filter;
            minValue = numFilter.getMin();
            maxValue = numFilter.getMax();
            fieldName = numFilter.getField();
        } else {
            // Some kind of other range filter?
            minValue = null;
            maxValue = null;
            fieldName = null;
        }

        boolean matchesAll;

        if (fieldName != null) {
            if (minValue == null && maxValue == null) {
                // silly
                matchesAll = true;
            } else {
                MinMaxLongs minMaxLongs = getIndexMinMaxLongs(searcher.getIndexReader(), fieldName);
                matchesAll = (minMaxLongs.minLong == null || minValue == null || minValue < minMaxLongs.minLong) &&
                    (minMaxLongs.maxLong == null || maxValue == null || maxValue > minMaxLongs.maxLong);
            }
        } else {
            matchesAll = false;
        }
        // nocommit matchesNone too?

        if (matchesAll) {
            // OK, rewrite to MatchAll:
            gen.writeStartObject();
            gen.writeFieldName("match_all");
            gen.writeStartObject();
            gen.writeEndObject();
            gen.writeEndObject();
        } else {
            // Does not match all; leave original:
            gen.writeStartObject();
            gen.writeFieldName(RangeFilterParser.NAME);
            gen.writeStartObject();
            // nocommit just copy at this point?
            XContentParser parser3 = xc.createParser(rangeFilterSource);
            parser3.nextToken();
            parser3.nextToken();
            gen.copyCurrentStructure(parser3);
            gen.writeEndObject();
            gen.writeEndObject();
        }
    }

    // nocommit forked from XContentHelper.copyCurrentStructure.  can we share?
    private static void rewriteCurrentStructure(IndexSearcher searcher, IndexQueryParserService parserService, XContentParser parser, XContentGenerator gen) throws IOException {
        XContentParser.Token token = parser.currentToken();
        switch (token) {

        case START_ARRAY:
            gen.writeStartArray();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                rewriteCurrentStructure(searcher, parserService, parser, gen);
            }
            gen.writeEndArray();
            break;

        case START_OBJECT:
            gen.writeStartObject();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                // nocommit real check
                assert token == XContentParser.Token.FIELD_NAME: "token=" + token;

                String fieldName = parser.currentName();
                gen.writeFieldName(fieldName);

                if (fieldName.equals("query")) {
                    // nocommit this is not necessarily safe, i.e. something could have a "query" that doesn't mean what we assume it means here:
                    token = parser.nextToken();
                    if (token == XContentParser.Token.START_OBJECT) {
                        token = parser.nextToken();
                        // nocommit real check
                        assert token == XContentParser.Token.FIELD_NAME;
                        if (parser.currentName().equals(RangeQueryParser.NAME)) {
                            rewriteOneRangeQuery(searcher, parserService, parser, gen);
                        } else {
                            // Query other than "range" query:
                            gen.writeStartObject();
                            gen.writeFieldName(parser.currentName());
                            // Copy over current query
                            parser.nextToken();
                            rewriteCurrentStructure(searcher, parserService, parser, gen);
                            token = parser.nextToken();
                            assert token == XContentParser.Token.END_OBJECT;
                            gen.writeEndObject();
                        }
                    } else {
                        // Could be query: string
                        XContentHelper.copyCurrentStructure(gen, parser);
                    }
                } else if (fieldName.equals("filter")) {
                    // nocommit this is not necessarily safe, i.e. something could have a "filter" that doesn't mean what we assume it means here:
                    token = parser.nextToken();
                    // nocommit real check
                    assert token == XContentParser.Token.START_OBJECT;
                    token = parser.nextToken();
                    // nocommit real check
                    assert token == XContentParser.Token.FIELD_NAME;
                    if (parser.currentName().equals(RangeFilterParser.NAME)) {
                        rewriteOneRangeFilter(searcher, parserService, parser, gen);
                    } else {
                        // Filter other than "range" filter:
                        gen.writeStartObject();
                        gen.writeFieldName(parser.currentName());
                        // Copy over current filter
                        parser.nextToken();
                        rewriteCurrentStructure(searcher, parserService, parser, gen);
                        gen.writeEndObject();
                    }
                } else {
                    parser.nextToken();
                    rewriteCurrentStructure(searcher, parserService, parser, gen);
                }
            }
            gen.writeEndObject();
            break;
            
        default: // others are simple:
            try {
                XContentHelper.copyCurrentEvent(gen, parser);
            } catch (JsonParseException jpe) {
                throw new QueryParsingException(parserService.getParseContext().index(), jpe.toString());
            }
        }
    }

    public static BytesReference rewriteRangeFilters(BytesReference source,
                                                     IndexSearcher searcher,
                                                     IndexQueryParserService parserService) {
        try {
            if (source == null || source.length() == 0) {
                // Empty source means matches all docs?
                return source;
            }
            //System.out.println("\nSOURCE IN: " + source.toUtf8());
            XContentType xct = XContentFactory.xContentType(source);
            XContent xc = xct.xContent();
            XContentParser parser = xc.createParser(source);
            BytesStreamOutput os = new BytesStreamOutput();
            XContentGenerator gen = xc.createGenerator(os);
            parser.nextToken();
            rewriteCurrentStructure(searcher, parserService, parser, gen);
            gen.close();
            BytesReference result = os.bytes();
            //System.out.println("\nSOURCE OUT: " + result.toUtf8());
            return result;
        } catch (Throwable t) {
            System.out.println("REWRITE FAILED:");
            t.printStackTrace(System.out);
            throw new RuntimeException(t);
        }
    }
}
