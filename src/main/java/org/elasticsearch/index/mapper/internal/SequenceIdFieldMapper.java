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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.version;

// nocommit should we index as DV field or inverted or both?  should we allow customizing PF/DVF?

/** Mapper for the _sequenceId field. */
public class SequenceIdFieldMapper extends AbstractFieldMapper<Long> implements InternalMapper, RootMapper {

    public static final String NAME = "_sequenceId";
    public static final String CONTENT_TYPE = "_sequenceId";

    public static class Defaults {

        public static final String NAME = SequenceIdFieldMapper.NAME;
        public static final float BOOST = 1.0f;
        public static final FieldType FIELD_TYPE = LongField.TYPE_NOT_STORED;

    }

    public static class Builder extends Mapper.Builder<Builder, SequenceIdFieldMapper> {

        public Builder() {
            super(Defaults.NAME);
        }

        @Override
        public SequenceIdFieldMapper build(BuilderContext context) {
            return new SequenceIdFieldMapper();
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder();
        }
    }

    public SequenceIdFieldMapper() {
        super(new Names(NAME, NAME, NAME, NAME), Defaults.BOOST, Defaults.FIELD_TYPE, null, null, null, null, null, null, null, null, ImmutableSettings.EMPTY);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        // nocommit how to ... do this?  now I'm just adding the seqId in InternalEngine...
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // _version added in preparse
    }

    @Override
    public Long value(Object value) {
        if (value == null || (value instanceof Long)) {
            return (Long) value;
        } else {
            return Long.parseLong(value.toString());
        }
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("long");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasDocValues() {
        return true;
    }
}
