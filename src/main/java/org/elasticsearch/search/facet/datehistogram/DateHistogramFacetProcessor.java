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

package org.elasticsearch.search.facet.datehistogram;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DateHistogramFacetProcessor extends AbstractComponent implements FacetProcessor {

    private final ImmutableMap<String, DateFieldParser> dateFieldParsers;

    @Inject
    public DateHistogramFacetProcessor(Settings settings) {
        super(settings);
        InternalDateHistogramFacet.registerStreams();

        dateFieldParsers = MapBuilder.<String, DateFieldParser>newMapBuilder()
                .put("year", new DateFieldParser.YearOfCentury())
                .put("1y", new DateFieldParser.YearOfCentury())
                .put("month", new DateFieldParser.MonthOfYear())
                .put("1m", new DateFieldParser.MonthOfYear())
                .put("week", new DateFieldParser.WeekOfWeekyear())
                .put("1w", new DateFieldParser.WeekOfWeekyear())
                .put("day", new DateFieldParser.DayOfMonth())
                .put("1d", new DateFieldParser.DayOfMonth())
                .put("hour", new DateFieldParser.HourOfDay())
                .put("1h", new DateFieldParser.HourOfDay())
                .put("minute", new DateFieldParser.MinuteOfHour())
                .put("1m", new DateFieldParser.MinuteOfHour())
                .put("second", new DateFieldParser.SecondOfMinute())
                .put("1s", new DateFieldParser.SecondOfMinute())
                .immutableMap();
    }

    @Override
    public String[] types() {
        return new String[]{DateHistogramFacet.TYPE, "dateHistogram"};
    }

    @Override
    public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        String valueScript = null;
        String scriptLang = null;
        Map<String, Object> params = null;
        String interval = null;
        DateTimeZone preZone = DateTimeZone.UTC;
        DateTimeZone postZone = DateTimeZone.UTC;
        boolean preZoneAdjustLargeInterval = false;
        long preOffset = 0;
        long postOffset = 0;
        float factor = 1.0f;
        Chronology chronology = ISOChronology.getInstanceUTC();
        DateHistogramFacet.ComparatorType comparatorType = DateHistogramFacet.ComparatorType.TIME;
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(fieldName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("field".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("key_field".equals(fieldName) || "keyField".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("value_field".equals(fieldName) || "valueField".equals(fieldName)) {
                    valueField = parser.text();
                } else if ("interval".equals(fieldName)) {
                    interval = parser.text();
                } else if ("time_zone".equals(fieldName) || "timeZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone".equals(fieldName) || "preZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone_adjust_large_interval".equals(fieldName) || "preZoneAdjustLargeInterval".equals(fieldName)) {
                    preZoneAdjustLargeInterval = parser.booleanValue();
                } else if ("post_zone".equals(fieldName) || "postZone".equals(fieldName)) {
                    postZone = parseZone(parser, token);
                } else if ("pre_offset".equals(fieldName) || "preOffset".equals(fieldName)) {
                    preOffset = parseOffset(parser.text());
                } else if ("post_offset".equals(fieldName) || "postOffset".equals(fieldName)) {
                    postOffset = parseOffset(parser.text());
                } else if ("factor".equals(fieldName)) {
                    factor = parser.floatValue();
                } else if ("value_script".equals(fieldName) || "valueScript".equals(fieldName)) {
                    valueScript = parser.text();
                } else if ("order".equals(fieldName) || "comparator".equals(fieldName)) {
                    comparatorType = DateHistogramFacet.ComparatorType.fromString(parser.text());
                } else if ("lang".equals(fieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for histogram facet, either using [field] or using [key_field]");
        }

        FieldMapper mapper = context.smartNameFieldMapper(keyField);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "(key) field [" + keyField + "] not found");
        }
        if (mapper.fieldDataType() != FieldDataType.DefaultTypes.LONG) {
            throw new FacetPhaseExecutionException(facetName, "(key) field [" + keyField + "] is not of type date");
        }

        if (interval == null) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        TimeZoneRounding.Builder tzRoundingBuilder;
        DateFieldParser fieldParser = dateFieldParsers.get(interval);
        if (fieldParser != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(fieldParser.parse(chronology));
        } else {
            // the interval is a time value?
            tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null));
        }

        TimeZoneRounding tzRounding = tzRoundingBuilder
                .preZone(preZone).postZone(postZone)
                .preZoneAdjustLargeInterval(preZoneAdjustLargeInterval)
                .preOffset(preOffset).postOffset(postOffset)
                .factor(factor)
                .build();

        if (valueScript != null) {
            return new ValueScriptDateHistogramFacetCollector(facetName, keyField, scriptLang, valueScript, params, tzRounding, comparatorType, context);
        } else if (valueField == null) {
            return new CountDateHistogramFacetCollector(facetName, keyField, tzRounding, comparatorType, context);
        } else {
            return new ValueDateHistogramFacetCollector(facetName, keyField, valueField, tzRounding, comparatorType, context);
        }
    }

    private long parseOffset(String offset) throws IOException {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null).millis();
        }
        return TimeValue.parseTimeValue(offset, null).millis();
    }

    private DateTimeZone parseZone(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return DateTimeZone.forOffsetHours(parser.intValue());
        } else {
            String text = parser.text();
            int index = text.indexOf(':');
            if (index != -1) {
                // format like -02:30
                return DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(text.substring(0, index)),
                        Integer.parseInt(text.substring(index + 1))
                );
            } else {
                // id, listed here: http://joda-time.sourceforge.net/timezones.html
                return DateTimeZone.forID(text);
            }
        }
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        InternalDateHistogramFacet first = (InternalDateHistogramFacet) facets.get(0);
        return first.reduce(name, facets);
    }

    static interface DateFieldParser {

        DateTimeField parse(Chronology chronology);

        static class WeekOfWeekyear implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.weekOfWeekyear();
            }
        }

        static class YearOfCentury implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.yearOfCentury();
            }
        }

        static class MonthOfYear implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.monthOfYear();
            }
        }

        static class DayOfMonth implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.dayOfMonth();
            }
        }

        static class HourOfDay implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.hourOfDay();
            }
        }

        static class MinuteOfHour implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.minuteOfHour();
            }
        }

        static class SecondOfMinute implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.secondOfMinute();
            }
        }
    }
}
