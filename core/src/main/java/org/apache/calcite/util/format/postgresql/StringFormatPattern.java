/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.util.format.postgresql;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.ParsePosition;
import java.time.ZonedDateTime;
import java.util.Locale;

/**
 * A format element that will produce a string. The "FM" prefix and "TH"/"th" suffixes
 * will be silently consumed when the pattern matches.
 */
public abstract class StringFormatPattern extends FormatPattern {
  private final ChronoUnitEnum chronoUnit;

  /**
   * Constructs a new StringFormatPattern for the provide list of pattern strings and
   * ChronoUnitEnum value. Child classes must use this constructor.
   *
   * @param chronoUnit ChronoUnitEnum value that this pattern parses
   * @param patterns array of pattern strings
   */
  protected StringFormatPattern(ChronoUnitEnum chronoUnit, String... patterns) {
    super(patterns);
    this.chronoUnit = chronoUnit;
  }

  @Override public @Nullable String convert(ParsePosition parsePosition, String formatString,
      ZonedDateTime dateTime, Locale locale) {
    String formatStringTrimmed = formatString.substring(parsePosition.getIndex());

    boolean haveFillMode = false;
    boolean haveTranslationMode = false;
    if (formatStringTrimmed.startsWith("FMTM") || formatStringTrimmed.startsWith("TMFM")) {
      haveFillMode = true;
      haveTranslationMode = true;
      formatStringTrimmed = formatStringTrimmed.substring(4);
    } else if (formatStringTrimmed.startsWith("FM")) {
      haveFillMode = true;
      formatStringTrimmed = formatStringTrimmed.substring(2);
    } else if (formatStringTrimmed.startsWith("TM")) {
      haveTranslationMode = true;
      formatStringTrimmed = formatStringTrimmed.substring(2);
    }

    String patternToUse = null;
    for (String pattern : getPatterns()) {
      if (formatStringTrimmed.startsWith(pattern)) {
        patternToUse = pattern;
        break;
      }
    }

    if (patternToUse == null) {
      return null;
    }

    formatStringTrimmed = formatStringTrimmed.substring(patternToUse.length());
    final String suffix;
    if (formatStringTrimmed.startsWith("TH") || formatStringTrimmed.startsWith("th")) {
      suffix = formatStringTrimmed.substring(0, 2);
    } else {
      suffix = null;
    }

    parsePosition.setIndex(parsePosition.getIndex() + patternToUse.length()
        + (haveFillMode ? 2 : 0) + (haveTranslationMode ? 2 : 0)
        + (suffix != null ? suffix.length() : 0));
    return dateTimeToString(
        dateTime,
        haveFillMode,
        suffix,
        haveTranslationMode ? locale : Locale.US);
  }

  @Override public ChronoUnitEnum getChronoUnit() {
    return chronoUnit;
  }

  /**
   * Extracts the datetime component from the provided datetime and formats it. This may
   * also involve translation to the provided locale.
   *
   * @param dateTime extract the datetime component from here
   * @param haveFillMode is fill mode enabled
   * @param suffix suffix modifier if any (TH or th)
   * @param locale locale to translate to
   * @return formatted string representation of datetime component
   */
  protected abstract String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale);

  @Override protected boolean isNumeric() {
    return false;
  }
}
