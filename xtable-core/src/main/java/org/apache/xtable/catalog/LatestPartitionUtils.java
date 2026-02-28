/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.catalog;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LatestPartitionUtils {

  public static final String LBC_FIRST_EVENT_PARTITION_PROPERTY = "lbc.first_event_partition";
  public static final String LBC_LAST_EVENT_PARTITION_PROPERTY = "lbc.last_event_partition";

  private static final Pattern DATE_HOUR_PARTITION_PATTERN =
      Pattern.compile(
          "([A-Za-z0-9_-]+_date=(\\d{4}-\\d{2}-\\d{2}))/([A-Za-z0-9_-]+_hour=(\\d{2}))(?=/|$)");

  public static Optional<PartitionTimestampBounds> getDateHourPartitionTimestampBounds(
      Collection<String> partitionPaths) {
    if (partitionPaths == null || partitionPaths.isEmpty()) {
      return Optional.empty();
    }

    Optional<DateHourPartition> firstPartition =
        partitionPaths.stream()
            .map(LatestPartitionUtils::parsePartitionPath)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .min(
                Comparator.comparing(DateHourPartition::getDate)
                    .thenComparing(DateHourPartition::getHour));
    Optional<DateHourPartition> lastPartition =
        partitionPaths.stream()
            .map(LatestPartitionUtils::parsePartitionPath)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .max(
                Comparator.comparing(DateHourPartition::getDate)
                    .thenComparing(DateHourPartition::getHour));

    if (!firstPartition.isPresent() || !lastPartition.isPresent()) {
      return Optional.empty();
    }

    return Optional.of(
        new PartitionTimestampBounds(
            toUtcHourString(firstPartition.get()), toUtcHourString(lastPartition.get())));
  }

  private static String toUtcHourString(DateHourPartition partition) {
    return LocalDateTime.of(partition.getDate(), java.time.LocalTime.of(partition.getHour(), 0))
        .toInstant(ZoneOffset.UTC)
        .toString();
  }

  private static Optional<DateHourPartition> parsePartitionPath(String partitionPath) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      return Optional.empty();
    }
    Matcher matcher = DATE_HOUR_PARTITION_PATTERN.matcher(partitionPath);
    if (!matcher.find()) {
      return Optional.empty();
    }

    try {
      LocalDate date = LocalDate.parse(matcher.group(2));
      int hour = Integer.parseInt(matcher.group(4));
      if (hour < 0 || hour > 23) {
        return Optional.empty();
      }
      return Optional.of(new DateHourPartition(date, hour));
    } catch (DateTimeParseException | NumberFormatException ex) {
      return Optional.empty();
    }
  }

  private static final class DateHourPartition {
    private final LocalDate date;
    private final int hour;

    private DateHourPartition(LocalDate date, int hour) {
      this.date = date;
      this.hour = hour;
    }

    private LocalDate getDate() {
      return date;
    }

    private int getHour() {
      return hour;
    }
  }

  @Getter
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static final class PartitionTimestampBounds {
    private final String firstTimestamp;
    private final String lastTimestamp;
  }
}
