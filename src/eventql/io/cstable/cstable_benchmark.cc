/**
 * Copyright (c) 2016 zScale Technology GmbH <legal@zscale.io>
 * Authors:
 *   - Paul Asmuth <paul@zscale.io>
 *   - Laura Schlimmer <laura@zscale.io>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License ("the license") as
 * published by the Free Software Foundation, either version 3 of the License,
 * or any later version.
 *
 * In accordance with Section 7(e) of the license, the licensing of the Program
 * under the license does not imply a trademark license. Therefore any rights,
 * title and interest in our trademarks remain entirely with us.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the license for more details.
 *
 * You can be released from the requirements of the license by purchasing a
 * commercial license. Buying such a license is mandatory as soon as you develop
 * commercial activities involving this program without disclosing the source
 * code of your own applications
 */
#include "eventql/util/stdtypes.h"
#include "eventql/util/application.h"
#include "eventql/util/cli/flagparser.h"
#include "eventql/util/io/file.h"
#include "eventql/util/io/fileutil.h"
#include "eventql/util/inspect.h"
#include "eventql/util/random.h"
#include <eventql/io/cstable/cstable_reader.h>
#include <eventql/io/cstable/cstable_writer.h>
#include "eventql/eventql.h"

static bool copyTable(
    const String& cstable_filepath,
    const String& input_cstable_file) {

  auto input_cstable = cstable::CSTableReader::openFile(input_cstable_file);
  auto cstable = cstable::CSTableWriter::createFile(
      cstable_filepath,
      cstable::BinaryFormatVersion::v0_2_0,
      input_cstable->columns());

  auto nrecords = input_cstable->numRecords();
  Vector<bool> copy_record(nrecords);
  for (size_t i = 0; i < nrecords; ++i) {
    copy_record[i] = true;
  }

  try {
    input_cstable->copyTo(cstable, input_cstable->columns(), copy_record);

  } catch (const std::exception& e) {
    logError(
        "cstable-benchmark",
        "error while copying table: $0 -- $1",
        input_cstable_file,
        e.what());

    return false;
  }

  return true;
}

int main(int argc, const char** argv) {
  Application::init();
  Application::logToStderr("cstable-benchmark");

  cli::FlagParser flags;

  flags.defineFlag(
      "input_cstable",
      cli::FlagParser::T_STRING,
      true,
      "t",
      NULL,
      "path to input cstable",
      "<input_cstable>");

  flags.defineFlag(
      "cycles",
      cli::FlagParser::T_INTEGER,
      false,
      "c",
      "10",
      "number of execution cycles",
      "<cycles>");

  flags.parseArgv(argc, argv);

  logInfo("cstable-benchmark", "Benchmarking CSTable copy...");

  auto input_cstable_file = flags.getString("input_cstable");
  if (!FileUtil::exists(input_cstable_file)) {
    logError("cstable-benchmark", "missing table file: $0", input_cstable_file);
    return false;
  }

  auto cycles = flags.getInt("cycles");
  if (cycles < 1) {
    logError("cstable-benchmark", "at least one cycle");
    return 1;
  }

  Vector<uint64_t> durations;
  Vector<double> bandwidths;
  uint64_t total_duration = 0;
  uint64_t total_size = 0;
  size_t num_errors = 0;
  for (size_t i = 0; i < cycles; ++i) {
    auto cstable_filepath = FileUtil::joinPaths(
        "/tmp",
        StringUtil::format(
            "$0.cst",
            Random::singleton()->hex64()));

    UnixTime start;
    if (!copyTable(cstable_filepath, input_cstable_file)) {
      ++num_errors;
      continue;
    }

    auto duration = (UnixTime() - start).milliseconds();
    durations.emplace_back(duration);
    total_duration += duration;

    auto size = (double) FileUtil::size(cstable_filepath) / 1024 / 1024;
    bandwidths.emplace_back(size / (duration));
    total_size += size;
    iputs("size $0, seconds $1", size, duration);

    FileUtil::rm(cstable_filepath);
  }

  auto stdout_os = OutputStream::getStdout();
  stdout_os->printf("%-26s", "Successful copies:");
  stdout_os->write(StringUtil::format("$0\n", cycles - num_errors));
  stdout_os->printf("%-26s", "Failed copies:");
  stdout_os->write(StringUtil::format("$0\n", num_errors));
  stdout_os->printf("%-26s", "Total time:");
  stdout_os->write(StringUtil::format(
      "$0 seconds\n",
      (double) total_duration / kMillisPerSecond));
  stdout_os->printf("%-26s", "Total copied:");
  stdout_os->write(StringUtil::format("$0 MB\n\n", total_size));

  {
    sort(durations.begin(), durations.end());
    auto ncycles = durations.size();
    auto min = (double) durations[0] / kMillisPerSecond;
    auto max = (double) durations[ncycles - 1] / kMillisPerSecond;
    auto mean = (double) (total_duration / ncycles) / kMillisPerSecond;
    double median;
    if (ncycles % 2 == 0) {
      median = (double) ((durations[ncycles / 2 - 1] + durations[ncycles / 2]) / 2) / kMillisPerSecond;
    } else {
      median = (double) durations[ncycles / 2] / kMillisPerSecond;
    }

    stdout_os->write("Times (s)\n");
    stdout_os->printf("%-8s %-8s %-8s %-8s\n", "min", "mean", "median", "max");
    stdout_os->printf(
        "%-8s %-8s %-8s %-8s\n\n",
        StringUtil::toString(min).c_str(),
        StringUtil::toString(mean).c_str(),
        StringUtil::toString(median).c_str(),
        StringUtil::toString(max).c_str());
  }

  {
    sort(bandwidths.begin(), bandwidths.end());
    auto ncycles = bandwidths.size();
    double min = bandwidths[0] * kMillisPerSecond;
    double max = bandwidths[ncycles - 1] * kMillisPerSecond;
    auto mean = ((double) total_size / total_duration) * kMillisPerSecond;
    double median;
    if (ncycles % 2 == 0) {
      median = (bandwidths[ncycles / 2 - 1] + bandwidths[ncycles / 2]) / 2 * kMillisPerSecond;
    } else {
      median = bandwidths[ncycles / 2] * kMillisPerSecond;
    }

    stdout_os->printf("%-26s\n", "Bandwith (MB/s):");
    stdout_os->printf("%-8s %-8s %-8s %-8s\n", "min", "mean", "median", "max");
    stdout_os->printf(
        "%-8s %-8s %-8s %-8s\n",
        StringUtil::toString(min).c_str(),
        StringUtil::toString(mean).c_str(),
        StringUtil::toString(median).c_str(),
        StringUtil::toString(max).c_str());
  }

  return 0;
}

