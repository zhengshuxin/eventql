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

static bool copyTable(const String& input_cstable_file) {
  if (!FileUtil::exists(input_cstable_file)) {
    logError("cstable-benchmark", "missing table file: $0", input_cstable_file);
    return false;
  }

  auto cstable_filename = Random::singleton()->hex64();
  auto cstable_filepath = FileUtil::joinPaths("/tmp", cstable_filename);

  auto input_cstable = cstable::CSTableReader::openFile(input_cstable_file);
  auto cstable = cstable::CSTableWriter::createFile(
      cstable_filepath + ".cst",
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

  FileUtil::rm(StringUtil::format("$0.cst", cstable_filepath));
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
  auto cycles = flags.getInt("cycles");

  if (cycles < 1) {
    logError("cstable-benchmark", "at least one cycle");
    return 1;
  }

  Vector<uint64_t> times;
  size_t num_errors = 0;
  uint64_t total_time = 0;
  for (size_t i = 0; i < cycles; ++i) {
    UnixTime start;
    if (!copyTable(input_cstable_file)) {
      ++num_errors;
      continue;
    }
    auto time = UnixTime() - start;
    times.emplace_back(time.milliseconds());
    total_time += time.milliseconds();
  }

  sort(times.begin(), times.end());
  auto stdout_os = OutputStream::getStdout();

  uint64_t median;
  auto ntimes = times.size();
  if (ntimes % 2 == 0) {
    median = (times[ntimes / 2 - 1] + times[ntimes / 2]) / 2;
  } else {
    median = times[ntimes / 2];
  }

  stdout_os->printf("%-26s", "Total time:");
  stdout_os->write(StringUtil::format("$0s\n", total_time / kMillisPerSecond));
  stdout_os->printf("%-26s", "Successful copies:");
  stdout_os->write(StringUtil::format("$0\n", cycles - num_errors));
  stdout_os->printf("%-26s", "Failed copies:");
  stdout_os->write(StringUtil::format("$0\n\n", num_errors));
  stdout_os->write("Copy Times (ms)\n");
  stdout_os->printf("%-8s %-8s %-8s %-8s\n", "min", "mean", "median", "max");
  stdout_os->printf(
      "%-8s %-8s %-8s %-8s\n",
      StringUtil::toString(times[0]).c_str(),
      StringUtil::toString(total_time / cycles).c_str(),
      StringUtil::toString(median).c_str(),
      StringUtil::toString(times[times.size() - 1]).c_str());

  return 0;
}

