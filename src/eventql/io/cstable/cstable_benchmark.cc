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

  logInfo(
      "cstable-benchmark",
      "copying $0 to $1.cst",
      input_cstable_file,
      cstable_filepath);

  auto input_cstable = cstable::CSTableReader::openFile(input_cstable_file);
  auto cstable = cstable::CSTableWriter::createFile(
      cstable_filepath + ".cst",
      cstable::BinaryFormatVersion::v0_2_0,
      input_cstable->columns());

  size_t rows_written = 0;

  try {
    Vector<Pair<
        RefPtr<cstable::ColumnReader>,
        RefPtr<cstable::ColumnWriter>>> columns;
    for (const auto& col : input_cstable->columns()) {
      columns.emplace_back(
              input_cstable->getColumnReader(col.column_name),
              cstable->getColumnWriter(col.column_name));
    }

    auto nrecords = input_cstable->numRecords();
    for (size_t i = 0; i < nrecords; ++i) {
     // String id_str;
     // input_id_col->readString(&rlvl, &dlvl, &id_str);

      for (auto& col : columns) {
        if (col.first.get()) {
          do {
            col.first->copyValue(col.second.get());
          } while (col.first->nextRepetitionLevel() > 0);
        } else {
          col.second->writeNull(0, 0);
        }
      }

      cstable->addRow();
      ++rows_written;
    }

  } catch (const std::exception& e) {
    logError(
        "cstable-benchmark",
        "error while copying table: $0 -- $1",
        input_cstable_file,
        e.what());

    return false;
  }

  cstable->commit();
  return true;
}

int main(int argc, const char** argv) {
  Application::init();
  Application::logToStderr("cstable-benchmark");

  logInfo("cstable-benchmark", "Benchmarking CSTable copy...");

  auto cycles = 10;
  auto input_cstable_file = argv[1];
  for (size_t i = 0; i < cycles; ++i) {
    if (!copyTable(input_cstable_file)) {
      return 1;
    }
  }

  logInfo("cstable-benchmark", "Copied cstable $ times", cycles);
  return 0;
}

