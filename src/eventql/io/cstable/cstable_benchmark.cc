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

int main(int argc, const char** argv) {
  Application::init();
  Application::logToStderr("evql");


  auto base_path = argv[1];
  String file_name = argv[2];

  auto input_cstable_file = FileUtil::joinPaths(
        base_path,
        file_name + ".cst");

  if (!FileUtil::exists(input_cstable_file)) {
    logError("evqld", "missing table file: $0", input_cstable_file);
    return 1;
  }

  auto cstable_filename = Random::singleton()->hex64();
  auto cstable_filepath = FileUtil::joinPaths(base_path, cstable_filename);

  //cstable::TableSchema::fromProtobuf(*table->schema());
  cstable::TableSchema cstable_schema_ext;
  cstable_schema_ext.addBool("__lsm_is_update", false);
  cstable_schema_ext.addString("__lsm_id", false);
  cstable_schema_ext.addUnsignedInteger("__lsm_version", false);
  cstable_schema_ext.addUnsignedInteger("__lsm_sequence", false);

  auto cstable = cstable::CSTableWriter::createFile(
      cstable_filepath + ".cst",
      cstable::BinaryFormatVersion::v0_2_0,
      cstable_schema_ext);

  auto is_update_col = cstable->getColumnWriter("__lsm_is_update");
  auto id_col = cstable->getColumnWriter("__lsm_id");
  auto version_col = cstable->getColumnWriter("__lsm_version");
  auto sequence_col = cstable->getColumnWriter("__lsm_sequence");
  size_t rows_written = 0;
  size_t rows_skipped = 0;

  try {
    auto input_cstable = cstable::CSTableReader::openFile(input_cstable_file);
    auto input_id_col = input_cstable->getColumnReader("__lsm_id");
    auto input_version_col = input_cstable->getColumnReader("__lsm_version");
    auto input_sequence_col = input_cstable->getColumnReader("__lsm_sequence");

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
      uint64_t rlvl;
      uint64_t dlvl;

      String id_str;
      input_id_col->readString(&rlvl, &dlvl, &id_str);
      SHA1Hash id(id_str.data(), id_str.size());
      uint64_t version;
      input_version_col->readUnsignedInt(&rlvl, &dlvl, &version);

      uint64_t sequence;
      input_sequence_col->readUnsignedInt(&rlvl, &dlvl, &sequence);

      is_update_col->writeBoolean(0, 0, false);
      id_col->writeString(0, 0, id_str);
      version_col->writeUnsignedInt(0, 0, version);
      sequence_col->writeUnsignedInt(0, 0, sequence);

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
        "evqld",
        "error while compacting table: $0 -- $1",
        input_cstable_file,
        e.what());

    return 1;
  }

  cstable->commit();


  return 0;
}
