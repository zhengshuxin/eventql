/**
 * Copyright (c) 2016 DeepCortex GmbH <legal@eventql.io>
 * Authors:
 *   - Paul Asmuth <paul@eventql.io>
 *   - Laura Schlimmer <laura@eventql.io>
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
#pragma once
#include "eventql/eventql.h"
#include <eventql/util/stdtypes.h>
#include <eventql/sql/table_schema.h>
#include <eventql/sql/qtree/QueryTreeNode.h>

namespace csql {

class CreateTableNode : public QueryTreeNode {
public:

  CreateTableNode(const String& table_name, TableSchema table_schema);
  CreateTableNode(const CreateTableNode& node);

  const String& getTableName() const;
  const TableSchema& getTableSchema() const;

  const Vector<String> getPrimaryKey() const;
  void setPrimaryKey(const Vector<String>& columns);

  RefPtr<QueryTreeNode> deepCopy() const;
  String toString() const;

  const std::vector<std::pair<std::string, std::string>>& getProperties() const;
  void addProperty(const std::string& key, const std::string& value);

protected:
  String table_name_;
  TableSchema table_schema_;
  Vector<String> primary_key_;
  std::vector<std::pair<std::string, std::string>> properties_;
};

} // namespace csql

