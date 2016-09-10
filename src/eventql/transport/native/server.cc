/**
 * Copyright (c) 2016 DeepCortex GmbH <legal@eventql.io>
 * Authors:
 *   - Paul Asmuth <paul@eventql.io>
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
#include "eventql/transport/native/server.h"
#include "eventql/transport/native/connection_tcp.h"
#include "eventql/transport/native/frames/query_result.h"
#include "eventql/util/logging.h"
#include "eventql/util/util/binarymessagereader.h"
#include "eventql/server/session.h"
#include "eventql/server/sql_service.h"
#include "eventql/server/rpc/partial_aggregate.h"
#include "eventql/sql/runtime/runtime.h"
#include "eventql/auth/client_auth.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <limits.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <netinet/tcp.h>

namespace eventql {
namespace native_transport {

Server::Server(Database* db) : db_(db) {}

void Server::startConnection(std::unique_ptr<NativeConnection> connection) {
  auto conn_ptr = connection.release();
  db_->startThread([this, conn_ptr] (Session* session) {
    std::unique_ptr<NativeConnection> conn(conn_ptr);

    auto rc = performHandshake(conn.get());
    if (!rc.isSuccess()) {
      logError("eventql", "Handshake error: $0", rc.getMessage());
      conn->close();
      return;
    }

    logDebug(
        "eventql",
        "Native connection established; id=$0",
        (const void*) conn.get());

    uint16_t opcode;
    std::string payload;
    bool cont = true;
    while (cont && rc.isSuccess()) {
      rc = conn->recvFrame(&opcode, &payload);
      if (!rc.isSuccess()) {
        break;
      }

      switch (opcode) {
        case EVQL_OP_BYE:
          cont = false;
          break;
        default:
          rc = performOperation(conn.get(), opcode, payload);
          break;
      }
    }

    conn->close();
  });
}

ReturnCode Server::performHandshake(NativeConnection* conn) {
  /* read HELLO frame */
  {
    uint16_t opcode;
    std::string payload;
    auto rc = conn->recvFrame(&opcode, &payload);
    if (!rc.isSuccess()) {
      conn->close();
      return rc;
    }

    switch (opcode) {
      case EVQL_OP_HELLO:
        break;
      default:
        conn->sendErrorFrame("invalid opcode");
        conn->close();
        return ReturnCode::error("ERUNTIME", "invalid opcode");
    }
  }

  /* send READY frame */
  {
    auto rc = conn->sendFrame(EVQL_OP_READY, nullptr, 0);
    if (!rc.isSuccess()) {
      conn->close();
      return rc;
    }
  }

  return ReturnCode::success();
}

ReturnCode Server::performOperation(
    NativeConnection* conn,
    uint16_t opcode,
    const std::string& payload) {
  logDebug("eventql", "Performing operation; opcode=$0", opcode);

  switch (opcode) {
    case EVQL_OP_QUERY:
      return performOperation_QUERY(db_, conn, payload);
    case EVQL_OP_QUERY_PARTIALAGGR:
      return performOperation_QUERY_PARTIALAGGR(
          db_,
          conn,
          payload.data(),
          payload.size());
    default:
      conn->sendErrorFrame("invalid opcode");
      conn->close();
      return ReturnCode::error("ERUNTIME", "invalid opcode");
  }

  return ReturnCode::success();
}

} // namespace native_transport
} // namespace eventql

