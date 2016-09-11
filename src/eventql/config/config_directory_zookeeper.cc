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
#include <regex>
#include <zookeeper.h>
#include <eventql/config/config_directory_zookeeper.h>
#include <eventql/util/protobuf/msg.h>

namespace eventql {

static int free_String_vector(struct String_vector *v) {
  if (v->data) {
    int32_t i;
    for(i=0;i<v->count; i++) {
      free(v->data[i]);
    }
    free(v->data);
    v->data = 0;
  }
  return 0;
}

ZookeeperConfigDirectory::ZookeeperConfigDirectory(
    const String& zookeeper_addrs,
    const String& cluster_name,
    Option<String> server_name,
    String listen_addr) :
    zookeeper_addrs_(zookeeper_addrs),
    zookeeper_timeout_(10000),
    cluster_name_(cluster_name),
    server_name_(server_name),
    listen_addr_(listen_addr),
    global_prefix_("/eventql"),
    state_(ZKState::INIT),
    zk_(nullptr),
    is_leader_(false) {
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
}

ZookeeperConfigDirectory::~ZookeeperConfigDirectory() {
  if (zk_) {
    zookeeper_close(zk_);
  }
}

static void zk_watch_cb(
    zhandle_t* zk,
    int type,
    int state,
    const char* path,
    void* ctx) {
  auto self = (ZookeeperConfigDirectory*) zoo_get_context(zk);
  assert(self != nullptr);
  self->handleZookeeperWatch(type, state, path);
}

Status ZookeeperConfigDirectory::start(bool create /* = false */) {
  std::unique_lock<std::mutex> lk(mutex_);
  if (state_ != ZKState::INIT) {
    return Status(eIllegalStateError, "state != ZK_INIT");
  }

  path_prefix_ = StringUtil::format("$0/$1", global_prefix_, cluster_name_);

  {
    auto rc = connect(&lk);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  {
    auto rc = load(create);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  return Status::success();
}

Status ZookeeperConfigDirectory::connect(std::unique_lock<std::mutex>* lk) {
  state_ = ZKState::CONNECTING;

  zk_ = zookeeper_init(
      zookeeper_addrs_.c_str(),
      &zk_watch_cb,
      zookeeper_timeout_,
      0, /* client id */
      this,
      0 /* flags */);

  if (!zk_) {
    return Status(eIOError, "zookeeper_init failed");
  }

  while (state_ < ZKState::LOADING) {
    logInfo("evqld", "Waiting for zookeeper ($0)", zookeeper_addrs_);
    cv_.wait_for(*lk, std::chrono::seconds(1));
  }

  if (!hasNode(global_prefix_)) {
    auto rc = zoo_create(
        zk_,
        global_prefix_.c_str(),
        nullptr,
        0,
        &ZOO_OPEN_ACL_UNSAFE,
        0,
        nullptr, /* path_buffer */
        0 /* path_buffer_len */);

    if (rc) {
      return Status(
          eIOError,
          StringUtil::format("zoo_create() failed: $0", getErrorString(rc)));
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::load(bool create) {
  if (!hasNode(path_prefix_ + "/config")) {
    if (create) {
      auto rc = zoo_create(
          zk_,
          path_prefix_.c_str(),
          nullptr,
          0,
          &ZOO_OPEN_ACL_UNSAFE,
          0,
          nullptr, /* path_buffer */
          0 /* path_buffer_len */);

      if (rc && rc != ZNODEEXISTS) {
        return Status(
            eIOError,
            StringUtil::format("zoo_create() failed: $0", getErrorString(rc)));
      }

      String config_path = path_prefix_ + "/config";
      rc = zoo_create(
          zk_,
          config_path.c_str(),
          nullptr,
          0,
          &ZOO_OPEN_ACL_UNSAFE,
          0,
          nullptr, /* path_buffer */
          0 /* path_buffer_len */);

      if (rc && rc != ZNODEEXISTS) {
        return Status(
            eIOError,
            StringUtil::format("zoo_create() failed: $0", getErrorString(rc)));
      }
    } else {
      return Status(
          eIOError,
          StringUtil::format("cluster doesn't exit: $0", cluster_name_));
    }
  }

  auto namespaces_path = StringUtil::format("$0/namespaces", path_prefix_);
  if (!hasNode(namespaces_path)) {
    auto rc = zoo_create(
        zk_,
        namespaces_path.c_str(),
        nullptr,
        0,
        &ZOO_OPEN_ACL_UNSAFE,
        0,
        nullptr, /* path_buffer */
        0 /* path_buffer_len */);

    if (rc && rc != ZNODEEXISTS) {
      return Status(
          eIOError,
          StringUtil::format("zoo_create() failed: $0", getErrorString(rc)));
    }
  }

  auto servers_path = StringUtil::format("$0/servers", path_prefix_);
  if (!hasNode(servers_path)) {
    auto rc = zoo_create(
        zk_,
        servers_path.c_str(),
        nullptr,
        0,
        &ZOO_OPEN_ACL_UNSAFE,
        0,
        nullptr, /* path_buffer */
        0 /* path_buffer_len */);

    if (rc && rc != ZNODEEXISTS) {
      return Status(
          eIOError,
          StringUtil::format("zoo_create() failed: $0", getErrorString(rc)));
    }
  }

  auto servers_live_path = StringUtil::format("$0/servers-live", path_prefix_);
  if (!hasNode(servers_live_path)) {
    auto rc = zoo_create(
        zk_,
        servers_live_path.c_str(),
        nullptr,
        0,
        &ZOO_OPEN_ACL_UNSAFE,
        0,
        nullptr, /* path_buffer */
        0 /* path_buffer_len */);

    if (rc && rc != ZNODEEXISTS) {
      return Status(
          eIOError,
          StringUtil::format("zoo_create() failed: $0", getErrorString(rc)));
    }
  }

  if (!server_name_.isEmpty()) {
    logInfo("evqld", "Registering with zookeeper...");
    auto server_cfg_path = StringUtil::format(
        "$0/servers/$1",
        path_prefix_,
        server_name_.get());

    if (!hasNode(server_cfg_path)) {
      return Status(
          eIOError,
          StringUtil::format("server doesn't exit: $0", server_name_.get()));
    }

    auto server_path = servers_live_path + "/" + server_name_.get();
    auto rc = zoo_create(
        zk_,
        server_path.c_str(),
        listen_addr_.data(),
        listen_addr_.size(),
        &ZOO_OPEN_ACL_UNSAFE,
        ZOO_EPHEMERAL,
        nullptr, /* path_buffer */
        0 /* path_buffer_len */);

    if (rc) {
      return Status(
          eIOError,
          StringUtil::format("zoo_create() failed: $0", getErrorString(rc)));
    }
  }

  logInfo("evqld", "Loading config from zookeeper...");

  {
    auto rc = syncClusterConfig(nullptr);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  {
    auto rc = syncLiveServers(nullptr);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  {
    auto rc = syncServers(nullptr);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  {
    auto rc = syncNamespaces(nullptr);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  state_ = ZKState::CONNECTED;
  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncClusterConfig(CallbackList* events) {
  struct Stat stat;
  if (getProtoNode(path_prefix_ + "/config", &cluster_config_, true, &stat)) {
    cluster_config_.set_version(stat.version + 1);
    if (events) {
      for (const auto& cb : on_cluster_change_) {
        events->emplace_back(std::bind(cb, getPatchedClusterConfig()));
      }
    }

    return Status::success();
  } else {
    return Status(
        eNotFoundError,
        StringUtil::format(
            "Cluster '$0' does not exist. Start with --create_cluster to create",
            cluster_name_));
  }
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncLiveServers(CallbackList* events) {
  Vector<String> servers;
  {
    auto rc = listChildren(path_prefix_ + "/servers-live", &servers, true);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  Set<String> all_servers(servers.begin(), servers.end());
  for (const auto& s : servers_live_) {
    all_servers.emplace(s.first);
  }

  for (const auto& server : all_servers) {
    auto rc = syncLiveServer(events, server);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncLiveServer(
    CallbackList* events,
    const String& server) {
  auto key = StringUtil::format("$0/servers-live/$1", path_prefix_, server);
  bool exists = hasNode(key);
  Buffer server_addr(1024);
  if (exists) {
    if (!getNode(key, &server_addr, true)) {
      return Status(
          eNotFoundError,
          StringUtil::format("server '$0' does not exist", server));
    }

    servers_live_[server] = server_addr.toString();
  } else {
    servers_live_.erase(server);
  }

  if (events) {
    const auto& iter = servers_.find(server);
    if (iter == servers_.end()) {
      return Status(
          eNotFoundError,
          StringUtil::format("server '$0' does not exist", server));
    }

    auto server_config = iter->second;
    if (exists) {
      server_config.set_server_status(SERVER_UP);
      server_config.set_server_addr(server_addr.toString());
    } else {
      server_config.set_server_status(SERVER_DOWN);
    }

    for (const auto& cb : on_server_change_) {
      events->emplace_back(std::bind(cb, server_config));
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncServers(CallbackList* events) {
  Vector<String> servers;
  {
    auto rc = listChildren(path_prefix_ + "/servers", &servers, true);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  for (const auto& server : servers) {
    auto rc = syncServer(events, server);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncServer(
    CallbackList* events,
    const String& server) {
  logDebug("evqld", "Loading server config from zookeeper: '$0'", server);

  auto key = StringUtil::format("$0/servers/$1", path_prefix_, server);
  ServerConfig server_config;
  struct Stat stat;
  if (!getProtoNode(key, &server_config, true, &stat)) {
    return Status(
        eNotFoundError,
        StringUtil::format("server '$0' does not exist", server));
  }

  server_config.set_version(stat.version + 1);
  servers_[server] = server_config;

  const auto& live_server = servers_live_.find(server);
  if (live_server != servers_live_.end()) {
    server_config.set_server_status(SERVER_UP);
    server_config.set_server_addr(live_server->second);
  } else {
    server_config.set_server_status(SERVER_DOWN);
  }

  if (events) {
    for (const auto& cb : on_server_change_) {
      events->emplace_back(std::bind(cb, server_config));
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncNamespaces(CallbackList* events) {
  Vector<String> namespaces;
  {
    auto rc = listChildren(path_prefix_ + "/namespaces", &namespaces, true);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  for (const auto& ns : namespaces) {
    auto rc = syncNamespace(events, ns);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncNamespace(
    CallbackList* events,
    const String& ns) {
  logDebug("evqld", "Loading namespace config from zookeeper: '$0'", ns);

  {
    auto key = StringUtil::format("$0/namespaces/$1/config", path_prefix_, ns);
    NamespaceConfig ns_config;
    struct Stat stat;
    if (!getProtoNode(key, &ns_config, true, &stat)) {
      return Status(
          eNotFoundError,
          StringUtil::format("namespace '$0' does not exist", ns));
    }

    ns_config.set_version(stat.version + 1);
    namespaces_[ns] = ns_config;

    if (events) {
      for (const auto& cb : on_namespace_change_) {
        events->emplace_back(std::bind(cb, ns_config));
      }
    }
  }

  {
    auto rc = syncTables(events, ns);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncTables(
    CallbackList* events,
    const String& ns) {
  Vector<String> tables;

  {
    auto key = StringUtil::format("$0/namespaces/$1/tables", path_prefix_, ns);
    auto rc = listChildren(key, &tables, true);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  for (const auto& table : tables) {
    auto rc = syncTable(events, ns, table);
    if (!rc.isSuccess()) {
      return rc;
    }
  }

  return Status::success();
}

// PRECONDITION: must hold mutex
Status ZookeeperConfigDirectory::syncTable(
    CallbackList* events,
    const String& ns,
    const String& table_name) {
  logDebug(
      "evqld",
      "Loading table config from zookeeper: '$0/$1'",
      ns,
      table_name);

  auto key = StringUtil::format(
      "$0/namespaces/$1/tables/$2",
      path_prefix_,
      ns,
      table_name);

  TableDefinition tbl_config;
  struct Stat stat;
  if (!getProtoNode(key, &tbl_config, true, &stat)) {
    return Status(
        eNotFoundError,
        StringUtil::format("table '$0/$1' does not exist", ns, table_name));
  }

  tbl_config.set_version(stat.version + 1);
  tables_[ns + "~" + table_name] = tbl_config;

  if (events) {
    for (const auto& cb : on_table_change_) {
      events->emplace_back(std::bind(cb, tbl_config));
    }
  }

  return Status::success();
}

// PRECONDITION: must NOT hold mutex
void ZookeeperConfigDirectory::stop() {
  zookeeper_close(zk_);
  zk_ = nullptr;
}

bool ZookeeperConfigDirectory::getNode(
    String key,
    Buffer* buf,
    bool watch /* = false */,
    struct Stat* stat /* = nullptr */) const {
  int buf_len = buf->size();

  auto rc = zoo_get(
      zk_,
      key.c_str(),
      watch,
      (char*) buf->data(),
      &buf_len,
      stat);

  if (rc) {
    return false;
  }

  if (buf_len > 0) {
    buf->resize(buf_len);
  } else {
    buf->resize(0);
  }

  return true;
}

bool ZookeeperConfigDirectory::hasNode(String key) {
  char buf[1];
  int buf_len = 1;

  auto rc = zoo_get(
      zk_,
      key.c_str(),
      0, /* watch */
      buf,
      &buf_len,
      nullptr /* stat */);

  if (rc == 0) {
    return true;
  } else if (rc == ZNONODE) {
    return false;
  } else {
    RAISEF(kRuntimeError, "zoo_get() failed: $0", getErrorString(rc));
  }
}

Status ZookeeperConfigDirectory::listChildren(
    String key,
    Vector<String>* children,
    bool watch /* = false */) {
  struct String_vector buf;
  buf.count = 0;
  buf.data = NULL;

  int rc = zoo_get_children(zk_, key.c_str(), watch, &buf);
  for (int i = 0; i < buf.count; ++i) {
    children->emplace_back(buf.data[i]);
  }
  free_String_vector(&buf);

  if (rc) {
    return Status(eIOError, getErrorString(rc));
  } else {
    return Status::success();
  }
}

void ZookeeperConfigDirectory::handleZookeeperWatch(
    int type,
    int state,
    String path) {
  std::unique_lock<std::mutex> lk(mutex_);
  CallbackList events;

  if (StringUtil::beginsWith(path, path_prefix_)) {
    path = path.substr(path_prefix_.size());
  }

  if (type == ZOO_SESSION_EVENT) {
    handleSessionEvent(state);
  }

  if (type == ZOO_CHILD_EVENT ||
      type == ZOO_CHANGED_EVENT) {
    handleChangeEvent(path, &events);
  }

  cv_.notify_all();
  lk.unlock();

  for (const auto& ev : events) {
    ev();
  }
}

Status ZookeeperConfigDirectory::handleChangeEvent(
    const String& vpath,
    CallbackList* events) {
  std::smatch m;

  if (vpath == "/config") {
    return syncClusterConfig(events);
  }

  if (vpath == "/namespaces") {
    return syncNamespaces(events);
  }

  if (vpath == "/servers") {
    return syncServers(events);
  }

  if (vpath == "/servers-live") {
    return syncLiveServers(events);
  }

  std::regex live_server_regex("/servers-live/([0-9A-Za-z_.-]+)");
  if (std::regex_match(vpath, m, live_server_regex)) {
    return syncLiveServer(events, m[1].str());
  }

  std::regex server_regex("/servers/([0-9A-Za-z_.-]+)");
  if (std::regex_match(vpath, m, server_regex)) {
    return syncServer(events, m[1].str());
  }

  std::regex namespace_regex("/namespaces/([0-9A-Za-z_.-]+)/config");
  if (std::regex_match(vpath, m, namespace_regex)) {
    return syncNamespace(events, m[1].str());
  }

  std::regex tables_path_regex("/namespaces/([0-9A-Za-z_.-]+)/tables");
  if (std::regex_match(vpath, m, tables_path_regex)) {
    return syncTables(events, m[1].str());
  }

  std::regex table_path_regex("/namespaces/([0-9A-Za-z_.-]+)/tables/([0-9A-Za-z_.-]+)");
  if (std::regex_match(vpath, m, table_path_regex)) {
    return syncTable(events, m[1].str(), m[2].str());
  }

  logWarning("evqld", "received zookeper watch on unknown path: $0", vpath);
  return Status::success();
}

/**
 * NOTE: if you're reading this and thinking  "wow, paul doesn't know what a
 * switch statement is, what an idiot" then think again. zookeeper.h defines
 * all states as extern int's so we can't switch over them :(
 */

void ZookeeperConfigDirectory::handleSessionEvent(int state) {
  if (state == ZOO_EXPIRED_SESSION_STATE ||
      state == ZOO_EXPIRED_SESSION_STATE ||
      state == ZOO_AUTH_FAILED_STATE) {
    handleConnectionLost();
    return;
  }

  if (state == ZOO_CONNECTED_STATE) {
    handleConnectionEstablished();
    return;
  }

  if (state == ZOO_CONNECTING_STATE ||
      state == ZOO_ASSOCIATING_STATE) {
    return;
  }

  logCritical("evqld", "ERROR: invalid zookeeper state: $0", state);
  handleConnectionLost();
}

void ZookeeperConfigDirectory::handleConnectionEstablished() {
  switch (state_) {
    case ZKState::CONNECTING:
      logInfo("evqld", "Zookeeper connection established");
      state_ = ZKState::LOADING;
      return;

    case ZKState::CONNECTION_LOST:
      logInfo("evqld", "Zookeeper connection re-established");
      return;

    case ZKState::INIT:
    case ZKState::LOADING:
    case ZKState::CLOSED:
    case ZKState::CONNECTED:
      logCritical(
          "evqld",
          "ERROR: invalid zookeeper state in handleConnectionEstablished");
      return;
  }
}

void ZookeeperConfigDirectory::handleConnectionLost() {
  is_leader_ = false;

  switch (state_) {
    case ZKState::CONNECTING:
      logError("evqld", "Zookeeper connection failed");
      return;

    case ZKState::LOADING:
      logInfo("evqld", "Zookeeper connection lost while loading");
      state_ = ZKState::CONNECTING;
      return;

    case ZKState::CONNECTED:
    case ZKState::CONNECTION_LOST:
      logWarning("evqld", "Zookeeper connection lost");
      state_ = ZKState::CONNECTION_LOST;
      return;

    case ZKState::INIT:
    case ZKState::CLOSED:
      state_ = ZKState::CONNECTION_LOST;
      logCritical(
          "evqld",
          "ERROR: invalid zookeeper state in handleConnectionLost");
      return;
  }
}

// PRECONDITION: must NOT hold mutex
ClusterConfig ZookeeperConfigDirectory::getClusterConfig() const {
  std::unique_lock<std::mutex> lk(mutex_);
  return getPatchedClusterConfig();
}

// PRECONDITION: must NOT hold mutex
void ZookeeperConfigDirectory::updateClusterConfig(ClusterConfig config) {
  std::unique_lock<std::mutex> lk(mutex_);
  updateClusterConfigWithLock(config);
}

// PRECONDITION: must hold mutex
void ZookeeperConfigDirectory::updateClusterConfigWithLock(
    ClusterConfig config) {
  auto buf = msg::encode(config);

  if (config.version() == 0) {
    // create
    {
      auto rc = zoo_create(
          zk_,
          path_prefix_.c_str(),
          nullptr,
          0,
          &ZOO_OPEN_ACL_UNSAFE,
          0,
          nullptr, /* path_buffer */
          0 /* path_buffer_len */);

      if (rc && rc != ZNODEEXISTS) {
        RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
      }
    }

    {
      auto path = StringUtil::format("$0/config", path_prefix_);
      auto rc = zoo_create(
          zk_,
          path.c_str(),
          (const char*) buf->data(),
          buf->size(),
          &ZOO_OPEN_ACL_UNSAFE,
          0,
          NULL /* path_buffer */,
          0 /* path_buffer_len */);

      if (rc) {
        RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
      }
    }
  } else {
    // update
    auto path = StringUtil::format("$0/config", path_prefix_);
    auto rc = zoo_set(
        zk_,
        path.c_str(),
        (const char*) buf->data(),
        buf->size(),
        config.version() - 1);

    if (rc) {
      RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
    }
  }
}

// PRECONDITION: must NOT hold mutex
void ZookeeperConfigDirectory::setClusterConfigChangeCallback(
    Function<void (const ClusterConfig& cfg)> fn) {
  std::unique_lock<std::mutex> lk(mutex_);
  on_cluster_change_.emplace_back(fn);
}

// PRECONDITION: must hold mutex
ClusterConfig ZookeeperConfigDirectory::getPatchedClusterConfig() const {
  ClusterConfig patched = cluster_config_;

  HashMap<String, String> old_addrs;
  for (const auto& n : patched.dht_nodes()) {
    old_addrs[n.name()] = n.addr();
  }

  patched.clear_dht_nodes();
  for (const auto& s : servers_) {
    auto dhtnode = patched.add_dht_nodes();
    dhtnode->set_name(s.second.server_id());
    dhtnode->set_status(DHTNODE_LIVE);
    const auto& live_server = servers_live_.find(s.second.server_id());
    if (live_server != servers_live_.end()) {
      dhtnode->set_addr(live_server->second);
    } else {
      dhtnode->set_addr(old_addrs[s.second.server_id()]);
    }
  }

  return patched;
}

String ZookeeperConfigDirectory::getServerID() const {
  if (server_name_.isEmpty()) {
    RAISE(kRuntimeError, "no server id available");
  }

  return server_name_.get();
}

bool ZookeeperConfigDirectory::hasServerID() const {
  return !server_name_.isEmpty();
}

bool ZookeeperConfigDirectory::electLeader() {
  std::unique_lock<std::mutex> lk(mutex_);

  if (is_leader_) {
    return true;
  }

  String leader_name;
  if (!server_name_.isEmpty()) {
    leader_name = server_name_.get();
  }

  auto key = StringUtil::format("$0/leader", path_prefix_);
  auto rc = zoo_create(
      zk_,
      key.c_str(),
      leader_name.data(),
      leader_name.size(),
      &ZOO_OPEN_ACL_UNSAFE,
      ZOO_EPHEMERAL,
      nullptr, /* path_buffer */
      0 /* path_buffer_len */);

  if (!rc) {
    is_leader_ = true;
    return true;
  } else if (rc == ZNODEEXISTS) {
    return false;
  } else  {
    RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
  }
}

String ZookeeperConfigDirectory::getLeader() const {
  std::unique_lock<std::mutex> lk(mutex_);

  Buffer leader_name(1024);
  auto key = StringUtil::format("$0/leader", path_prefix_);
  if (getNode(key, &leader_name, true)) {
    return leader_name.toString();
  } else {
    return "";
  }
}

ServerConfig ZookeeperConfigDirectory::getServerConfig(
    const String& server_name) const {
  std::unique_lock<std::mutex> lk(mutex_);
  auto iter = servers_.find(server_name);
  if (iter == servers_.end()) {
    RAISEF(kNotFoundError, "server not found: $0", server_name);
  }

  auto server_config = iter->second;
  const auto& live_server = servers_live_.find(server_name);
  if (live_server != servers_live_.end()) {
    server_config.set_server_status(SERVER_UP);
    server_config.set_server_addr(live_server->second);
  } else {
    server_config.set_server_status(SERVER_DOWN);
  }

  return server_config;
}

Vector<ServerConfig> ZookeeperConfigDirectory::listServers() const {
  Vector<ServerConfig> servers;

  std::unique_lock<std::mutex> lk(mutex_);
  for (const auto& server : servers_) {
    servers.emplace_back(server.second);

    auto& server_config = servers.back();
    const auto& live_server = servers_live_.find(server_config.server_id());
    if (live_server != servers_live_.end()) {
      server_config.set_server_status(SERVER_UP);
      server_config.set_server_addr(live_server->second);
    } else {
      server_config.set_server_status(SERVER_DOWN);
    }
  }

  return servers;
}

void ZookeeperConfigDirectory::setServerConfigChangeCallback(
    Function<void (const ServerConfig& cfg)> fn) {
  std::unique_lock<std::mutex> lk(mutex_);
  on_server_change_.emplace_back(fn);
}

void ZookeeperConfigDirectory::updateServerConfig(ServerConfig cfg) {
  std::unique_lock<std::mutex> lk(mutex_);

  cfg.clear_server_addr();
  cfg.clear_server_status();

  auto buf = msg::encode(cfg);
  auto path = StringUtil::format(
      "$0/servers/$1",
      path_prefix_,
      cfg.server_id());

  if (cfg.version() == 0) {
    // create
    auto rc = zoo_create(
        zk_,
        path.c_str(),
        (const char*) buf->data(),
        buf->size(),
        &ZOO_OPEN_ACL_UNSAFE,
        0,
        NULL /* path_buffer */,
        0 /* path_buffer_len */);

    if (rc) {
      RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
    }
  } else {
    // update
    auto rc = zoo_set(
        zk_,
        path.c_str(),
        (const char*) buf->data(),
        buf->size(),
        cfg.version() - 1);

    if (rc) {
      RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
    }
  }
}

RefPtr<NamespaceConfigRef> ZookeeperConfigDirectory::getNamespaceConfig(
    const String& customer_key) const {
  std::unique_lock<std::mutex> lk(mutex_);
  auto iter = namespaces_.find(customer_key);
  if (iter == namespaces_.end()) {
    RAISEF(kNotFoundError, "namespace not found: $0", customer_key);
  }

  return mkRef(new NamespaceConfigRef(iter->second));
}

void ZookeeperConfigDirectory::listNamespaces(
    Function<void (const NamespaceConfig& cfg)> fn) const {
  std::unique_lock<std::mutex> lk(mutex_);
  for (const auto& ns : namespaces_) {
    fn(ns.second);
  }
}

void ZookeeperConfigDirectory::setNamespaceConfigChangeCallback(
    Function<void (const NamespaceConfig& cfg)> fn) {
  std::unique_lock<std::mutex> lk(mutex_);
  on_namespace_change_.emplace_back(fn);
}

void ZookeeperConfigDirectory::updateNamespaceConfig(NamespaceConfig cfg) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto buf = msg::encode(cfg);

  if (cfg.version() == 0) {
    // create
    // FIXME if we fail between the three creates, we end up with an incomplete namespace
    {
      auto path = StringUtil::format(
          "$0/namespaces/$1",
          path_prefix_,
          cfg.customer());

      auto rc = zoo_create(
          zk_,
          path.c_str(),
          nullptr, /* data */
          0, /* data len */
          &ZOO_OPEN_ACL_UNSAFE,
          0,
          NULL /* path_buffer */,
          0 /* path_buffer_len */);

      if (rc) {
        RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
      }
    }

    {
      auto path = StringUtil::format(
          "$0/namespaces/$1/config",
          path_prefix_,
          cfg.customer());

      auto rc = zoo_create(
          zk_,
          path.c_str(),
          (const char*) buf->data(),
          buf->size(),
          &ZOO_OPEN_ACL_UNSAFE,
          0,
          NULL /* path_buffer */,
          0 /* path_buffer_len */);

      if (rc) {
        RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
      }
    }

    {
      auto path = StringUtil::format(
          "$0/namespaces/$1/tables",
          path_prefix_,
          cfg.customer());

      auto rc = zoo_create(
          zk_,
          path.c_str(),
          nullptr,
          0,
          &ZOO_OPEN_ACL_UNSAFE,
          0,
          nullptr, /* path_buffer */
          0 /* path_buffer_len */);

      if (rc) {
        RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
      }
    }
  } else {
    // update
    auto path = StringUtil::format(
        "$0/namespaces/$1/config",
        path_prefix_,
        cfg.customer());

    auto rc = zoo_set(
        zk_,
        path.c_str(),
        (const char*) buf->data(),
        buf->size(),
        cfg.version() - 1);

    if (rc) {
      RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
    }
  }
}

TableDefinition ZookeeperConfigDirectory::getTableConfig(
    const String& db_namespace,
    const String& table_name) const {
  std::unique_lock<std::mutex> lk(mutex_);
  auto iter = tables_.find(db_namespace + "~" + table_name);
  if (iter == tables_.end()) {
    RAISEF(kNotFoundError, "table not found: $0", table_name);
  }

  return iter->second;
}

void ZookeeperConfigDirectory::updateTableConfig(
    const TableDefinition& table,
    bool force /* = false */) {
  std::unique_lock<std::mutex> lk(mutex_);

  auto buf = msg::encode(table);
  auto path = StringUtil::format(
      "$0/namespaces/$1/tables/$2",
      path_prefix_,
      table.customer(),
      table.table_name());

  if (table.version() == 0) {
    // create
    auto rc = zoo_create(
        zk_,
        path.c_str(),
        (const char*) buf->data(),
        buf->size(),
        &ZOO_OPEN_ACL_UNSAFE,
        0,
        NULL /* path_buffer */,
        0 /* path_buffer_len */);

    if (rc) {
      RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
    }
  } else {
    // update
    auto rc = zoo_set(
        zk_,
        path.c_str(),
        (const char*) buf->data(),
        buf->size(),
        table.version() - 1);

    if (rc) {
      RAISEF(kRuntimeError, "zoo_create() failed: $0", getErrorString(rc));
    }
  }
}

void ZookeeperConfigDirectory::listTables(
    Function<void (const TableDefinition& table)> fn) const {
  std::unique_lock<std::mutex> lk(mutex_);
  for (const auto& tbl : tables_) {
    fn(tbl.second);
  }
}

void ZookeeperConfigDirectory::setTableConfigChangeCallback(
    Function<void (const TableDefinition& tbl)> fn) {
  std::unique_lock<std::mutex> lk(mutex_);
  on_table_change_.emplace_back(fn);
}

const char* ZookeeperConfigDirectory::getErrorString(int err) const {
  switch (err) {
    case ZOK: return "Everything is OK";
    case ZSYSTEMERROR: return "System Error";
    case ZRUNTIMEINCONSISTENCY: return "A runtime inconsistency was found";
    case ZDATAINCONSISTENCY: return "A data inconsistency was found";
    case ZCONNECTIONLOSS: return "Connection to the server has been lost";
    case ZMARSHALLINGERROR: return "Error while marshalling or unmarshalling data";
    case ZUNIMPLEMENTED: return "Operation is unimplemented";
    case ZOPERATIONTIMEOUT: return "Operation timeout";
    case ZBADARGUMENTS: return "Invalid arguments";
    case ZINVALIDSTATE: return "Invliad zhandle state";
    case ZAPIERROR: return "API Error";
    case ZNONODE: return "Node does not exist";
    case ZNOAUTH: return "Not authenticated";
    case ZBADVERSION: return "Version conflict";
    case ZNOCHILDRENFOREPHEMERALS: return "Ephemeral nodes may not have children";
    case ZNODEEXISTS: return "The node already exists";
    case ZNOTEMPTY: return "The node has children";
    case ZSESSIONEXPIRED: return "The session has been expired by the server";
    case ZINVALIDCALLBACK: return "Invalid callback specified";
    case ZINVALIDACL: return "Invalid ACL specified";
    case ZAUTHFAILED: return "Client authentication failed";
    case ZCLOSING: return "ZooKeeper is closing";
    case ZNOTHING: return "(not error) no server responses to process";
    case ZSESSIONMOVED: return "session moved to another server, so operation is ignored";
    default: return "ZooKeeper Error";
  }
}

} // namespace eventql
