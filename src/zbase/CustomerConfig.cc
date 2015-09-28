/**
 * Copyright (c) 2015 - The CM Authors <legal@clickmatcher.com>
 *   All Rights Reserved.
 *
 * This file is CONFIDENTIAL -- Distribution or duplication of this material or
 * the information contained herein is strictly forbidden unless prior written
 * permission is obtained.
 */
#include "stx/protobuf/MessageSchema.h"
#include "zbase/CustomerConfig.h"
#include "zbase/JoinedSession.pb.h"

using namespace stx;

namespace zbase {

CustomerConfig createCustomerConfig(const String& customer) {
  CustomerConfig conf;

  conf.set_customer(customer);

  auto hook = conf.mutable_logjoin_config()->add_webhooks();
  hook->set_id("23");
  hook->set_target_url("http://localhost:8080/mywebhook");

  {
    auto ev = conf.mutable_logjoin_config()->add_session_event_schemas();
    ev->set_evtype("search_query");
    ev->set_evid(16);
    auto schema = msg::MessageSchema::fromProtobuf(zbase::JoinedSearchQuery::descriptor());
    schema->setName(ev->evtype());
    ev->set_schema(schema->encode().toString());
  }

  {
    auto ev = conf.mutable_logjoin_config()->add_session_event_schemas();
    ev->set_evtype("page_view");
    ev->set_evid(25);
    auto schema = msg::MessageSchema::fromProtobuf(zbase::JoinedPageView::descriptor());
    schema->setName(ev->evtype());
    ev->set_schema(schema->encode().toString());
  }

  {
    auto ev = conf.mutable_logjoin_config()->add_session_event_schemas();
    ev->set_evtype("cart_items");
    ev->set_evid(52);
    auto schema = msg::MessageSchema::fromProtobuf(zbase::JoinedCartItem::descriptor());
    schema->setName(ev->evtype());
    ev->set_schema(schema->encode().toString());
  }

  conf.mutable_logjoin_config()->set_session_attributes_schema(
      msg::MessageSchema::fromProtobuf(
          zbase::DefaultSessionAttributes::descriptor())->encode().toString());

  conf.mutable_logjoin_config()->set_session_schema_next_field_id(74);

  {
    auto liconf = conf.mutable_logfile_import_config();
    auto lidef = liconf->add_logfiles();
    lidef->set_name("access_log");
    lidef->set_regex(R"(^(?<http_status>[^|]*)\|(?<loveos_region>[^|]*)\|(?<loveos_cluster>[^|]*)\|(?<forwarded_for>[^|]*)\|(?<dawanda_session>[^|]*)\|(?<bytes_sent>[^|]*)\|(?<request_time>[^|]*)\|(?<upstream_response_time>[^|]*)\|(?<http_method>[^| ]*) ?(?<path>[^ ]*) ?(?<http_version>HTTP\/[102\.]+)\|(?<rails_runtime_ms>[^|]*)\|(?<remote_addr>[^|]*)\|(?<time>[^|]*)\|(?<http_host>[^|]*)\|(?<referrer>[^|]*)\|(?<user_agent>[^|]*)\|(?<upstream_addr>[^|\n]*))");

    {
      auto f = lidef->add_source_fields();
      f->set_name("datacenter");
      f->set_id(2);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_source_fields();
      f->set_name("rack");
      f->set_id(3);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_source_fields();
      f->set_name("host");
      f->set_id(4);
      f->set_type("STRING");
    }

    {
      auto f = lidef->add_row_fields();
      f->set_name("http_status");
      f->set_id(5);
      f->set_type("UINT64");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("loveos_region");
      f->set_id(7);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("loveos_cluster");
      f->set_id(8);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("forwarded_for");
      f->set_id(9);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("dawanda_session");
      f->set_id(10);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("bytes_sent");
      f->set_id(11);
      f->set_type("UINT64");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("request_time");
      f->set_id(12);
      f->set_type("DOUBLE");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("upstream_response_time");
      f->set_id(13);
      f->set_type("DOUBLE");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("http_method");
      f->set_id(14);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("path");
      f->set_id(15);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("http_version");
      f->set_id(16);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("rails_runtime_ms");
      f->set_id(17);
      f->set_type("DOUBLE");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("remote_addr");
      f->set_id(18);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("time");
      f->set_id(19);
      f->set_type("DATETIME");
      f->set_format("%d/%b/%Y:%H:%M:%S");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("http_host");
      f->set_id(20);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("referrer");
      f->set_id(21);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("user_agent");
      f->set_id(22);
      f->set_type("STRING");
    }
    {
      auto f = lidef->add_row_fields();
      f->set_name("upstream_addr");
      f->set_id(23);
      f->set_type("STRING");
    }
  }

  return conf;
}

void eventDefinitonRemoveField(EventDefinition* def, const String& field) {
  auto schema = msg::MessageSchema::decode(def->schema());

  auto cur_schema = schema;
  auto cur_field = field;
  while (StringUtil::includes(cur_field, ".")) {
    auto prefix_len = cur_field.find('.');
    auto prefix = field.substr(0, prefix_len);

    cur_field = cur_field.substr(prefix_len + 1);
    cur_schema = cur_schema->fieldSchema(cur_schema->fieldId(prefix));
  }

  auto edit_id = cur_schema->fieldId(cur_field);
  cur_schema->removeField(edit_id);

  def->set_schema(schema->encode().toString());
}

} // namespace zbase

