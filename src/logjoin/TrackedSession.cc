/**
 * Copyright (c) 2015 - The CM Authors <legal@clickmatcher.com>
 *   All Rights Reserved.
 *
 * This file is CONFIDENTIAL -- Distribution or duplication of this material or
 * the information contained herein is strictly forbidden unless prior written
 * permission is obtained.
 */
#include <fnord-base/inspect.h>
#include <fnord-base/Currency.h>
#include "logjoin/TrackedSession.h"
#include "common.h"

using namespace fnord;

namespace cm {

TrackedSession::TrackedSession() :
  num_cart_items(0),
  num_order_items(0),
  gmv_eurcents(0),
  cart_value_eurcents(0) {}

void TrackedSession::update() {
  /* update queries (mark items as clicked) */
  for (auto& cur_query : queries) {

    /* search for matching item visits */
    for (auto& cur_visit : item_visits) {
      auto cutoff = cur_query.time.unixMicros() +
          kMaxQueryClickDelaySeconds * kMicrosPerSecond;

      if (cur_visit.time < cur_query.time ||
          cur_visit.time.unixMicros() > cutoff) {
        continue;
      }

      for (auto& qitem : cur_query.items) {
        if (cur_visit.item == qitem.item) {
          qitem.clicked = true;
          break;
        }
      }
    }
  }

  /* calculate global gmv */
  HashMap<String, uint64_t> cart_eurcents_per_item;
  HashMap<String, uint64_t> gmv_eurcents_per_item;
  for (const auto& ci : cart_items) {
    auto currency = currencyFromString(ci.currency);
    auto eur = Money(100, Currency::EUR); // FIXPAUL!!
    //auto eur = cconv_.convert(Money(ci.price_cents, currency), Currency::EUR);
    auto eurcents = eur.cents;
    eurcents *= ci.quantity;
    cart_eurcents_per_item.emplace(ci.item.docID().docid, eurcents);

    ++num_cart_items;
    cart_value_eurcents += eurcents;
    if (ci.checkout_step == 1) {
      gmv_eurcents_per_item.emplace(ci.item.docID().docid, eurcents);
      ++num_order_items;
      gmv_eurcents += eurcents;
    }
  }

  /* calculate gmv and ctrs per query */
  for (auto& q : queries) {
    auto slrid = extractAttr(q.attrs, "slrid");

    for (auto& i : q.items) {
      // DAWANDA HACK
      if (i.position >= 1 && i.position <= 4 && slrid.isEmpty()) {
        ++q.nads;
        q.nadclicks += i.clicked;
      }
      // EOF DAWANDA HACK

      ++q.nitems;

      if (i.clicked) {
        ++q.nclicks;

        {
          auto ci = cart_eurcents_per_item.find(i.item.docID().docid);
          if (ci != cart_eurcents_per_item.end()) {
            ++q.num_cart_items;
            q.cart_value_eurcents += ci->second;
          }
        }

        {
          auto ci = gmv_eurcents_per_item.find(i.item.docID().docid);
          if (ci != gmv_eurcents_per_item.end()) {
            ++q.num_order_items;
            q.gmv_eurcents += ci->second;
          }
        }
      }
    }
  }
}

void TrackedSession::insertLogline(
    const DateTime& time,
    const String& evtype,
    const String& evid,
    const URI::ParamList& logline) {
  if (evtype.length() != 1) {
    RAISE(kParseError, "e param invalid");
  }

  switch (evtype[0]) {

    /* query event */
    case 'q': {
      TrackedQuery query;
      query.time = time;
      query.eid = evid;
      query.fromParams(logline);
      insertQuery(query);
      break;
    }

    /* item visit event */
    case 'v': {
      TrackedItemVisit visit;
      visit.time = time;
      visit.eid = evid;
      visit.fromParams(logline);
      insertItemVisit(visit);
      break;
    }

    /* cart event */
    case 'c': {
      auto cart_items = TrackedCartItem::fromParams(logline);
      for (auto& ci : cart_items) {
        ci.time = time;
      }

      insertCartVisit(cart_items);
      break;
    }

    case 'u':
      return;

    default:
      RAISE(kParseError, "invalid e param");

  }
}

void TrackedSession::insertQuery(const TrackedQuery& query) {
  for (auto& q : queries) {
    if (q.eid == query.eid) {
      q.merge(query);
      return;
    }
  }

  queries.emplace_back(query);
}

void TrackedSession::insertItemVisit(const TrackedItemVisit& visit) {
  for (auto& v : item_visits) {
    if (v.eid == visit.eid) {
      v.merge(visit);
      return;
    }
  }

  item_visits.emplace_back(visit);
}

void TrackedSession::insertCartVisit(
    const Vector<TrackedCartItem>& new_cart_items) {
  for (const auto& cart_item : new_cart_items) {
    bool merged = false;

    for (auto& c : cart_items) {
      if (c.item == cart_item.item) {
        c.merge(cart_item);
        merged = true;
        break;
      }
    }

    if (!merged) {
      cart_items.emplace_back(cart_item);
    }
  }
}

void TrackedSession::debugPrint(const std::string& uid) const {
  fnord::iputs(" > queries: ", 1);
  for (const auto& query : queries) {
    fnord::iputs(
        "    > query time=$0 eid=$2\n        > attrs: $1",
        query.time,
        query.attrs,
        query.eid);

    for (const auto& item : query.items) {
      fnord::iputs(
          "        > qitem: id=$0 clicked=$1 position=$2 variant=$3",
          item.item,
          item.clicked,
          item.position,
          item.variant);
    }
  }

  fnord::iputs(" > item visits: ", 1);
  for (const auto& view : item_visits) {
    fnord::iputs(
        "    > visit: item=$0 time=$1 eid=$3 attrs=$2",
        view.item,
        view.time,
        view.attrs,
        view.eid);
  }

  fnord::iputs("", 1);
}


/*
JoinedSession TrackedSession::toJoinedSession() const {
  JoinedSession sess;
  sess.customer_key = customer_key;

  for (const auto& p : queries) {
    sess.queries.emplace_back(p.second);
  }

  for (const auto& p : item_visits) {
    sess.item_visits.emplace_back(p.second);
  }

  return sess;
}
*/

} // namespace cm
