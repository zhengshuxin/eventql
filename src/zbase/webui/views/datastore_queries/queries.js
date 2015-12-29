ZBase.registerView((function() {
  var load = function(url) {
    var qparams = {
      with_categories: true,
      type: "all",
      author: "all",
    };

    var owner_param = UrlUtil.getParamValue(url, "owner");
    if (owner_param) {
      qparams.owner = owner_param;
    }

    var author_param = UrlUtil.getParamValue(url, "author");
    if (author_param) {
      qparams.author = author_param;
    }

    var category_param = UrlUtil.getParamValue(url, "category");
    if (category_param) {
      qparams.category_prefix = category_param;
    }

    var pstatus_param = UrlUtil.getParamValue(url, "publishing_status");
    if (pstatus_param) {
      qparams.publishing_status = pstatus_param;
    }

    var search_param = UrlUtil.getParamValue(url, "q");
    if (search_param) {
      qparams.search = search_param;
    }

    $.showLoader();

    searchDocuments(qparams, function(r) {
      if (r.status == 200) {
        render(JSON.parse(r.response), qparams, url);
      } else {
        renderError(r.statusText);
      }
    });
  };

  var searchDocuments = function(qparams, callback) {
    $.httpGet("/api/v1/documents?" + $.buildQueryString(qparams), callback);
  };

  var render = function(data, qparams, path) {
    var reports = data.documents;
    var categories = data.categories;

    var page = $.getTemplate(
        "views/datastore_queries",
        "zbase_datastore_queries_main_tpl");

    renderTable(
        page.querySelector(".zbase_datastore_queries tbody"),
        reports);

    $("z-dropdown.create_new_doc", page).addEventListener("change", function(e) {
      $.createNewDocument(this.getValue());
      this.setValue([]);
    });

    var search = $("z-search.search_queries", page);
    search.addEventListener("z-search-autocomplete", searchAutocomplete);
    search.addEventListener("z-search-submit", searchSubmit);

    if (qparams.search) {
      $("z-input", search).setAttribute("data-value", qparams.search);
      var query_bc = $("zbase-breadcrumbs .query_breadcrumb", page);
      query_bc.href = getSearchUrl(qparams.search);
      query_bc.innerHTML = "Search results for '" + qparams.search + "'";
    }

    $.handleLinks(page);
    $.replaceViewport(page)
    $.hideLoader();
  };

  var renderTable = function(tbody_elem, reports) {
    var doc_types = {
      report: true,
      sql_query: true
    };

    reports.forEach(function(doc) {
      if (!doc_types[doc.type]) {
        return;
      }
      var url = getPathPrefixForDocType(doc.type) + doc.uuid;

      var tr = document.createElement("tr");
      tr.innerHTML = 
          "<td><a href='" + url + "'>" + $.escapeHTML(doc.name) + "</a></td>" +
          "<td><a href='" + url + "'>" + $.escapeHTML(doc.type) + "</a></td>" +
          "<td><a href='" + url + "'>" + DateUtil.printTimeAgo(doc.mtime) + "</a></td>";

      tbody_elem.appendChild(tr);
    });

    $.handleLinks(tbody_elem);
  };

  var renderError = function(msg) {
    var error_elem = document.createElement("div");
    error_elem.classList.add("zbase_error");
    error_elem.innerHTML = 
        "<span>" +
        "<h2>We're sorry</h2>" +
        "<h1>An error occured.</h1><p>" + msg + "</p> " +
        "<p>Please try it again or contact support if the problem persists.</p>" +
        "<a href='/a/datastore/queries' class='z-button secondary'>" + 
        "<i class='fa fa-refresh'></i>&nbsp;Reload</a>" +
        "</span>";

    $.replaceViewport(error_elem);
    $.hideLoader();
  };

  var getPathPrefixForDocType = function(doctype) {
    switch (doctype) {
      case "report":
        return "/a/reports/";
      case "sql_query":
        return "/a/sql/";
    }
  };

  var searchAutocomplete = function(e) {
    var term = e.detail.value;
    var search_widget = this;

    searchDocuments({search: term}, function(r) {
      if (r.status == 200) {
        var documents = JSON.parse(r.response).documents;
        var items = [];

        for (var i = 0; i < documents.length; i++) {
          if (i > 10) {
            break;
          }

          items.push({
            query: documents[i].name,
            data_value: documents[i].name});
        }

        search_widget.autocomplete(term, items);
      }
    });
  };

  var searchSubmit = function(e) {
    var term = e.detail.value;
    var search_widget = this;

    searchDocuments({search: term}, function(r) {
      if (r.status == 200) {
        var documents = JSON.parse(r.response).documents;
        if (documents.length == 1) {
          var path;
          switch (documents[0].type) {
            case "sql_query":
              path = "/a/sql/" + documents[0].uuid;
              break;

            case "report":
              path = "/a/reports/" + documents[0].uuid;
              break;
          }

          var input = $("input", search_widget);
          input.value = "";
          input.blur();

          $.navigateTo(path);
          return;
        } else {
          $.navigateTo(getSearchUrl(term));
        }
      }
    });
  };

  var getSearchUrl = function(search_term) {
    var path = "/a/datastore/queries"
    if (search_term.length > 0) {
      path += "?q=" + search_term;
    }

    return path;
  };

  return {
    name: "datastore_queries",
    loadView: function(params) { load(params.path); },
    unloadView: function() {},
    handleNavigationChange: load
  };

})());