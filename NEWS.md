couchbeam NEWS
--------------

unreleased (bounded-otp27 line)
-------------------------------

Names (the database, a design or view name, a `list' function name)

- each must be one request-path segment, or the door refuses it before any
  budget: `{error, {unsafe_db_name, Name}}',
  `{error, {invalid_view_name, ViewName}}', `{error, {invalid_param, Entry}}'.
  Refused: CR/LF, `?', `#', `/', a space, a tab, any other C0 control, DEL,
  an empty name, and an empty, `.' or `..' segment of the percent-decoded
  name (so `%2E%2E', `..%2Fx' and `%2F' too — CouchDB reads `%2F' in a name
  as `/', which is why `account%2Fab' is one name and two segments)
- document ids keep the wider contract — `?' and `#' in one are urlencoded
  and address the document — but are refused for CR/LF and for an empty or
  dot segment of the decoded id (an id ending in `_design/' among them),
  with `{error, missing_doc_id}'; query halves carrying CR/LF are refused
  with `{error, {invalid_param, Entry}}'

The request line at the transport door (`request_bounded/6,7',
`db_request_bounded/7')

- the method must be letters and not `connect' (`{error, {unsafe_method,
  Method}}') — door policy, narrower than the HTTP token grammar, because
  CouchDB needs no other and `connect' is hackney's tunnel flow
- the URL is refused with `{error, {unsafe_url, Path}}' when it carries
  CR/LF, when its raw path carries `#', a space, a C0 control or DEL, when
  its raw query carries a byte above 127 (the query is written unencoded),
  when it has a query and no path (`http://host?x=y', and
  `http://u:5984?x@host/db', whose authority a `?' cuts short), or when its
  netloc — the `Host' header, percent-decoded and IDN-converted by
  `hackney_url:normalize/2' for a DNS name — carries an invalid escape, a
  space, a control or a byte above 127. `http://host:5984' with no path is
  legal and passes. A URL `hackney_url:parse_url/1' cannot parse at all
  answers `{error, {unsafe_url, undefined}}'
- credentials in the netloc must be percent-encoded. Raw `?' or `#' in a
  password cuts the URL there; a raw `@' or a `/' after a numeric prefix
  re-parses the authority into a host the caller never named, and no door
  can tell — `http://u:12/x@host:5984/db' is host `u', port 12 to hackney
  and to this door alike. Where the URL becomes unparseable, the server
  record is never made: `server_connection/2', and `server_connection/4'
  through it, parse the URL. Where it stays parseable but wrong — a numeric
  port half, `http://u:12?34@host:5984' — the record is made and every
  bounded door then answers `{error, {unsafe_url, <<>>}}' while the legacy
  doors keep sending the request
- `request_line_safe/1' (shape and CR/LF) and `addressable/1' (one path
  segment) are exported for direct transport callers, which must judge
  every segment they place into a URL themselves — by the time the door
  sees it a `?' is the query, so `POST /db?x/_find' reaches `/db'.
  `segments_usable/1' is exported for `couchbeam' itself

Headers, and the options and body hackney turns into headers

- a header name must be a nonempty HTTP token; a value must carry no C0
  control other than HT and no DEL, while Latin-1 bytes above 127 stay
  accepted. A header list with a malformed tail answers
  `{error, {unsafe_header, undefined}}' before header preparation
- the four names hackney reads back before writing take narrower values:
  none of them may carry the parameterised `{Value, Params}' form
  (`hackney_bstr:to_binary/1' has no tuple clause); `content-type' must be
  a binary (`parse_content_type/1' matches binaries only under a streamed
  multipart body); `content-length' must be a nonnegative integer or a
  binary of digits, since hackney keeps a caller's value as the framing of
  a streamed or function body; `expect' and `transfer-encoding' take any
  scalar. Conflicting Content-Length values and Transfer-Encoding together
  with Content-Length are refused; numerically equal lengths remain legal.
  Transfer-Encoding, when present, must be a single `chunked' value: no
  other coding is implemented by hackney. Omitted (`undefined') header
  values are removed from the actual outgoing list after all header
  producers run. Chunked bodies use separate header/body writes so the
  request line is never chunk-framed. Every other name accepts every half
- the `cookie' option is a binary, `{Name, Value}', `{Name, Value, Opts}'
  or a list of those — the `{cookie, string()}' form the legacy
  `server_connection/4' doc named is refused, hackney would write a string
  as one integer cookie per character. Cookie options, when present, must
  be `secure'/`http_only' = true, a nonnegative integer `max_age', and a
  binary/iodata `domain'/`path' carrying no control, `;' or `,'
- the `basic_auth' option must be `{User, Password}' with halves
  `hackney_bstr:to_binary/1' renders and no CR/LF, and `insecure_basic_auth'
  must resolve to a boolean (request option, then hackney app default),
  including when credentials come from URL userinfo; otherwise
  `{error, {unsafe_header, <<"Authorization">>}}'
- a streamed multipart body must carry a boundary that is both an RFC 2046
  boundary and an HTTP token (1 to 70 characters of DIGIT / ALPHA /
  `'+_-.', since hackney writes `boundary=' unquoted) and a size that is
  `chunked' or a nonnegative integer; otherwise
  `{error, {unsafe_header, Name}}' names `Content-Type' or `Content-Length'
- `follow_redirect', `proxy' and `path_encode_fun' options are refused with
  `{error, {unsupported_option, Option}}', the bare atom form included —
  hackney reads its options with `proplists', where a bare atom is the
  option set to `true'. Environment proxies are the caller's to disable
  (`no_proxy_env'): under one, hackney writes the whole URL as the request
  target through `hackney_url:unparse_url/1', which urlencodes the
  credentials and drops the already-refused fragment, so the request line
  stays whole but the netloc distinction above no longer holds

Shape of the doors

- all bounded doors require a `#db{}' first argument (function_clause on
  another term); `save_doc_bounded/4' no longer returns `invalid_document'
  for a non-db argument. Input refusals precede budget creation; the
  transport refusals above happen after it, and on the view door inside its
  budgeted stream

version 1.7.1 / 2025-07-24
---------------------------

- update hackney depdendency to 1.25.0

version 1.7.0 / 2025-05-28
--------------------------

- fix resource leaks and race conditions in stream modules
- fix unclosed hackney connections on error paths
- add proper cleanup for monitor references using try-finally
- fix ETS table race conditions by checking process liveness
- improve stream initialization order to prevent races
- add error handling for hackney operations to prevent leaks
- fix changes stream registration race condition by registering before parent notification

version 1.6.0 / 2025-01-26
--------------------------

- add support for CouchDB _find endpoint
- add ability to query _show functions  
- add option for disabling view_stream usage (enabled by default)
- fix error handling in gen_changes callback handling
- fix resource cleanup in stream modules to prevent connection leaks
- improve replication test reliability and timeout handling
- update hackney dependency to 1.23
- update jsx dependency
- add GitHub Actions for CouchDB testing
- fix dialyzer complaints and pattern matching issues
- OTP 27 compatibility improvements

version 1.5.4 / 2025-02-20
--------------------------

- bump hackney 1.22.0

version 1.5.3 / 2024-08-20
--------------------------

- fix packaging

version 1.5.1 / 2024-11-07
--------------------------

- fix pattern matching

version 1.5.1 / 2024-11-07
--------------------------

- fix: handle condition that may cause persistent CLOSE-WAIT sockets

version 1.5.0 / 2023-10-11
--------------------------

- use hackney 1.20.0
- fix compatibility with couchdb 3
- fix compatibility with Erlang >= 23

version 1.4.1 / 2016-09-26
--------------------------

- maintainance update

version 1.4.0 / 2016-09-22
--------------------------

- maintainance update.

version 1.3.1 / 2016-07-01
--------------------------

- fix: accept 202 status in `couchbeam:save_doc/4` function (#144)
- fix: spec syntax to build with Erlang 19 (#145)

version 1.3.0 / 2016-03-22
--------------------------

- add `couchbeam:all_dbs/2`
- add `couchbeam:view_cleanup/1`
- add `couchbeam:design_info/2`
- add `post_decode` function to view stream
- add Elixir mix support
- fix: handle http errors in view stream (#140)
- fix: build with latest rebar3

version 1.2.1 / 2015/11/04
--------------------------

- also support hackney 1.4.4 for rebar2.
- fix hex.pm release to really use 1.4.4

version 1.2.0 / 2015/11/04
--------------------------

- move to eunit for tests.
- hex.pm support
- mix & rebar3 build tools support
- bump hackney to 1.4.4
- bump jsx to 2.2.8

### Breaking change

erlang-oauth is now optionnal and won't be installed by default.

version 1.1.8 / 2015-08-27
--------------------------

- use latest stable branch of [hackney](https://github.com/benoitc/hackney)

version 1.1.7 / 2015-03-11
--------------------------

- bump [hackney](https://github.com/benoitc/hackney) to 1.1.0
- fix `Conten-Type` header ehen posting doc IDS in changes #126
- fix documentation

version 1.1.6 / 2015-01-02
--------------------------

- fix `included_applications` (#122)

version 1.1.5 / 2014-12-09
--------------------------

- improvement: do not force connections options to nodelay
- update to [Hackney](https://github.com/benoitc/hackney) 1.0.4 fix #120
- fix: retry fecthing UUIDS on error (#121)

version 1.1.4 / 2014-12-01
--------------------------

- update to [Hackney](https://github.com/benoitc/hackney) 1.0.1: more SSL
  certificate authority handling.
- fix: changes stream

version 1.1.3 / 2014-11-30
--------------------------

- update to [Hackney](https://github.com/benoitc/hackney) 1.0.0

version 1.1.2 / 2014-11-15
--------------------------

- remove spurious prints

version 1.1.1 / 2014-11-11
--------------------------

- update to [hackney 0.15.0](https://github.com/benoitc/hackney/releases ),
  improving performances and concurrency
- fix `couchbeam:doc_exists/2`(#116)
- fix `couchbeam:reply_att/1 (#114)


version 1.1.0 / 2014-10-28
--------------------------

- update to [hackney 0.14.3](https://github.com/benoitc/hackney/releases)
- fix memory leaks
- correctly close sockets
- fix streaming issue: don't wait the stream timeout to report the initial
  error.
- update JSX dependency to version 2.1.1

version 1.0.7 / 2014-07-08
--------------------------

- bump to [hackney 0.13.0](https://github.com/benoitc/hackney/releases/tag/0.13.0)

version 1.0.6 / 2014-04-18
--------------------------

- bump to [hackney 0.12.1](https://github.com/benoitc/hackney/releases/tag/0.12.2)


version 1.0.5 / 2014-04-18
--------------------------

- improve connections with HTTP proxies
- improve content-types detection of attachments
- improve URL encoding normalzation, useful when connecting to an
  international domain/URI
- URL resolving is faster
- bump to [hackney 0.12.0](https://github.com/benoitc/hackney/releases/tag/0.12.0)

version 1.0.4 / 2014-04-15
--------------------------

- remove spurious print

version 1.0.3 / 2014-04-15
--------------------------

- add support for the `new_edits` option in bulk doc API.
- improvement: send a doc as multipart that already contains attachments
- bump [hackney](http://github.com/benoitc/hackney) to 0.11.2
- fix path encoding

version 1.0.2 / 2014-01-03
--------------------------

- fix: send a doc as multipart that already contains attachments


version 1.0.1 / 2013-12-30
--------------------------

- fix connection reusing in changes and view streams
- bump hackney version to 0.10.1


version 1.0.0 / 2013-12-21
--------------------------

**First stable release**. This is a supported release.

- send a doc and its attachments efficiently using the [multipart
  API](http://docs.couchdb.org/en/latest/api/document/common.html#creating-multiple-attachments).
- add `couchbeam:get_config/{1,2,3}`, `couchbeam:set_config/{4,5}` and
  `couchbeam:delete_config/{3,4}` to use the [config
API](http://docs.couchdb.org/en/latest/api/server/configuration.html).
- add `couchbeam_uuids:random/0` and `couchbeam_uuids:utc_random/0` to
  generate UUIDS in your app instead of reusing the UUID generated on
the node. By default couchbeam is fetching from the node, which is
- add `{error, forbidden}` and `{error, unauthenticated}` as possible
  results of a reply.
better if you want to use UUID based on the node time.
- fix accept header handling


version 0.10.0 / 2013-12-21
---------------------------

- add `couchbeam:copy_doc/{2,3}` to support the COPY API
- add `couchbeam:get_missing_revs/2` to get the list of missing
  revisions
- add support of the [multipart
  API](http://docs.couchdb.org/en/latest/api/document/common.html#efficient-multiple-attachments-retrieving) when fetching a doc: This change make
  `couchbeam:open_doc/3` return a multipart response `{ok, {multipart,
Stream}}` when using the setting `attachments=true` option. A new option
{`accept. <<"multipart/mixed">>}" can also be used with the options
`open_revs` or `revs` to fetch the response as a multipart.
- bump the [hackney](http://github.com/benoitc/hackney) version to
  **0.9.1** .


With this change you can now efficiently retrieve a doc with all of its
attachments or a doc wit all its revisions.
 master

version 0.9.3 / 2013-12-07
--------------------------

- fix: `couchbeam:open_or_create_db/2'

version 0.9.2 / 2013-12-07
--------------------------

- bump hackney version to 0.8.3

version 0.9.1 / 2013-12-05
--------------------------

- fix design docid encoding

version 0.9.0 / 2013-12-05
--------------------------

This is a major release pre-1.0. API is now frozen and won't change much
until the version 1.0.

- replaced the use of `ibrowse` by `hackney` to handle HTTP connections
- new [streaming
  API](https://github.com/benoitc/couchbeam#stream-view-results) in view
- breaking change: remobe
- breaking change: remove deprecated view API. Everything is now managed in the
  [couch_view](https://github.com/benoitc/couchbeam/blob/master/doc/couchbeam_view.md) module.
- replace `couchbeam_changes:stream` and `couchbeam_changes:fetch`
  functions by `couchbeam_changes/follow` and `couchbeam_changes:follow_once`.
- breaking change: new attachment API
- new: JSX a pure erlang JSON encoder/decoder is now the default. Jiffy
  can be set at the compilation by defining `WITH_JIFFY` in the Erlang
options.
- removed mochiweb dependency.


version 0.7.0 / 2011-07-05
--------------------------

This release contains backwards incompatible changes.

- New and more efficient couchbeam_changes API, we now parse json stream
  instead of the try catch steps we used before.
- New and more efficient couchbeam_view API. we now parse json stream
  instead of getting all results. New couchbeam_view:stream and
couchbeam_view fetch functions have been added. We also don't use any
more a view record in other functions
- HTTP functions have been moved to couchbeam_httpc modules
- gen_changes behaviour has been updated to use the couchbeam_changes
  API. It's also abble to restart a lost connection for longpoll and
continuous feeds.

Breaking Changes:

- couchbeam:view and couchbeam:all_docs have been deprecated. Old views
  functions using the #view{} record from these functions have been
moved in couchbeam_oldview module.
- couchbeam:wait_changes, couchbeam:wait_changes_once, couchbeam:changes
  functions have been deprecated and are now replaced by
couchbeam_changes:stream and couchbeam_changes:fetch functions.
