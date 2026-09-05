%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_httpc).

-export([request/5, request_bounded/6, request_bounded/7,
         db_request/5, db_request/6, db_request_bounded/7,
         guard_ephemeral_worker/2,
         json_body/1,
         bounded_json_body/2,
         bounded_encode_json/2, bounded_encode_json/3,
         new_request_budget/1,
         invalid_query_param/1,
         request_line_safe/1,
         addressable/1,
         segments_usable/1,
         cancel_request/1,
         request_identity/2,
         db_resp/2,
         make_headers/4,
         maybe_oauth_header/4]).
-export_type([request_budget_spec/0, request_budget/0, request_identity/0]).

-ifdef(TEST).
-export([decode_bounded_json/3, request_options/2,
         unsafe_header/1, unsafe_cookie/1, unsafe_body_header/1,
         unsafe_basic_auth/1, usable_method/1]).
-endif.
%% urls utils
-export([server_url/1, db_url/1, doc_url/2]).
%% atts utols
-export([reply_att/1, wait_mp_doc/2, len_doc_to_mp_stream/3, send_mp_doc/5]).

-include("couchbeam.hrl").
-include_lib("hackney/include/hackney_lib.hrl").

%% `receive ... after' and `gen_server:call/3' accept at most 16#FFFFFFFF ms.
%% The longest derived wait is the view collector's delivery allowance:
%% `cleanup_deadline/1' (T) plus `couchbeam_view:bounded_cleanup_delivery_budget/1'
%% (2 * T), fed into `receive … after' as up to 3 * TimeoutMs. The
%% caller-facing ceiling is therefore a third of the timer range.
-define(MAX_BUDGET_TIMEOUT_MS, (16#FFFFFFFF div 3)).

%% `{TimeoutMs, MaxResponseBytes}': the first element is the absolute request
%% deadline in milliseconds counted from `new_request_budget/1', the second one
%% caps the cumulative raw response bytes.
-type request_budget_spec() :: {pos_integer(), pos_integer()}.
-type request_budget() :: #{'deadline_ms' := integer(),
                            'timeout_ms' := pos_integer(),
                            'max_response_bytes' := pos_integer()}.

%% @doc Perform an unbounded request. A `request_budget' entry in `Options'
%% is caller data, not an instruction: the deadline is a parameter of
%% `prepare_request/5', and only `request_bounded/6,7' passes one.
request(Method, Url, Headers, Body, Options) ->
    %% Without a budget the preparation cannot refuse: `request_options/2'
    %% passes the options through untouched.
    {'ok', FinalHeaders, FinalOpts} =
        prepare_request(Method, Url, Headers, Options, 'undefined'),
    request_prepared(Method, Url, FinalHeaders, Body, FinalOpts).

-spec prepare_request(term(), term(), list(), list(),
                      'undefined' | request_budget()) ->
          {'ok', list(), list()} |
          {'error', 'timeout' | 'invalid_request_budget'}.
prepare_request(Method, Url, Headers, Options, Budget) ->
    {FinalHeaders, FinalOpts0} = make_headers(Method, Url, Headers, Options),
    case request_options(FinalOpts0, Budget) of
        {'ok', FinalOpts} -> {'ok', FinalHeaders, FinalOpts};
        {'error', _}=Error -> Error
    end.

-spec request_prepared(term(), term(), list(), term(), list()) -> term().
request_prepared(Method, Url, Headers, Body, Options) ->
    hackney:request(Method, Url, Headers, Body, Options).

db_request(Method, Url, Headers, Body, Options) ->
    db_request(Method, Url, Headers, Body, Options, []).

%% @doc Legacy database request. Never bounded: the budget is a parameter of
%% `db_request_bounded/7', so a `request_budget' entry travelling in a
%% `#db.options' list (`open_db/3' merges the server options into it) cannot
%% turn this call into a bounded one whose streamed reply legacy callers
%% cannot consume.
db_request(Method, Url, Headers, Body, Options, Expect) ->
    db_resp(request(Method, Url, Headers, Body, Options), Expect).

%% @doc Bounded database request. `Budget' is the only source of the deadline.
%% On `{'ok', Status, Headers, Ref}' the caller owns the reply and must end
%% it in the same process: either read it with `bounded_json_body/2' or drop
%% it with `cancel_request/1'. A `Ref' that is simply abandoned keeps the
%% guardian, the transport lease and this call's process-dictionary entry
%% alive until the calling process dies -- the budget bounds the reply, not
%% the caller's silence. The doors of `couchbeam' do this for their callers.
%% The path segments of `Url' are the caller's: the door judges the request
%% line as hackney writes it (`request_line_safe/1', `unsafe_request_line/2'),
%% where a `?' already is the query — so every segment a caller places into
%% the URL, the database name first, must pass `addressable/1' before the
%% URL is built; `POST /db?x/_find' reaches `/db' otherwise.
-spec db_request_bounded(term(), term(), list(), term(), list(), [integer()],
                         request_budget()) ->
          {'ok', integer(), list(), reference()} | {'error', term()}.
db_request_bounded(Method, Url, Headers, Body, Options, Expect,
                   #{'deadline_ms' := _, 'timeout_ms' := _,
                     'max_response_bytes' := _}=Budget) ->
    %% The budget shape is checked first; the request line and the headers
    %% are refused inside `request_bounded/7' (`unsafe_method',
    %% `unsafe_url', `unsafe_header'), see `request_line_safe/1'.
    Resp = bounded_request(Method, Url, Headers, Body, Options, Budget),
    db_resp_bounded(Resp, Expect, Budget);
db_request_bounded(_Method, _Url, _Headers, _Body, _Options, _Expect,
                   _Budget) ->
    {'error', 'invalid_request_budget'}.


json_body(Ref) ->
    case hackney:body(Ref) of
        {ok, Body} ->
            couchbeam_ejson:decode(Body);
        {error, _} = Error ->
            Error
    end.

make_header_accept(Headers) ->
    case couchbeam_util:get_value(<<"Accept">>, Headers) of
        undefined -> [{<<"Accept">>, <<"application/json, */*;q=0.9">>} | Headers];
        _ -> Headers
    end.

make_headers(Headers, Options) ->
    Funs = [fun make_header_accept/1
           ,fun make_kazoo_headers/1
           ],
    NewHeaders = lists:foldl(fun(Fun, Acc) -> Fun(Acc) end, Headers, Funs),
    {filter_undefined_headers(NewHeaders), Options}.

filter_undefined_headers(Headers) ->
    lists:filter(fun({_, undefined}) -> false;
                    (_) -> true
                 end, Headers).

%% @doc Build a typed request budget from `{TimeoutMs, MaxResponseBytes}'.
%% The deadline is absolute and starts now: transport, status, headers, body,
%% JSON decode and the cleanup proof of a bounded call all count against it.
%% The cleanup proofs run on fresh budgets of `TimeoutMs' each, so the
%% wall-clock ceiling of one call is larger than the deadline: a direct
%% bounded call (`couchbeam:db_info_bounded/2', `open_doc_bounded/4',
%% `save_doc_bounded/4') returns within `5 * TimeoutMs' — the request itself
%% (T), the proof that the upload worker released the transport
%% (`release_request_worker/4': T for the worker exit, T for the owner
%% cleanup) and the transport cleanup proof with its acknowledgement
%% (`close_request/1': T plus T) — while `couchbeam_view:fetch_bounded/4'
%% returns within `4 * TimeoutMs', because its collector caps the cleanup
%% wait independently of the stream process (its delivery allowance sits one
%% `TimeoutMs' above the stream's acknowledgement deadline, see
%% `couchbeam_view:bounded_cleanup_delivery_budget/1'). `TimeoutMs' above
%% `?MAX_BUDGET_TIMEOUT_MS' (`16#FFFFFFFF div 3') is refused: the longest
%% derived wait, `3 * TimeoutMs', must fit the Erlang timer range.
%% `MaxResponseBytes' caps the payload as it is received — headers and
%% chunked-transfer framing are not counted, and a chunk is compared after it
%% has arrived — so the raw payload held by one call is `MaxResponseBytes'
%% plus one transport chunk, plus the single flat copy `bounded_body_done/4'
%% makes before decoding. The decoded term is a further copy on top of that:
%% on the document doors it is built in the decoder process and copied to
%% the caller with the result, on the view door every row travels decoder →
%% stream → collector.
%% One cost is not in the budget and belongs in the caller's plan: a bounded
%% call never returns its connection to hackney's pool. Every terminal path
%% goes through `close_request/1', which kills the transport lease, and the
%% manager closes the socket -- so unlike the legacy path, each call pays a
%% fresh connect (and TLS handshake).
-spec new_request_budget(request_budget_spec()) ->
          {'ok', request_budget()} | {'error', 'invalid_request_budget'}.
new_request_budget({TimeoutMs, MaxResponseBytes})
  when is_integer(TimeoutMs), TimeoutMs > 0,
       TimeoutMs =< ?MAX_BUDGET_TIMEOUT_MS,
       is_integer(MaxResponseBytes), MaxResponseBytes > 0 ->
    {'ok', #{'deadline_ms' => erlang:monotonic_time('millisecond') + TimeoutMs,
             'timeout_ms' => TimeoutMs,
             'max_response_bytes' => MaxResponseBytes}};
new_request_budget(_) ->
    {'error', 'invalid_request_budget'}.

%% @doc Query parameters and request options travel to
%% `hackney_url:make_url/3' as `{Key, Value}' pairs, and `hackney_url:qs/1'
%% renders both halves through `hackney_bstr:to_binary/1', which accepts
%% binaries, Latin-1 atoms, integers and Latin-1 strings only. Any other key
%% or value (a float, a tuple, a map, an atom or a string with code points
%% above 255) crashes URL building after the budget clock started; a
%% malformed entry (`{accept, _, _}') would also slip past the per-key
%% refusal scans of the doors. Answers the first offending entry, the
%% non-list tail of an improper list, or the whole term when it is not a
%% list. Walked by hand rather than through `lists:dropwhile/2', which has no
%% clause for an improper tail.
%%
%% A line terminator (CR or LF) in either half of a pair is refused as well,
%% see `request_line_safe/1'. For query pairs this refusal duplicates what
%% `hackney_url:qs/1' does today (it percent-encodes both halves); it
%% stands because whether a door can split a request must be a
%% contract of the door, not a property of the dependency's encoder —
%% GHSA-j9wq-vxxc-94wf / CVE-2026-47075 (`hackney_url:make_url/3' passing a
%% raw query string through unencoded) is the class.
-spec invalid_query_param(term()) -> 'undefined' | {'invalid_param', term()}.
invalid_query_param([]) ->
    'undefined';
invalid_query_param([Entry | Rest]) ->
    case valid_query_param(Entry) of
        'true' -> invalid_query_param(Rest);
        'false' -> {'invalid_param', Entry}
    end;
invalid_query_param(Tail) ->
    {'invalid_param', Tail}.

-spec valid_query_param(term()) -> boolean().
valid_query_param({Key, Value}) ->
    renderable_query_term(Key) andalso renderable_query_term(Value);
valid_query_param(_Entry) ->
    'false'.

%% `hackney_bstr:to_binary/1' renders an atom with `atom_to_binary/2' in
%% `latin1', so an atom is only as renderable as its name. Renderable also
%% means free of CR and LF, on every shape that carries characters.
-spec renderable_query_term(term()) -> boolean().
renderable_query_term(Term) when is_binary(Term); is_list(Term) ->
    request_line_safe(Term);
renderable_query_term(Term) when is_integer(Term) ->
    'true';
renderable_query_term(Term) when is_atom(Term) ->
    renderable_query_chars(atom_to_list(Term));
renderable_query_term(_Term) ->
    'false'.

%% @doc A term the doors may write into the request line: a binary or a
%% Latin-1 string carrying neither CR nor LF — and only those two shapes.
%% A byte above 127 in a name reaches the path percent-encoded as that
%% byte, whether the name is a binary or a Latin-1 string
%% (`hackney_url:make_url/3' turns a string into bytes with
%% `list_to_binary/1', then `pathencode/1' writes `%E9'); a direct
%% caller's string URL goes through `hackney_url:parse_url/1', which
%% encodes it as UTF-8 first (`%C3%A9').
%% An iolist, a `#hackney_url{}' or a string beyond Latin-1, all of which
%% hackney itself accepts, are refused here by design. This is the contract
%% behind every address half a bounded call sends — the database name, the
%% document id, the design and view names, a `list' function name, each
%% query half — and, through `request_bounded/7', behind the URL of every
%% bounded request, whether it comes from a door, from `db_request_bounded/7'
%% or from a direct caller (the consumer's maintenance module builds its
%% `_find'/`_bulk_docs' URLs itself). The method (`usable_method/1': letters
%% only) and the headers (`header_half_safe/1': a half, or hackney's
%% `{Value, Params}' form) narrow or widen it the way the wire does. A
%% direct caller owns its path segments: by the time the transport door sees
%% the URL a `?' in it is the query, so `addressable/1' is exported for such
%% a caller to judge its database name with.
%%
%% Why the door checks and not the encoder: `hackney_url:make_url/3' joins
%% path parts raw; `hackney_url:parse_url/1' then cuts the URL at the first
%% `#' (the fragment is never sent — on the direct connections the door
%% requires, see `unsafe_request_line/2') and at the first `?' (the rest is the
%% query, sent as it is), and only the path half is percent-encoded
%% (`hackney_url:pathencode/1', replaceable by the `path_encode_fun'
%% request option) before the request line is written (`cut_query/1' and
%% `cut_fragment/1' on the pinned 1.25.0). So a
%% database, design or view name carrying `?' followed by a line terminator
%% splits the request on the pinned hackney as it is; a document id survives
%% only because `couchbeam_util:encode_docid/1' urlencodes it, a query half
%% only because `hackney_url:qs/1' does, a header only for as long as its
%% value carries none (`hackney_headers_new:to_iolist/1' writes
%% `Name: Value' raw). The legacy doors, `couchbeam_view:show/4' with its raw
%% `query_string' option and every other official entry point stay as
%% official code leaves them: protected by those encoders alone.
-spec request_line_safe(term()) -> boolean().
request_line_safe(Term) when is_binary(Term) ->
    'nomatch' =:= binary:match(Term, [<<"\r">>, <<"\n">>]);
request_line_safe(Term) when is_list(Term) ->
    renderable_query_chars(Term);
request_line_safe(_Term) ->
    'false'.

%% @doc A term the doors may place into the request path as one segment: a
%% non-empty `request_line_safe/1' term carrying none of `?', `#', `/', a
%% space or a tab, and not a dot segment. `?', `#' and `/' re-address the
%% request: hackney cuts the URL at `?' and `#', so `PUT /db#x/doc' reaches
%% `/db' and creates a database and `GET /db?x/doc' answers the database
%% with a query; `pathencode/1' keeps a slash, so `PUT /db/x/doc' writes an
%% attachment of document `x' (Kazoo names its databases `account%2F…',
%% percent-encoded, and that form passes); and an empty name — or one
%% `fix_path/1' strips to empty, `/' — collapses the path (`PUT /doc'
%% creates a database). A space, a tab, any other C0 control and DEL are
%% what the transport door refuses in the request line (a tab is refused
%% with the controls, below 32): judged here, the name is refused before
%% any budget and not there, after one (on the view door, after the stream
%% started). `.' and `..' pass `pathencode/1' as they are, and an
%% intermediary that normalises dot segments re-addresses `PUT /../doc' to
%% `/doc' — the same class as `/'; judged after percent-decoding, on every
%% `/'-separated segment of the decoded name (`segments_usable/1'), so
%% `%2E%2E', `..%2Fx' and `%2F' are refused with `..'. This is the contract
%% of the database name, the design and view names and a `list' function
%% name; a document id is urlencoded by
%% `couchbeam_util:encode_docid/1' and keeps the wider `request_line_safe/1'
%% contract.
-spec addressable(term()) -> boolean().
addressable(Term) when is_binary(Term); is_list(Term) ->
    Term =/= <<>> andalso Term =/= []
        andalso request_line_safe(Term)
        andalso addressable_bytes(iolist_to_binary(Term));
addressable(_Term) ->
    'false'.

-spec addressable_bytes(binary()) -> boolean().
addressable_bytes(Bin) ->
    segments_usable(hackney_url:urldecode(Bin, 'skip'))
        andalso lists:all(fun(C) -> C >= 32 andalso C =/= 127 end,
                          binary_to_list(Bin))
        andalso 'nomatch' =:= binary:match(Bin, [<<"?">>, <<"#">>, <<"/">>,
                                                <<" ">>]).

%% @private Exported for `couchbeam', not for consumers.
%% @doc Every `/'-separated segment of a percent-decoded path half is a
%% name: not empty, not `.', not `..'. Why the decoded form: RFC 3986
%% lets a normaliser decode the unreserved characters (`%2E' is `.',
%% section 6.2.2.2) and then remove the dot segments (section 6.2.2.3), so
%% `%2E%2E' can become `..' on the way; `/' is reserved and a conforming
%% normaliser keeps `%2F', but CouchDB itself reads `%2F' in a name as `/'
%% (Kazoo's `account%2Fab' is the database `account/ab' on the server), so
%% a segment behind it is judged as the server sees it — `..%2Fx' is `../x'
%% and `%2F' is two empty names. `%3F', `%23' and `%0D%0A' decode to
%% reserved or control characters no normaliser touches and are data. The
%% decode is single, as one normalisation pass is: `%252E%252E' decodes to
%% `%2E%2E' and passes, and only a second decoding hop would see `..'
%% there — a hop that would equally re-address any other percent-encoding
%% and is not modelled here. The document id, split the same way in
%% `couchbeam:usable_doc_id/1', shares the predicate (its policy: an owner
%% decision recorded with the port).
-spec segments_usable(binary()) -> boolean().
segments_usable(Decoded) ->
    lists:all(fun(Segment) ->
                      Segment =/= <<>> andalso Segment =/= <<".">>
                          andalso Segment =/= <<"..">>
              end, binary:split(Decoded, <<"/">>, ['global'])).

%% `io_lib:latin1_char_list/1' answers `false' on anything but a flat list of
%% Latin-1 characters, so the membership scans only ever run on one.
-spec renderable_query_chars(list()) -> boolean().
renderable_query_chars(Chars) ->
    io_lib:latin1_char_list(Chars)
        andalso not lists:member($\r, Chars)
        andalso not lists:member($\n, Chars).

%% @doc Read and decode the whole body of a bounded request `Ref' within
%% `Budget'. Must run in the process that adopted `Ref' via
%% `request_bounded/6,7' (the ownership record lives in its process
%% dictionary). The transport is closed and its cleanup proven before any
%% result is returned; a body fully received but not decoded before the
%% absolute deadline is reported as `{'error', 'timeout'}'.
-spec bounded_json_body(reference(), request_budget()) ->
          {'ok', term(), non_neg_integer()} |
          {'error', 'timeout' | 'response_too_large' |
                    'invalid_request_budget' | term()}.
bounded_json_body(Ref, #{'deadline_ms' := _,
                         'timeout_ms' := _,
                         'max_response_bytes' := _}=Budget) ->
    case bounded_binary_body(Ref, Budget) of
        {'ok', Body, Bytes} -> decode_bounded_json(Body, Bytes, Budget);
        {'error', _}=Error -> Error
    end;
bounded_json_body(Ref, _) ->
    close_request_result(Ref, {'error', 'invalid_request_budget'}).

%% @doc Encode `Term' to JSON in a killable worker bounded by `Budget'.
%% The caller-facing arity: no `src/' call site uses it, both encode through
%% the arity-3 form, which carries the TEST delay hook. Exported (and pinned
%% by `official_and_bounded_api_exports_coexist_test') because a consumer that
%% builds its own JSON body under a budget has no reason to know about hooks.
-spec bounded_encode_json(term(), request_budget()) ->
          {'ok', binary()} | {'error', 'timeout' | term()}.
bounded_encode_json(Term, Budget) ->
    bounded_encode_json(Term, Budget, []).

-spec bounded_encode_json(term(), request_budget(), list()) ->
          {'ok', binary()} | {'error', 'timeout' | term()}.
bounded_encode_json(Term, #{'deadline_ms' := _, 'timeout_ms' := _,
                            'max_response_bytes' := _}=Budget, Options) ->
    %% `Options' is `#db.options', read here for one thing only: the TEST
    %% delay hook, resolved in this process (outside TEST the clause is a
    %% constant 0). The list is not captured by the worker closure below, so
    %% the credentials a `basic_auth' entry carries do not travel into the
    %% spawned process.
    Delay = encode_test_delay(Options),
    run_bounded_worker(
      fun() ->
              maybe_test_delay(Delay),
              %% Caught inside the worker, like `decode_json_result/1' does:
              %% an unencodable term (a pid or a fun inside the document) is
              %% an input error with a typed verdict, not a crash report from
              %% an anonymous process.
              try couchbeam_ejson:encode(Term) of
                  Encoded -> Encoded
              catch
                  Class:Reason -> {'encode_error', Class, Reason}
              end
      end,
      %% `couchbeam_ejson:encode/1' answers a binary from jsx and iodata from
      %% jiffy (`WITH_JIFFY' builds); both are one JSON document. The iodata
      %% clause is exercised by no gate of this lineage: the suite runs on
      %% jsx and Kazoo builds the dependency without `WITH_JIFFY'.
      fun(Encoded) when is_binary(Encoded) -> {'ok', Encoded};
         (Encoded) when is_list(Encoded) -> {'ok', iolist_to_binary(Encoded)};
         ({'encode_error', Class, Reason}) ->
              {'error', {'invalid_json_encoding', {Class, Reason}}};
         (Unexpected) -> {'error', {'invalid_json_encoding', Unexpected}}
      end,
      Budget);
bounded_encode_json(_Term, _Budget, _Options) ->
    %% Refused before any worker is spawned, like every other bounded door.
    {'error', 'invalid_request_budget'}.

%% An already expired budget is refused before the worker exists: spawning
%% an encoder (and its guard) only to kill it would start work for nothing.
-spec run_bounded_worker(fun(() -> term()), fun((term()) -> term()),
                         request_budget()) -> term().
run_bounded_worker(WorkFun, ResultFun, Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            run_bounded_worker(WorkFun, ResultFun, Budget, TimeoutMs);
        _ ->
            {'error', 'timeout'}
    end.

-spec run_bounded_worker(fun(() -> term()), fun((term()) -> term()),
                         request_budget(), pos_integer()) -> term().
run_bounded_worker(WorkFun, ResultFun, Budget, TimeoutMs) ->
    Parent = self(),
    Token = make_ref(),
    {WorkerPid, MonitorRef} = spawn_monitor(
                                fun() -> Parent ! {Token, WorkFun()} end),
    guard_ephemeral_worker(Parent, WorkerPid),
    receive
        {Token, Result} ->
            erlang:demonitor(MonitorRef, ['flush']),
            case budget_status(Budget) of
                'ok' -> ResultFun(Result);
                {'error', 'timeout'} -> {'error', 'timeout'}
            end;
        {'DOWN', MonitorRef, 'process', WorkerPid, Reason} ->
            flush_request_result(Token),
            {'error', {'json_encoding_failed', Reason}}
    after TimeoutMs ->
            exit(WorkerPid, 'kill'),
            _ = await_worker_down(
                  WorkerPid, MonitorRef, cleanup_deadline(Budget)),
            flush_request_result(Token),
            {'error', 'timeout'}
    end.

%% A streamed multipart body names two headers hackney writes after its
%% request headers have been prepared
%% (`hackney_request:handle_multipart_body/5'): the caller's boundary is
%% interpolated into `Content-Type', and the size is stored as
%% `Content-Length' as it is — `chunked' selects `Transfer-Encoding:
%% chunked' instead, an integer is the length, anything else reaches the
%% wire through `hackney_bstr:to_binary/1' unchecked. Judged in the order
%% hackney stores them: the boundary, then the size. The bare
%% `stream_multipart' atom names neither (hackney mints the boundary and
%% streams chunked).
-spec unsafe_body_header(term()) -> 'safe' | {'unsafe', binary()}.
unsafe_body_header({'stream_multipart', Size}) ->
    unsafe_multipart_size(Size);
unsafe_body_header({'stream_multipart', Size, Boundary}) ->
    case multipart_boundary(Boundary) of
        'true' -> unsafe_multipart_size(Size);
        'false' -> {'unsafe', <<"Content-Type">>}
    end;
unsafe_body_header(_Body) ->
    'safe'.

%% hackney writes `multipart/form-data; boundary=' and the boundary raw —
%% unquoted — so the boundary must be both an RFC 2046 boundary (section
%% 5.1.1: 1 to 70 characters, not ending in a space) and an HTTP token: of
%% the `bchars' set, `()/:=?,' and space are `tspecials' a receiver stops
%% the token at, so a boundary carrying one is read short and the parts
%% never match it, and a `;' or a `"' would start another parameter of
%% `Content-Type' outright. What is left is DIGIT / ALPHA / `'+_-.'.
-spec multipart_boundary(term()) -> boolean().
multipart_boundary(Boundary) when is_binary(Boundary) ->
    Size = byte_size(Boundary),
    Size >= 1 andalso Size =< 70
        andalso lists:all(fun boundary_char/1, binary_to_list(Boundary));
multipart_boundary(_Boundary) ->
    'false'.

-spec boundary_char(byte()) -> boolean().
boundary_char(C) ->
    (C >= $a andalso C =< $z) orelse (C >= $A andalso C =< $Z)
        orelse (C >= $0 andalso C =< $9)
        orelse lists:member(C, "'+_-.").

-spec unsafe_multipart_size(term()) -> 'safe' | {'unsafe', binary()}.
unsafe_multipart_size('chunked') ->
    'safe';
unsafe_multipart_size(Size) when is_integer(Size), Size >= 0 ->
    'safe';
unsafe_multipart_size(_Size) ->
    {'unsafe', <<"Content-Length">>}.

%% @private Exported for `couchbeam_view_stream', not for consumers.
%% @doc Bind an ephemeral worker's lifetime to `Parent'. A worker that is
%% only reachable through a killable call must not outlive the process that
%% wanted its result: without this the worker runs to completion on a body
%% that may be as large as `max_response_bytes'.
-spec guard_ephemeral_worker(pid(), pid()) -> 'ok'.
guard_ephemeral_worker(Parent, WorkerPid) ->
    _ = spawn(fun() ->
                      ParentRef = erlang:monitor('process', Parent),
                      WorkerRef = erlang:monitor('process', WorkerPid),
                      receive
                          {'DOWN', ParentRef, 'process', Parent, _Reason} ->
                              exit(WorkerPid, 'kill');
                          {'DOWN', WorkerRef, 'process', WorkerPid, _Reason} ->
                              'ok'
                      end
              end),
    'ok'.

%% @doc Close a bounded request and prove its transport cleanup. Process
%% affine: only the process that adopted `Ref' through `request_bounded/6,7'
%% holds the ownership record; from any other process the call degrades to a
%% best-effort `hackney' close without the guardian cleanup proof.
-spec cancel_request(reference()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
cancel_request(Ref) ->
    close_request(Ref).

-spec bounded_binary_body(reference(), request_budget()) ->
          {'ok', binary(), non_neg_integer()} | {'error', term()}.
bounded_binary_body(Ref, Budget) ->
    bounded_body(Ref, Budget, [], 0).

-spec bounded_body(reference(), request_budget(), [binary()],
                   non_neg_integer()) ->
          {'ok', binary(), non_neg_integer()} | {'error', term()}.
bounded_body(Ref, Budget, Acc, Bytes) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            case hackney:stream_next(Ref) of
                'ok' ->
                    receive
                        {'hackney_response', Ref, Message} ->
                            bounded_body_chunk(
                              Message, Ref, Budget, Acc, Bytes)
                    after TimeoutMs ->
                            close_request_result(Ref, {'error', 'timeout'})
                    end;
                {'error', Reason} ->
                    close_request_result(Ref, {'error', Reason})
            end;
        _ ->
            close_request_result(Ref, {'error', 'timeout'})
    end.

-spec bounded_body_chunk(term(), reference(), request_budget(), [binary()],
                         non_neg_integer()) ->
          {'ok', binary(), non_neg_integer()} | {'error', term()}.
bounded_body_chunk(Chunk, Ref,
                   #{'max_response_bytes' := MaxBytes}=Budget, Acc, Bytes) ->
    case is_binary(Chunk) of
        'true' ->
            NewBytes = Bytes + byte_size(Chunk),
            case NewBytes =< MaxBytes of
                'true' -> bounded_body(
                            Ref, Budget, [Chunk | Acc], NewBytes);
                'false' ->
                    close_request_result(
                      Ref, {'error', 'response_too_large'})
            end;
        'false' ->
            bounded_body_control(Chunk, Ref, Budget, Acc, Bytes)
    end.

-spec bounded_body_control(term(), reference(), request_budget(), [binary()],
                           non_neg_integer()) ->
          {'ok', binary(), non_neg_integer()} | {'error', term()}.
bounded_body_control('done', Ref, Budget, Acc, Bytes) ->
    bounded_body_done(Ref, Budget, Acc, Bytes);
bounded_body_control({'error', Reason}, Ref, _Budget, _Acc, _Bytes) ->
    close_request_result(Ref, {'error', Reason});
bounded_body_control(Unexpected, Ref, _Budget, _Acc, _Bytes) ->
    close_request_result(
      Ref, {'error', {'unexpected_response_message', Unexpected}}).

-spec bounded_body_done(reference(), request_budget(), [binary()],
                        non_neg_integer()) ->
          {'ok', binary(), non_neg_integer()} |
          {'error', 'timeout' | 'transport_cleanup_timeout'}.
bounded_body_done(Ref, Budget, Acc, Bytes) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            close_request_result(
              Ref, {'ok', iolist_to_binary(lists:reverse(Acc)), Bytes});
        _ ->
            close_request_result(Ref, {'error', 'timeout'})
    end.

-spec decode_bounded_json(binary(), non_neg_integer(), request_budget()) ->
          {'ok', term(), non_neg_integer()} | {'error', term()}.
decode_bounded_json(Body, Bytes, Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            decode_with_watchdog(Body, Bytes, Budget);
        _ ->
            {'error', 'timeout'}
    end.

-spec decode_with_watchdog(binary(), non_neg_integer(), request_budget()) ->
          {'ok', term(), non_neg_integer()} | {'error', term()}.
decode_with_watchdog(Body, Bytes, Budget) ->
    Parent = self(),
    Token = make_ref(),
    Delay = caller_decode_test_delay(),
    {DecoderPid, MonitorRef} = spawn_monitor(
                                 fun() ->
                                         maybe_test_delay(Delay),
                                         Parent ! {Token,
                                                   decode_json_result(Body)}
                                 end),
    guard_ephemeral_worker(Parent, DecoderPid),
    WatchdogMs = erlang:max(0, remaining_timeout(Budget)),
    receive
        {Token, {'ok', Json}} ->
            erlang:demonitor(MonitorRef, ['flush']),
            guarded_decode_result({'ok', Json}, Bytes, Budget);
        {Token, {'error', _}=Error} ->
            erlang:demonitor(MonitorRef, ['flush']),
            guarded_decode_result(Error, Bytes, Budget);
        {'DOWN', MonitorRef, 'process', DecoderPid, Reason} ->
            %% The decoder catches its own JSON errors
            %% (`decode_json_result/1'); a `'DOWN'' is a decoder killed from
            %% outside — a transient failure, not a malformed body.
            flush_decode_result(Token),
            {'error', {'json_decoding_failed', Reason}}
    after WatchdogMs ->
            exit(DecoderPid, 'kill'),
            %% An unconditional wait, deliberately: the process was just sent
            %% an untrappable kill, so the monitor message is guaranteed and
            %% bounded by the scheduler, not by the request deadline.
            receive
                {'DOWN', MonitorRef, 'process', DecoderPid, _} -> 'ok'
            end,
            flush_decode_result(Token),
            {'error', 'timeout'}
    end.

-spec guarded_decode_result({'ok', term()} | {'error', term()},
                            non_neg_integer(), request_budget()) ->
          {'ok', term(), non_neg_integer()} | {'error', term()}.
guarded_decode_result(Result, Bytes, Budget) ->
    case remaining_timeout(Budget) of
        Remaining when Remaining > 0 ->
            case Result of
                {'ok', Json} -> {'ok', Json, Bytes};
                {'error', _}=Error -> Error
            end;
        _ ->
            {'error', 'timeout'}
    end.

-spec decode_json_result(binary()) -> {'ok', term()} | {'error', term()}.
decode_json_result(Body) ->
    try couchbeam_ejson:decode(Body) of
        Json -> {'ok', Json}
    catch
        Class:Reason -> {'error', {'invalid_json', {Class, Reason}}}
    end.

-spec flush_decode_result(reference()) -> 'ok'.
flush_decode_result(Token) ->
    receive
        {Token, _} -> flush_decode_result(Token)
    after 0 ->
            'ok'
    end.

-spec bounded_request(term(), term(), list(), term(), list(),
                      request_budget()) ->
          {'ok', integer(), list(), reference()} | {'error', term()}.
bounded_request(Method, Url, Headers, Body, Options, Budget) ->
    case request_bounded(Method, Url, Headers, Body, Options, Budget) of
        {'ok', Ref} -> bounded_response_status(Ref, Budget);
        Error -> Error
    end.

%% @doc Start a bounded asynchronous request owned by the calling process.
%% @equiv request_bounded(Method, Url, Headers, Body, Options, Budget, self())
-spec request_bounded(term(), term(), list(), term(), list(), request_budget()) ->
          {'ok', reference()} | {'error', term()}.
request_bounded(Method, Url, Headers, Body, Options, Budget) ->
    request_bounded(Method, Url, Headers, Body, Options, Budget, self()).

%% @doc Start a bounded asynchronous request. `Budget' is the single source of
%% the deadline and derives the `hackney' connect/receive/checkout timeouts;
%% a `request_budget' entry in `Options' is inert caller data (it travels to
%% `hackney', which ignores it), never an instruction. Kazoo headers are prepared in the
%% calling process before the killable worker is spawned, so the caller's
%% process dictionary context is what reaches the wire. The returned `Ref' is
%% adopted by the calling process; `bounded_json_body/2' and `cancel_request/1'
%% must be invoked from it. When `LifecycleOwner' differs from the caller, the
%% owner receives the guardian lifecycle protocol
%% `{'bounded_transport_guardian', Caller, GuardianPid}',
%% `{'bounded_transport_cleanup_started', Caller, CleanupBudget}' and
%% `{'bounded_transport_cleanup', Caller, Ref | 'undefined'}' and must consume
%% it (see `couchbeam_view:fetch_bounded/4'); its death closes the transport
%% even while the caller is blocked. The path segments of `Url' are the
%% caller's: a `?' in one is the query by the time this door sees it, so a
%% direct caller judges each segment with `addressable/1' before building
%% the URL (see `db_request_bounded/7'). `Options' is a proper list by
%% type; one that is not raises in the calling process, before any worker
%% or connection. Refusals here come in the order of the request line, the
%% options and the headers; the shape of `Budget' is the type's contract
%% (`db_request_bounded/7' judges it first, this door inside
%% `prepare_request/5').
-spec request_bounded(term(), term(), list(), term(), list(), request_budget(),
                      pid()) ->
          {'ok', reference()} | {'error', term()}.
request_bounded(Method, Url, Headers, Body, Options, Budget,
                LifecycleOwner) ->
    case unsafe_request_line(Method, Url) of
        'undefined' ->
            case follow_redirect_requested(Options) of
                'true' ->
                    {'error', {'unsupported_option', 'follow_redirect'}};
                'false' ->
                    %% Read as hackney reads its options
                    %% (`proplists:get_value/2'): a bare atom is the option
                    %% set to `true', and `path_encode_fun = true' has no
                    %% clause in `hackney_url:normalize/2'.
                    case proplists:is_defined('proxy', Options) of
                        'true' -> {'error', {'unsupported_option', 'proxy'}};
                        'false' ->
                            case proplists:is_defined('path_encode_fun',
                                                      Options) of
                                'true' ->
                                    {'error', {'unsupported_option',
                                               'path_encode_fun'}};
                                'false' ->
                                    request_bounded_checked(
                                      Method, Url, Headers, Body, Options,
                                      Budget, LifecycleOwner)
                            end
                    end
            end;
        Refusal ->
            {'error', Refusal}
    end.

%% The last point before hackney, and the only one every bounded caller
%% passes — the doors, `db_request_bounded/7' and a direct caller alike. A
%% method or a URL that would split (CR/LF anywhere in it: the netloc is
%% written as the `Host' header raw, `hackney_request:maybe_add_host/2';
%% only the credentials, which base64 hides, would survive one), truncate
%% (`#': hackney cuts the URL at the first one wherever it sits and never
%% sends the fragment) or malform (C0/DEL in the target, SP/HT or a byte
%% above 127 in the raw query, an empty path) the request line is refused
%% here. SP/HT in the path are also refused by policy:
%% pathencode writes them as `+' / `%09', not as raw whitespace.
%% Judged on what hackney writes after the method —
%% `#hackney_url.raw_path', the path with the raw query and fragment — and
%% not on the netloc, where the consumer keeps its CouchDB credentials raw:
%% a password with a space becomes a `basic_auth' option and an
%% `Authorization' header, never a byte of the request line. A URL
%% `hackney_url:parse_url/1' cannot parse (on 1.25.0 `cut_query/1' cuts the
%% whole URL at the first `?', a `?' inside the credentials included) is
%% refused here rather than crashing the worker under a budget. The refusal
%% names the URL by its path only, as `request_identity/2' names it.
%% This netloc distinction requires direct connections: explicit proxy
%% options are refused; callers must disable environment proxies with
%% no_proxy_env when their environment configures one.
-spec unsafe_request_line(term(), term()) ->
          'undefined' | {'unsafe_method', term()} | {'unsafe_url', term()}.
unsafe_request_line(Method, Url) ->
    case usable_method(Method) of
        'false' ->
            {'unsafe_method', Method};
        'true' ->
            case request_line_safe(Url) andalso raw_path_usable(Url) of
                'true' ->
                    'undefined';
                'false' ->
                    {'unsafe_url',
                     maps:get('path', request_identity(Method, Url))}
            end
    end.

%% Every `%' in the netloc opens a valid escape, and the decoded netloc —
%% what `normalize/2' hands to `maybe_add_host/2' for a DNS host — is
%% printable ASCII without a space. `urldecode/2' in `skip' mode is the
%% same walk `urldecode/1' does in `crash' mode, so a netloc whose escapes
%% are valid decodes identically in both.
-spec netloc_usable(binary()) -> boolean().
netloc_usable(Netloc) ->
    Decoded = hackney_url:urldecode(Netloc, 'skip'),
    percent_escapes_valid(Netloc)
        andalso lists:all(fun(C) -> C > 32 andalso C < 127 end,
                          binary_to_list(Decoded)).

-spec percent_escapes_valid(binary()) -> boolean().
percent_escapes_valid(<<$%, H, L, Rest/binary>>) ->
    hex_digit(H) andalso hex_digit(L) andalso percent_escapes_valid(Rest);
percent_escapes_valid(<<$%, _Rest/binary>>) ->
    'false';
percent_escapes_valid(<<_C, Rest/binary>>) ->
    percent_escapes_valid(Rest);
percent_escapes_valid(<<>>) ->
    'true'.

-spec hex_digit(byte()) -> boolean().
hex_digit(C) ->
    (C >= $0 andalso C =< $9) orelse (C >= $a andalso C =< $f)
        orelse (C >= $A andalso C =< $F).

-spec raw_path_usable(binary() | string()) -> boolean().
raw_path_usable(Url) ->
    try hackney_url:parse_url(Url) of
        #hackney_url{path=Path, raw_path=RawPath, qs=Qs, netloc=Netloc}
          when is_binary(Path), is_binary(RawPath), is_binary(Qs),
               is_binary(Netloc) ->
            %% An empty path with a query is a request target without a
            %% leading slash (`GET ?x=y HTTP/1.1', `hackney:make_request/6'
            %% joins path and query as they are) — and what a `?' inside
            %% the authority leaves behind: `cut_query/1' cuts the whole
            %% URL at the first `?', so `http://u:5984?x@host/db' parses as
            %% host `u', port 5984, no path, and would leave for a host the
            %% caller never named. An empty path WITHOUT a query is the
            %% legal `http://host:5984', which `hackney_request:perform/2'
            %% writes as `GET / HTTP/1.1'; it passes. The raw query is
            %% written unencoded, so a byte above 127 in it is a malformed
            %% target as well; the path half is percent-encoded by
            %% `pathencode/1' and keeps Latin-1.
            %% The netloc becomes the `Host' header, but not always as
            %% written: `hackney_url:normalize/2' keeps it byte for byte
            %% only when the host is an IP literal, and for a DNS name it
            %% percent-decodes the host in CRASH mode
            %% (`hackney_url:urldecode/1'), IDN-converts it and rebuilds
            %% the netloc. So a `%' that is not a valid escape kills the
            %% worker after the budget, and `ho%20st' would put a raw space
            %% into the `Host' header — the netloc is judged as hackney
            %% will write it, decoded, and its escapes are judged valid.
            (Path =/= <<>> orelse Qs =:= <<>>)
                andalso netloc_usable(Netloc)
                andalso 'nomatch' =:= binary:match(RawPath, [<<"#">>, <<" ">>])
                andalso lists:all(fun(C) -> C >= 32 andalso C =/= 127 end,
                                  binary_to_list(RawPath))
                andalso lists:all(fun(C) -> C < 128 end, binary_to_list(Qs));
        #hackney_url{} ->
            'false'
    catch
        _Class:_Reason -> 'false'
    end.

%% A method hackney renders with `hackney_bstr:to_binary/1' and uppercases:
%% an atom, a binary or a string of letters only, judged code point by code
%% point (an atom is Unicode on OTP 27, and `atom_to_binary/2' with `latin1'
%% raises on a code point above 255 where a refusal is due; a `$'-anchored
%% regex lets a bare trailing LF through). A space or a tab in it re-parses
%% the request target on a lenient server (`GET /x HTTP/1.1 /db …'), an
%% integer is a nonsense token, and CR/LF splits the line. `connect' is
%% refused by name although it is letters: `hackney:make_request/6' has a
%% clause of its own for it that writes `CONNECT host:port' and drops the
%% path, a tunnel flow the bounded reader was never written for.
-spec usable_method(term()) -> boolean().
usable_method(Method)
  when is_atom(Method); is_binary(Method); is_list(Method) ->
    Chars = method_chars(Method),
    letters_only(Chars)
        andalso "connect" =/= string:lowercase(Chars);
usable_method(_Method) ->
    'false'.

%% Only reached for the three shapes `hackney_bstr:to_binary/1' renders.
-spec method_chars(atom() | binary() | list()) -> list().
method_chars(Method) when is_atom(Method) -> atom_to_list(Method);
method_chars(Method) when is_binary(Method) -> binary_to_list(Method);
method_chars(Method) -> Method.

%% A non-empty, proper list of ASCII letters.
-spec letters_only(term()) -> boolean().
letters_only([]) ->
    'false';
letters_only(Chars) ->
    letters_only_from(Chars).

-spec letters_only_from(term()) -> boolean().
letters_only_from([]) ->
    'true';
letters_only_from([C | Rest])
  when is_integer(C), C >= $A, C =< $Z;
       is_integer(C), C >= $a, C =< $z ->
    letters_only_from(Rest);
letters_only_from(_Other) ->
    'false'.

%% Every header hackney writes as `Name: Value' without validation, the ones
%% this module adds included: `X-Kazoo-Log-ID' is the caller's call id,
%% which the consumer sets from the request it serves — whether a given
%% ingress lets a line terminator into it is that ingress's matter, the door
%% judges the bytes it is given. Answers a pair by its name, never by its
%% value, and an entry that is not a pair whole, under a tag no header name
%% can collide with. A binary, a Latin-1 string or an iolist of them is a
%% half, and so is hackney's parameterised value `{Value, Params}' when every
%% part is (a parameter is a `{Key, Value}' pair or a bare key, as
%% `hackney_headers_new:params_to_iolist/2' writes them); an atom or an
%% integer renders as `hackney_bstr' renders it. A name is never the
%% parameterised form: `to_iolist/1' lowercases it with
%% `hackney_bstr:to_lower/1', which has no clause for a tuple.
-spec unsafe_header(list()) -> 'safe' | {'unsafe', term()}.
unsafe_header(Headers) ->
    case unsafe_header_entries(Headers) of
        'safe' -> unsafe_framing(filter_undefined_headers(Headers));
        Unsafe -> Unsafe
    end.

-spec unsafe_header_entries(term()) -> 'safe' | {'unsafe', term()}.
unsafe_header_entries([]) ->
    'safe';
unsafe_header_entries([{_Name, 'undefined'} | Rest]) ->
    unsafe_header_entries(Rest);
unsafe_header_entries([{Name, Value} | Rest]) ->
    case header_name_safe(Name) andalso header_value_safe(Name, Value) of
        'true' -> unsafe_header_entries(Rest);
        'false' -> {'unsafe', Name}
    end;
unsafe_header_entries([Entry | _Rest]) ->
    {'unsafe', Entry};
unsafe_header_entries(_Tail) ->
    {'unsafe', 'undefined'}.

%% Hackney preserves caller framing on streamed and empty bodies.
%% Equal lengths are unambiguous; conflicting lengths or TE plus CL are not.
%% Omitted values have already been removed, just as preparation removes them.
-spec unsafe_framing(list()) -> 'safe' | {'unsafe', binary()}.
unsafe_framing(Headers) ->
    Lengths = [canonical_length(V) || {K, V} <- Headers,
                  hackney_bstr:to_lower(K) =:= <<"content-length">>],
    Transfers = [hackney_bstr:to_lower(V) || {K, V} <- Headers,
                    hackney_bstr:to_lower(K) =:= <<"transfer-encoding">>],
    case lists:usort(Lengths) of
        [] -> unsafe_transfer_encoding(Transfers);
        [_] when Transfers =:= [] -> 'safe';
        _ -> {'unsafe', <<"Content-Length">>}
    end.

%% Hackney selects chunk framing only for exactly `chunked', and does not
%% apply any other transfer coding. Repeated fields would declare it twice.
-spec unsafe_transfer_encoding([binary()]) -> 'safe' | {'unsafe', binary()}.
unsafe_transfer_encoding([]) -> 'safe';
unsafe_transfer_encoding([<<"chunked">>]) -> 'safe';
unsafe_transfer_encoding(_Transfers) -> {'unsafe', <<"Transfer-Encoding">>}.

-spec canonical_length(non_neg_integer() | binary()) -> binary().
canonical_length(Value) when is_integer(Value) ->
    integer_to_binary(Value);
canonical_length(<<$0, Rest/binary>>) when Rest =/= <<>> ->
    canonical_length(Rest);
canonical_length(Value) -> Value.

-spec header_name_safe(term()) -> boolean().
header_name_safe(Name) ->
    header_scalar_safe(Name)
        andalso begin
                    Bin = hackney_bstr:to_binary(Name),
                    Bin =/= <<>> andalso lists:all(fun header_token/1,
                                                   binary_to_list(Bin))
                end.

-spec header_token(byte()) -> boolean().
header_token(C) ->
    (C >= $a andalso C =< $z) orelse (C >= $A andalso C =< $Z)
        orelse (C >= $0 andalso C =< $9)
        orelse lists:member(C, "!#$%&'*+-.^_`|~").

-spec header_value_bytes(binary()) -> boolean().
header_value_bytes(Bin) ->
    lists:all(fun(C) -> C =:= 9 orelse (C >= 32 andalso C =/= 127) end,
              binary_to_list(Bin)).

%% The `cookie' request option becomes a `Cookie' header inside hackney
%% (`hackney_request:maybe_add_cookies/2'), after any header scan. A binary
%% is written as it is. `{Name, Value}' and `{Name, Value, Opts}' go through
%% `hackney_cookie:setcookie/3', which refuses — a `badmatch' inside the
%% killable worker, after the budget started — a name carrying `=', `,',
%% `;', a space, a tab, CR, LF, VT or FF and a value carrying any of those
%% but `=', takes each setcookie half as iodata (an atom raises there),
%% and writes the `domain' and `path' options raw after `; Domain=' and
%% `; Path='. A list
%% holds those shapes, each written as its own header. Every shape is judged
%% here, before any budget, by what hackney would write or refuse.
-spec unsafe_cookie(list()) -> 'safe' | {'unsafe', binary()}.
unsafe_cookie(Options) ->
    case cookie_safe(proplists:get_value('cookie', Options, [])) of
        'true' -> 'safe';
        'false' -> {'unsafe', <<"Cookie">>}
    end.

-define(COOKIE_VALUE_REFUSED, [<<",">>, <<";">>, <<" ">>, <<"\t">>,
                               <<"\r">>, <<"\n">>, <<"\v">>, <<"\f">>]).

-spec cookie_safe(term()) -> boolean().
cookie_safe(Cookie) when is_binary(Cookie) ->
    header_half_safe(Cookie);
cookie_safe({Name, Value}) ->
    cookie_pair_safe(Name, Value, []);
cookie_safe({Name, Value, Opts}) ->
    cookie_pair_safe(Name, Value, Opts);
cookie_safe(Cookies) when is_list(Cookies) ->
    cookie_list_safe(Cookies);
cookie_safe(_Other) ->
    'false'.

-spec cookie_list_safe(term()) -> boolean().
cookie_list_safe([]) ->
    'true';
cookie_list_safe([Cookie | Rest]) when is_binary(Cookie); is_tuple(Cookie) ->
    cookie_safe(Cookie) andalso cookie_list_safe(Rest);
cookie_list_safe(_Other) ->
    'false'.

%% The name is judged non-empty for the reason a header name is
%% (`header_name_safe/1'): `hackney_cookie:setcookie/3' accepts an empty one
%% and writes `Cookie: =1; Version=1', a pair with no name. An empty cookie
%% VALUE, and an empty `cookie' binary, are well-formed and stay accepted —
%% they write an empty header value, as any other header may.
-spec cookie_pair_safe(term(), term(), term()) -> boolean().
cookie_pair_safe(Name, Value, Opts) ->
    cookie_half_safe(Name, [<<"=">> | ?COOKIE_VALUE_REFUSED])
        andalso iolist_size(Name) > 0
        andalso cookie_half_safe(Value, ?COOKIE_VALUE_REFUSED)
        andalso cookie_opts_safe(Opts).

-spec cookie_half_safe(term(), [binary()]) -> boolean().
cookie_half_safe(Half, Refused) when is_binary(Half); is_list(Half) ->
    header_half_safe(Half)
        andalso 'nomatch' =:= binary:match(iolist_to_binary(Half), Refused);
cookie_half_safe(_Half, _Refused) ->
    'false'.

%% Scan the list directly: keyfind accepts larger tuples and raises on
%% improper lists. Unknown options are ignored by setcookie; known options
%% must have the shapes its rendering clauses accept.
-spec cookie_opts_safe(term()) -> boolean().
cookie_opts_safe([]) ->
    'true';
cookie_opts_safe([{Key, Value} | Rest]) ->
    cookie_opt_safe(Key, Value) andalso cookie_opts_safe(Rest);
cookie_opts_safe([Other | Rest]) when is_tuple(Other), tuple_size(Other) > 0 ->
    not lists:member(element(1, Other),
                     ['domain', 'path', 'secure', 'http_only', 'max_age'])
        andalso cookie_opts_safe(Rest);
cookie_opts_safe([_Ignored | Rest]) ->
    cookie_opts_safe(Rest);
cookie_opts_safe(_Opts) ->
    'false'.

-spec cookie_opt_safe(term(), term()) -> boolean().
cookie_opt_safe(Key, Value) when Key =:= 'secure'; Key =:= 'http_only' ->
    Value =:= 'true';
cookie_opt_safe('max_age', Value) ->
    is_integer(Value) andalso Value >= 0;
cookie_opt_safe(Key, Value) when Key =:= 'domain'; Key =:= 'path' ->
    %% Written raw after `; Domain=' / `; Path=': a `;' or `,' inside would
    %% end the attribute and start another cookie pair on the same header.
    cookie_half_safe(Value, [<<";">>, <<",">>]);
cookie_opt_safe(_Key, _Value) ->
    'true'.

%% hackney reads four request headers back before it writes them, and each
%% reader takes a different shape — so the rule is per name, not one rule
%% for the four. Common to all of them: hackney reads the value with
%% `hackney_headers_new:get_value/2', which returns the parameterised
%% `{Value, Params}' form whole, and every reader then hands it to
%% `hackney_bstr:to_binary/1', which has no tuple clause — that form is
%% refused on these four names and stays legal on every other.
%% Beyond that: `expect' and `transfer-encoding' go through
%% `hackney_bstr:to_lower/1' (`hackney_request:expectation/1',
%% `req_type/2'), which renders an atom or a Latin-1 string through
%% `to_binary/1' as well, so any scalar is safe there; `content-type' is
%% parsed by `hackney_headers_new:parse_content_type/1' under a streamed
%% multipart body, and that parser matches binaries only; `content-length'
%% is stored back as the framing of the request, so it must be what a
%% Content-Length may say — a nonnegative integer or a binary of digits.
%% Nothing here rests on hackney overwriting a caller's Content-Length: it
%% does for a plain body (`handle_body/4' recomputes it), and does not for
%% a streamed or function body, which is the case this refusal is for.
-spec header_value_safe(term(), term()) -> boolean().
header_value_safe(Name, {_Value, _Params}=Half) ->
    not hackney_reads_back(Name) andalso header_half_safe(Half);
header_value_safe(Name, Half) ->
    case hackney_bstr:to_lower(hackney_bstr:to_binary(Name)) of
        <<"content-type">> -> is_binary(Half) andalso header_value_bytes(Half);
        <<"content-length">> -> content_length_value(Half);
        _Other -> header_half_safe(Half)
    end.

%% Judged after `header_name_safe/1', so the name renders as a binary.
-spec hackney_reads_back(term()) -> boolean().
hackney_reads_back(Name) ->
    lists:member(hackney_bstr:to_lower(hackney_bstr:to_binary(Name)),
                 [<<"expect">>, <<"transfer-encoding">>, <<"content-type">>,
                  <<"content-length">>]).

-spec content_length_value(term()) -> boolean().
content_length_value(Value) when is_integer(Value) ->
    Value >= 0;
content_length_value(Value) when is_binary(Value) ->
    Value =/= <<>>
        andalso lists:all(fun(C) -> C >= $0 andalso C =< $9 end,
                          binary_to_list(Value));
content_length_value(_Value) ->
    'false'.

-spec header_half_safe(term()) -> boolean().
header_half_safe({Value, Params}) when is_list(Params) ->
    header_scalar_safe(Value) andalso header_params_safe(Params);
header_half_safe(Term) ->
    header_scalar_safe(Term).

-spec header_scalar_safe(term()) -> boolean().
header_scalar_safe(Term) when is_binary(Term); is_list(Term) ->
    try iolist_to_binary(Term) of
        Bin -> header_value_bytes(Bin)
    catch
        'error':'badarg' -> 'false'
    end;
header_scalar_safe(Term) when is_atom(Term); is_integer(Term) ->
    renderable_query_term(Term)
        andalso header_value_bytes(hackney_bstr:to_binary(Term));
header_scalar_safe(_Term) ->
    'false'.

-spec header_params_safe(term()) -> boolean().
header_params_safe([]) ->
    'true';
header_params_safe([{K, V} | Rest]) ->
    header_scalar_safe(K) andalso header_scalar_safe(V)
        andalso header_params_safe(Rest);
header_params_safe([K | Rest]) when not is_tuple(K) ->
    header_scalar_safe(K) andalso header_params_safe(Rest);
header_params_safe(_Other) ->
    'false'.

%% A redirect makes `hackney' emit `redirect'/`see_other' control messages the
%% bounded reader has no clause for, and it re-targets the request at a host
%% the caller never named. Refuse it instead of failing later as an
%% `unexpected_response_message'.
-spec follow_redirect_requested(list()) -> boolean().
follow_redirect_requested(Options) ->
    couchbeam_util:get_value('follow_redirect', Options) =:= 'true'.

-spec request_bounded_checked(term(), term(), list(), term(), list(),
                              request_budget(), pid()) ->
          {'ok', reference()} | {'error', term()}.
request_bounded_checked(Method, Url, Headers, Body, Options, Budget,
                        LifecycleOwner) ->
    %% The caller's list is scanned twice, and this pass is the one that
    %% has to be first: `prepare_request/5' walks the list in THIS process
    %% before it prepends anything — `make_header_accept/1' reads it with
    %% `couchbeam_util:get_value/2' (`lists:keyfind/3', then
    %% `lists:member/2') and `filter_undefined_headers/1' folds it with
    %% `lists:filter/2' — so an improper tail or a non-pair entry would
    %% raise here, uncaught, instead of being refused. The second pass,
    %% `unsafe_wire_header/4', judges what hackney will write: this list,
    %% the headers `prepare_request/5' added, and the `Cookie',
    %% `Authorization' and multipart `Content-Type'/`Content-Length'
    %% headers hackney makes from the options and the body. A refusal names
    %% the first offending entry the pass that sees it walks, so a caller's
    %% bad header is named before a generated one.
    case unsafe_header(Headers) of
        {'unsafe', Name} -> {'error', {'unsafe_header', Name}};
        'safe' -> request_bounded_headers_checked(
                    Method, Url, Headers, Body, Options, Budget, LifecycleOwner)
    end.

-spec request_bounded_headers_checked(term(), term(), list(), term(), list(),
                                      request_budget(), pid()) ->
          {'ok', reference()} | {'error', term()}.
request_bounded_headers_checked(Method, Url, Headers, Body, Options, Budget,
                                LifecycleOwner) ->
    Parent = self(),
    Token = make_ref(),
    Hooks = test_hooks(Options),
    RequestOptions = [{'async', 'once'}, {'stream_to', Parent}
                      | proplists:delete(
                          'async',
                          proplists:delete(
                            'stream_to', strip_test_options(Options)))],
    case prepare_request(Method, Url, Headers, RequestOptions, Budget) of
        {'ok', FinalHeaders0, FinalOptions0} ->
            FinalHeaders = filter_undefined_headers(FinalHeaders0),
            %% Pin the effective app default to the value validated here.
            AuthOptions = pin_insecure_auth_flag(FinalOptions0),
            %% Checked after `prepare_request/5': the Kazoo headers it adds
            %% are as caller-controlled as the ones handed in, and so is the
            %% `Cookie' header hackney will add from the options.
            case unsafe_wire_header(FinalHeaders, AuthOptions, Body, Url) of
                'safe' ->
                    FinalOptions = request_framing_options(
                                     FinalHeaders, AuthOptions),
                    RequestFun = fun() ->
                                         request_prepared(
                                           Method, Url, FinalHeaders, Body,
                                           FinalOptions)
                                 end,
                    request_bounded_prepared(
                      Parent, LifecycleOwner, Token, RequestFun, Hooks,
                      Budget, request_identity(Method, Url));
                {'unsafe', Name} ->
                    {'error', {'unsafe_header', Name}}
            end;
        {'error', _}=Error ->
            Error
    end.

%% Header bytes must use the raw send path even when the body is chunked.
%% Hackney's perform_all path sends the combined head/body through send_chunk.
-spec request_framing_options(list(), list()) -> list().
request_framing_options(Headers, Options) ->
    case lists:any(fun({Name, _}) ->
                      hackney_bstr:to_lower(Name) =:= <<"transfer-encoding">>
                   end, Headers) of
        'true' -> [{'perform_all', 'false'}
                   | proplists:delete('perform_all', Options)];
        'false' -> Options
    end.

%% Everything hackney will put on the wire that this module can still
%% refuse: the caller's headers with the ones `prepare_request/5' added,
%% then the headers hackney itself makes — `Cookie' from the `cookie'
%% option and `Authorization' from `basic_auth'
%% (`hackney_request:perform/2', `maybe_add_cookies/2'), then the
%% `Content-Type' and `Content-Length' a streamed multipart body dictates
%% (`handle_multipart_body/5'). The order here is the order of the
%% refusals, not hackney's write order: `perform/2' merges its defaults and
%% `hackney_headers_new:to_iolist/1' writes by insertion index.
-spec unsafe_wire_header(list(), list(), term(), term()) ->
          'safe' | {'unsafe', term()}.
unsafe_wire_header(Headers, Options, Body, Url) ->
    case unsafe_header(Headers) of
        'safe' ->
            case unsafe_cookie(Options) of
                'safe' ->
                    case unsafe_effective_auth(Url, Options) of
                        'safe' -> unsafe_body_header(Body);
                        UnsafeAuth -> UnsafeAuth
                    end;
                UnsafeCookie -> UnsafeCookie
            end;
        {'unsafe', _Name}=Unsafe -> Unsafe
    end.

%% The `basic_auth' request option becomes the `Authorization' header
%% inside hackney (`hackney_request:perform/2'), each half through
%% `hackney_bstr:to_binary/1' — a float, a tuple or a string beyond Latin-1
%% would crash the worker after the budget. base64 hides every byte, a line
%% terminator included, so a CR/LF here could not split the request; it is
%% refused all the same, because `renderable_query_term/1' is the one rule
%% every caller-supplied half of this module is judged by and a credential
%% carrying one is a caller defect wherever it came from. In the same
%% branch hackney reads `insecure_basic_auth', whose `case' has clauses for
%% `true' and `false' only — any other value is a `case_clause' in the
%% worker, after the budget — so it is judged here, where it is read.
%% URL credentials also activate the flag, via `unsafe_effective_auth/2'.
-spec unsafe_basic_auth(list()) -> 'safe' | {'unsafe', binary()}.
unsafe_basic_auth(Options) ->
    case proplists:get_value('basic_auth', Options) of
        'undefined' -> 'safe';
        {User, Password} ->
            case renderable_query_term(User)
                andalso renderable_query_term(Password)
                andalso insecure_flag_usable(Options) of
                'true' -> 'safe';
                'false' -> {'unsafe', <<"Authorization">>}
            end;
        _Other -> {'unsafe', <<"Authorization">>}
    end.

-spec insecure_flag_usable(list()) -> boolean().
insecure_flag_usable(Options) ->
    is_boolean(effective_insecure_auth_flag(Options)).

-spec effective_insecure_auth_flag(list()) -> term().
effective_insecure_auth_flag(Options) ->
    proplists:get_value('insecure_basic_auth', Options,
                       hackney_app:get_app_env('insecure_basic_auth', 'true')).

-spec pin_insecure_auth_flag(list()) -> list().
pin_insecure_auth_flag(Options) ->
    [{'insecure_basic_auth', effective_insecure_auth_flag(Options)}
     | proplists:delete('insecure_basic_auth', Options)].

-spec unsafe_effective_auth(term(), list()) -> 'safe' | {'unsafe', binary()}.
unsafe_effective_auth(Url, Options) ->
    case unsafe_basic_auth(Options) of
        'safe' ->
            #hackney_url{user=User} = hackney_url:parse_url(Url),
            case User =:= <<>> orelse insecure_flag_usable(Options) of
                'true' -> 'safe';
                'false' -> {'unsafe', <<"Authorization">>}
            end;
        Unsafe -> Unsafe
    end.

%% @private Exported for `couchbeam_view_stream', not for consumers.
%% What an operator can act on when a guardian logs an unproven cleanup:
%% the method and the URL path. Never the URL itself — Kazoo carries the
%% CouchDB credentials in it — and not the query string, which carries view
%% keys.
-type request_identity() :: #{'method' := term(),
                              'path' := binary() | 'undefined'}.
-spec request_identity(term(), term()) -> request_identity().
request_identity(Method, Url) ->
    Path = try hackney_url:parse_url(Url) of
               #hackney_url{path=P} when is_binary(P) -> P;
               #hackney_url{} -> 'undefined'
           catch
               _Class:_Reason -> 'undefined'
           end,
    #{'method' => Method, 'path' => Path}.

-spec request_bounded_prepared(
        pid(), pid(), reference(), fun(() -> term()), test_hooks(),
        request_budget(), request_identity()) ->
          {'ok', reference()} | {'error', term()}.
request_bounded_prepared(Parent, LifecycleOwner, Token, RequestFun, Hooks,
                         Budget, Identity) ->
    case start_request_guardian(Parent, LifecycleOwner, Token, Budget,
                                Identity) of
        {'ok', GuardianPid} ->
            case before_guardian_resources(
                   hook('guardian_ready', Hooks), GuardianPid, Budget) of
                'ok' ->
                    GuardianPid ! {Token, 'start', RequestFun, Hooks},
                    await_guardian_resources(
                      Token, GuardianPid, Hooks, Budget);
                {'error', _}=Error ->
                    guardian_cancel_result(GuardianPid, Budget, Error)
            end;
        {'error', _}=Error ->
            Error
    end.

-spec start_request_guardian(pid(), pid(), reference(), request_budget(),
                             request_identity()) ->
          {'ok', pid()} | {'error', 'timeout'}.
start_request_guardian(Parent, LifecycleOwner, Token, Budget, Identity) ->
    GuardianPid = spawn(
                    fun() ->
                            request_guardian_init(
                              Parent, LifecycleOwner, Token, Budget,
                              Identity)
                    end),
    %% The announcement is sent by this process, not by the guardian: Erlang
    %% orders the messages of one sender, so the lifecycle owner is guaranteed
    %% to see it before this process's `'DOWN''. An owner that reaches a
    %% terminal message without an announcement therefore knows no guardian
    %% ever existed (`couchbeam_view:await_stream_transport_cleanup/3').
    notify_lifecycle_guardian(LifecycleOwner, Parent, GuardianPid),
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {Token, 'guardian_ready', GuardianPid} ->
                    {'ok', GuardianPid}
            after TimeoutMs ->
                    exit(GuardianPid, 'kill'),
                    flush_request_result(Token),
                    {'error', 'timeout'}
            end;
        _ ->
            exit(GuardianPid, 'kill'),
            flush_request_result(Token),
            {'error', 'timeout'}
    end.

-spec request_guardian_init(pid(), pid(), reference(), request_budget(),
                            request_identity()) ->
          no_return().
request_guardian_init(Parent, LifecycleOwner, Token, Budget, Identity) ->
    OwnerMonitors = monitor_request_owners(Parent, LifecycleOwner),
    Parent ! {Token, 'guardian_ready', self()},
    request_guardian_await_start(
      Parent, LifecycleOwner, Token, Budget, OwnerMonitors, Identity).

-spec monitor_request_owners(pid(), pid()) -> [reference()].
monitor_request_owners(Parent, Parent) ->
    [erlang:monitor('process', Parent)];
monitor_request_owners(Parent, LifecycleOwner) ->
    [erlang:monitor('process', Parent),
     erlang:monitor('process', LifecycleOwner)].

-spec request_guardian_await_start(pid(), pid(), reference(), request_budget(),
                                   [reference()], request_identity()) ->
          no_return().
request_guardian_await_start(Parent, LifecycleOwner, Token, Budget,
                             OwnerMonitors, Identity) ->
    receive
        {Token, 'start', RequestFun, Hooks} ->
            LeasePid = spawn(fun transport_lease/0),
            WorkerHold = hook('worker_hold', Hooks),
            WorkerPid = spawn(
                          fun() ->
                                  Result = RequestFun(),
                                  hold_worker(WorkerHold),
                                  Parent ! {Token, Result},
                                  receive
                                      {Token, 'release'} -> 'ok'
                                  end
                          end),
            %% Both outlive nothing but this guardian: a guardian that dies
            %% (killed, crashed) before its cleanup ran must take the worker
            %% blocked in `hackney:request/5' and the lease that owns the
            %% transport down with it, or the socket survives them all.
            guard_ephemeral_worker(self(), WorkerPid),
            guard_ephemeral_worker(self(), LeasePid),
            WorkerRef = erlang:monitor('process', WorkerPid),
            LeaseRef = erlang:monitor('process', LeasePid),
            notify_upload_worker(hook('upload', Hooks), WorkerPid),
            notify_upload_context(
              hook('upload_context', Hooks), WorkerPid, LeasePid, self(),
              Parent),
            State = #{'parent' => Parent,
                      'lifecycle_owner' => LifecycleOwner,
                      'token' => Token,
                      'budget' => Budget,
                      'owner_monitors' => OwnerMonitors,
                      'worker_pid' => WorkerPid,
                      'worker_ref' => WorkerRef,
                      'lease_pid' => LeasePid,
                      'lease_ref' => LeaseRef,
                      'request_ref' => 'undefined',
                      'request' => Identity,
                      'hooks' => Hooks},
            request_guardian_before_started(
              hook('guardian_started', Hooks), State);
        {'bounded_guardian_cancel', From, CleanupToken, _CleanupBudget} ->
            %% No worker, lease or request exists yet: nothing to clean, but
            %% the lifecycle owner was told this guardian's pid when it was
            %% spawned and waits for its cleanup verdict.
            notify_lifecycle_cleanup(LifecycleOwner, Parent, 'undefined'),
            notify_guardian_cleanup_ack(
              {From, CleanupToken}, 'ok'),
            exit('normal');
        {'DOWN', MonitorRef, 'process', _Pid, _Reason} ->
            case lists:member(MonitorRef, OwnerMonitors) of
                'true' ->
                    %% Same as the cancel branch: nothing to clean, the
                    %% lifecycle owner waits for the verdict.
                    notify_lifecycle_cleanup(
                      LifecycleOwner, Parent, 'undefined'),
                    exit('normal');
                'false' ->
                    request_guardian_await_start(
                      Parent, LifecycleOwner, Token, Budget, OwnerMonitors,
                      Identity)
            end
    end.

-spec request_guardian_before_started('undefined' | pid(), map()) -> no_return().
request_guardian_before_started('undefined', State) ->
    request_guardian_publish_started(State);
request_guardian_before_started(HookPid,
                                #{'parent' := Parent,
                                  'worker_pid' := WorkerPid,
                                  'lease_pid' := LeasePid}=State) ->
    HookPid ! {'bounded_guardian_started_ready', self(), WorkerPid,
               LeasePid, Parent},
    request_guardian_before_started_wait(State).

-spec request_guardian_before_started_wait(map()) -> no_return().
request_guardian_before_started_wait(#{'owner_monitors' := OwnerMonitors}=State) ->
    receive
        {'bounded_guardian_started_continue', GuardianPid}
          when GuardianPid =:= self() ->
            request_guardian_publish_started(State);
        {'bounded_guardian_cancel', From, CleanupToken, CleanupBudget} ->
            request_guardian_cleanup(
              State, {From, CleanupToken}, CleanupBudget, 'true');
        {'DOWN', MonitorRef, 'process', _Pid, _Reason} ->
            case lists:member(MonitorRef, OwnerMonitors) of
                'true' ->
                    request_guardian_cleanup(
                      State, 'undefined', cleanup_deadline(
                                              maps:get('budget', State)),
                      'true');
                'false' ->
                    request_guardian_before_started_wait(
                      guardian_mark_dependent_down(MonitorRef, State))
            end
    end.

-spec request_guardian_publish_started(map()) -> no_return().
request_guardian_publish_started(#{'parent' := Parent,
                                   'token' := Token,
                                   'worker_pid' := WorkerPid,
                                   'lease_pid' := LeasePid}=State) ->
    Parent ! {Token, 'guardian_started', self(), WorkerPid, LeasePid},
    request_guardian_loop(State).

-spec request_guardian_loop(map()) -> no_return().
request_guardian_loop(#{'token' := Token,
                        'owner_monitors' := OwnerMonitors,
                        'hooks' := Hooks}=State) ->
    receive
        {Token, 'known_ref', KnownRef, From} ->
            RefState = State#{'request_ref' => KnownRef},
            request_guardian_before_ref_ack(
              hook('guardian_ref_ack', Hooks), RefState, KnownRef, From);
        {'bounded_guardian_cleanup_complete', KnownRef, From, CleanupToken,
         CleanupBudget} ->
            request_guardian_cleanup(
              State#{'request_ref' => KnownRef}, {From, CleanupToken},
              CleanupBudget, 'true');
        {'bounded_guardian_cancel', From, CleanupToken, CleanupBudget} ->
            request_guardian_cleanup(
              State, {From, CleanupToken}, CleanupBudget, 'true');
        {'DOWN', MonitorRef, 'process', _Pid, _Reason} ->
            case lists:member(MonitorRef, OwnerMonitors) of
                'true' ->
                    request_guardian_cleanup(
                      State, 'undefined', cleanup_deadline(
                                              maps:get('budget', State)),
                      'true');
                'false' ->
                    request_guardian_loop(
                      guardian_mark_dependent_down(MonitorRef, State))
            end
    end.

-spec request_guardian_before_ref_ack('undefined' | pid(), map(),
                                      reference(), pid()) -> no_return().
request_guardian_before_ref_ack('undefined', State, Ref, From) ->
    guardian_publish_ref_ack(State, Ref, From);
request_guardian_before_ref_ack(HookPid, State, Ref, From) ->
    HookPid ! {'bounded_guardian_ref_ack_ready', self(), Ref, From},
    request_guardian_before_ref_ack_wait(State, Ref, From).

-spec request_guardian_before_ref_ack_wait(map(), reference(), pid()) ->
          no_return().
request_guardian_before_ref_ack_wait(
  #{'owner_monitors' := OwnerMonitors}=State, Ref, From) ->
    receive
        {'bounded_guardian_ref_ack_continue', GuardianPid, Ref}
          when GuardianPid =:= self() ->
            guardian_publish_ref_ack(State, Ref, From);
        {'bounded_guardian_cleanup_complete', Ref, CleanupFrom, CleanupToken,
         CleanupBudget} ->
            request_guardian_cleanup(
              State, {CleanupFrom, CleanupToken}, CleanupBudget, 'true');
        {'bounded_guardian_cancel', CleanupFrom, CleanupToken,
         CleanupBudget} ->
            request_guardian_cleanup(
              State, {CleanupFrom, CleanupToken}, CleanupBudget, 'true');
        {'DOWN', MonitorRef, 'process', _Pid, _Reason} ->
            case lists:member(MonitorRef, OwnerMonitors) of
                'true' ->
                    request_guardian_cleanup(
                      State, 'undefined', cleanup_deadline(
                                              maps:get('budget', State)),
                      'true');
                'false' ->
                    request_guardian_before_ref_ack_wait(
                      guardian_mark_dependent_down(MonitorRef, State),
                      Ref, From)
            end
    end.

-spec guardian_publish_ref_ack(map(), reference(), pid()) -> no_return().
guardian_publish_ref_ack(#{'token' := Token, 'hooks' := Hooks}=State,
                         Ref, From) ->
    From ! {Token, 'known_ref_ack', self(), Ref},
    notify_guardian_ref_ack_sent(hook('guardian_ref_ack', Hooks), Ref),
    request_guardian_loop(State).

-spec notify_guardian_ref_ack_sent('undefined' | pid(), reference()) -> 'ok'.
notify_guardian_ref_ack_sent('undefined', _Ref) ->
    'ok';
notify_guardian_ref_ack_sent(HookPid, Ref) ->
    HookPid ! {'bounded_guardian_ref_ack_sent', self(), Ref},
    'ok'.

-spec guardian_mark_dependent_down(reference(), map()) -> map().
guardian_mark_dependent_down(MonitorRef, #{'worker_ref' := MonitorRef}=State) ->
    State#{'worker_ref' => 'down'};
guardian_mark_dependent_down(MonitorRef, #{'lease_ref' := MonitorRef}=State) ->
    State#{'lease_ref' => 'down'};
guardian_mark_dependent_down(_MonitorRef, State) ->
    State.

-spec request_guardian_cleanup(
        map(), 'undefined' | {pid(), reference()}, request_budget(),
        boolean()) -> no_return().
request_guardian_cleanup(#{'parent' := Parent,
                           'lifecycle_owner' := LifecycleOwner,
                           'worker_pid' := WorkerPid,
                           'worker_ref' := WorkerRef,
                           'lease_pid' := LeasePid,
                           'lease_ref' := LeaseRef,
                           'request_ref' := Ref,
                           'request' := Identity,
                           'hooks' := Hooks}=State,
                         AckTarget, CleanupBudget, NotifyLifecycle) ->
    TestHook = hook('guardian_started', Hooks),
    notify_guardian_cleanup_test(TestHook, 'started', Ref),
    hold_guardian_cleanup(hook('guardian_cleanup_hold', Hooks), Ref,
                          CleanupBudget),
    maybe_stop_guardian_worker(WorkerPid, WorkerRef),
    maybe_stop_guardian_lease(LeasePid, LeaseRef),
    maybe_notify_lifecycle_cleanup_started(
      NotifyLifecycle, LifecycleOwner, Parent, CleanupBudget),
    CleanupResult = guardian_confirm_cleanup(
                      WorkerPid, WorkerRef, LeasePid, LeaseRef, Ref,
                      CleanupBudget),
    case CleanupResult of
        'ok' ->
            notify_guardian_cleanup_test(TestHook, 'complete', Ref),
            maybe_log_recovered_cleanup(NotifyLifecycle, Ref, Identity),
            maybe_notify_lifecycle_cleanup(
              NotifyLifecycle, LifecycleOwner, Parent, Ref),
            notify_guardian_cleanup_ack(AckTarget, 'ok'),
            exit('normal');
        {'error', 'timeout'}=Error ->
            notify_guardian_cleanup_test(TestHook, 'failed', Ref),
            maybe_log_unproven_cleanup(
              NotifyLifecycle, Ref, WorkerPid, LeasePid, Identity),
            notify_guardian_cleanup_ack(AckTarget, Error),
            request_guardian_retry(
              State#{'worker_ref' => guardian_down_state(WorkerPid, WorkerRef),
                     'lease_ref' => guardian_down_state(LeasePid, LeaseRef)})
    end.

-spec request_guardian_retry(map()) -> no_return().
request_guardian_retry(#{'budget' := Budget}=State) ->
    request_guardian_cleanup(
      State, 'undefined', cleanup_deadline(Budget), 'false').

%% The first cleanup attempt is the only one that notifies the lifecycle owner;
%% autonomous retries pass `'false''. Logging on that first transition keeps a
%% retrying guardian visible without one line per retry.
-spec maybe_log_unproven_cleanup(boolean(), 'undefined' | reference(),
                                 pid(), pid(), request_identity()) -> 'ok'.
maybe_log_unproven_cleanup('true', Ref, WorkerPid, LeasePid,
                           #{'method' := Method, 'path' := Path}) ->
    logger:warning(
      #{'event' => 'couchbeam_bounded_cleanup_unproven',
        'request_ref' => Ref,
        'worker' => WorkerPid,
        'lease' => LeasePid,
        'guardian' => self(),
        'method' => Method,
        'path' => Path},
      #{'domain' => ['couchbeam', 'bounded_transport']}),
    'ok';
maybe_log_unproven_cleanup('false', _Ref, _WorkerPid, _LeasePid, _Identity) ->
    'ok'.

%% The counterpart of the warning above: a proof that arrived only on a
%% retry (`NotifyLifecycle' is `'false'' for every retry) closes the story the
%% warning opened, so a log reader can tell a recovered guardian from one that
%% is still looping.
-spec maybe_log_recovered_cleanup(boolean(), 'undefined' | reference(),
                                  request_identity()) -> 'ok'.
maybe_log_recovered_cleanup('true', _Ref, _Identity) ->
    'ok';
maybe_log_recovered_cleanup('false', Ref, #{'method' := Method,
                                            'path' := Path}) ->
    %% The same identity keys as the warning, so the two lines join.
    logger:notice(
      #{'event' => 'couchbeam_bounded_cleanup_recovered',
        'request_ref' => Ref,
        'guardian' => self(),
        'method' => Method,
        'path' => Path},
      #{'domain' => ['couchbeam', 'bounded_transport']}),
    'ok'.

-spec maybe_notify_lifecycle_cleanup_started(
        boolean(), pid(), pid(), request_budget()) -> 'ok'.
maybe_notify_lifecycle_cleanup_started(
  'true', LifecycleOwner, Parent, CleanupBudget) ->
    notify_lifecycle_cleanup_started(
      LifecycleOwner, Parent, CleanupBudget);
maybe_notify_lifecycle_cleanup_started(
  'false', _LifecycleOwner, _Parent, _CleanupBudget) ->
    'ok'.

-spec maybe_notify_lifecycle_cleanup(
        boolean(), pid(), pid(), 'undefined' | reference()) -> 'ok'.
maybe_notify_lifecycle_cleanup('true', LifecycleOwner, Parent, Ref) ->
    notify_lifecycle_cleanup(LifecycleOwner, Parent, Ref);
maybe_notify_lifecycle_cleanup('false', _LifecycleOwner, _Parent, _Ref) ->
    'ok'.

-spec notify_guardian_cleanup_test('undefined' | pid(), atom(),
                                   'undefined' | reference()) -> 'ok'.
notify_guardian_cleanup_test('undefined', _Phase, _Ref) ->
    'ok';
notify_guardian_cleanup_test(HookPid, Phase, Ref) ->
    HookPid ! {'bounded_guardian_cleanup', Phase, self(), Ref},
    'ok'.

-spec guardian_confirm_cleanup(pid(), 'down' | reference(), pid(),
                               'down' | reference(),
                               'undefined' | reference(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
guardian_confirm_cleanup(WorkerPid, WorkerRef, LeasePid, LeaseRef, Ref,
                         Budget) ->
    case maybe_await_guardian_dependent(WorkerPid, WorkerRef, Budget) of
        'ok' ->
            case maybe_await_guardian_dependent(LeasePid, LeaseRef, Budget) of
                'ok' -> guardian_confirm_transport_cleanup(
                          WorkerPid, LeasePid, Ref, Budget);
                {'error', 'timeout'}=Error -> Error
            end;
        {'error', 'timeout'}=Error -> Error
    end.

-spec guardian_confirm_transport_cleanup(pid(), pid(),
                                         'undefined' | reference(),
                                         request_budget()) ->
          'ok' | {'error', 'timeout'}.
guardian_confirm_transport_cleanup(WorkerPid, _LeasePid, 'undefined', Budget) ->
    await_owned_transport_cleanup(WorkerPid, Budget);
guardian_confirm_transport_cleanup(_WorkerPid, LeasePid, Ref, Budget) ->
    _ = ownership_barrier(Ref, LeasePid, Budget),
    await_request_cleanup(Ref, Budget).

-spec notify_guardian_cleanup_ack(
        'undefined' | {pid(), reference()}, term()) -> 'ok'.
notify_guardian_cleanup_ack('undefined', _Result) ->
    'ok';
notify_guardian_cleanup_ack({AckPid, CleanupToken}, Result) ->
    AckPid ! {'bounded_guardian_cleanup_ack', self(), CleanupToken, Result},
    'ok'.

-spec maybe_stop_guardian_worker(pid(), 'down' | reference()) -> 'ok'.
maybe_stop_guardian_worker(_WorkerPid, 'down') ->
    'ok';
maybe_stop_guardian_worker(WorkerPid, _WorkerRef) ->
    exit(WorkerPid, 'kill'),
    'ok'.

-spec maybe_stop_guardian_lease(pid(), 'down' | reference()) -> 'ok'.
maybe_stop_guardian_lease(_LeasePid, 'down') ->
    'ok';
maybe_stop_guardian_lease(LeasePid, _LeaseRef) ->
    exit(LeasePid, 'kill'),
    'ok'.

-spec maybe_await_guardian_dependent(pid(), 'down' | reference(),
                                    request_budget()) ->
          'ok' | {'error', 'timeout'}.
maybe_await_guardian_dependent(_Pid, 'down', _Budget) ->
    'ok';
maybe_await_guardian_dependent(Pid, MonitorRef, Budget) ->
    await_guardian_dependent_down(Pid, MonitorRef, Budget).

-spec guardian_down_state(pid(), 'down' | reference()) ->
          'down' | reference().
guardian_down_state(_Pid, 'down') ->
    'down';
guardian_down_state(Pid, MonitorRef) ->
    case is_process_alive(Pid) of
        'true' -> MonitorRef;
        'false' ->
            erlang:demonitor(MonitorRef, ['flush']),
            'down'
    end.

-spec await_guardian_dependent_down(pid(), reference(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
await_guardian_dependent_down(Pid, MonitorRef, Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'DOWN', MonitorRef, 'process', Pid, _Reason} -> 'ok'
            after TimeoutMs ->
                    {'error', 'timeout'}
            end;
        _ ->
            {'error', 'timeout'}
    end.

-spec await_guardian_resources(reference(), pid(), test_hooks(),
                               request_budget()) ->
          {'ok', reference()} | {'error', term()}.
await_guardian_resources(Token, GuardianPid, Hooks, Budget) ->
    GuardianRef = erlang:monitor('process', GuardianPid),
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {Token, 'guardian_started', GuardianPid,
                 WorkerPid, LeasePid} ->
                    erlang:demonitor(GuardianRef, ['flush']),
                    MonitorRef = erlang:monitor('process', WorkerPid),
                    await_bounded_request(
                      Token, WorkerPid, MonitorRef, LeasePid, GuardianPid,
                      Hooks, Budget);
                {'DOWN', GuardianRef, 'process', GuardianPid, Reason} ->
                    %% Before `guardian_started' the caller knows no worker
                    %% and no lease. A `normal' exit means the guardian never
                    %% spawned them (`request_guardian_await_start/5' exits
                    %% normal only on cancel or owner death, before any
                    %% resource); any other exit may leave a worker inside
                    %% `hackney:request/5' to its guard — a cleanup this
                    %% caller cannot prove.
                    flush_request_result(Token),
                    case Reason of
                        'normal' -> {'error', 'request_guardian_down'};
                        _Other -> {'error', 'transport_cleanup_timeout'}
                    end
            after TimeoutMs ->
                    erlang:demonitor(GuardianRef, ['flush']),
                    cancel_and_flush(GuardianPid, Token, Budget,
                                     {'error', 'timeout'})
            end;
        _ ->
            erlang:demonitor(GuardianRef, ['flush']),
            cancel_and_flush(GuardianPid, Token, Budget, {'error', 'timeout'})
    end.

%% Cancel through the guardian, then drain everything the worker or guardian
%% may still have sent to this process under `Token' (including a late
%% `{Token, {'ok', Ref}}' and the `hackney' messages that followed it).
-spec cancel_and_flush(pid(), reference(), request_budget(), term()) -> term().
cancel_and_flush(GuardianPid, Token, Budget, Result) ->
    Final = guardian_cancel_result(GuardianPid, Budget, Result),
    flush_request_result(Token),
    Final.

%% The lease has one job: to stay alive as the tracked `hackney' owner until
%% the guardian kills it. Nothing is ever sent to it.
-spec transport_lease() -> no_return().
transport_lease() ->
    receive after 'infinity' -> 'ok' end.

-spec await_bounded_request(reference(), pid(), reference(), pid(), pid(),
                            test_hooks(), request_budget()) ->
          {'ok', reference()} | {'error', term()}.
await_bounded_request(Token, WorkerPid, MonitorRef, LeasePid, GuardianPid,
                      Hooks, Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {Token, {'ok', Ref}} when is_reference(Ref) ->
                    case guardian_register_ref(
                           GuardianPid, Token, Ref,
                           hook('guardian_ref_ack', Hooks), Budget) of
                        'ok' ->
                            adopt_bounded_request(
                              Token, WorkerPid, MonitorRef, LeasePid,
                              GuardianPid, Ref, hook('handoff', Hooks),
                              Budget);
                        {'error', 'timeout'} ->
                            %% The deadline passed while the ref was being
                            %% registered. `cleanup_failed_handoff/7' asks
                            %% the guardian to clean up, and an `'ok'' from
                            %% it is a proven cleanup: the verdict is the
                            %% deadline, as on the sibling hand-off timeouts.
                            %% `transport_cleanup_timeout' is reserved for a
                            %% cleanup nobody proved.
                            CleanupResult = cleanup_failed_handoff(
                                              Token, WorkerPid, MonitorRef,
                                              LeasePid, GuardianPid, Ref,
                                              Budget),
                            flush_guardian_ref_ack(
                              Token, GuardianPid, Ref),
                            cleanup_result(CleanupResult, {'error', 'timeout'});
                        {'error', {'guardian_down', Reason}} ->
                            %% The guardian left while the ref was being
                            %% registered; hackney may already have streamed
                            %% the first response message to this process.
                            CleanupResult = recover_after_guardian_exit(
                                              Reason, [WorkerPid, LeasePid],
                                              Ref, Budget),
                            erlang:demonitor(MonitorRef, ['flush']),
                            flush_request_result(Token),
                            flush_response_messages(Ref),
                            cleanup_result(
                              CleanupResult,
                              {'error', 'request_guardian_down'})
                    end;
                {Token, {'error', _}=Error} ->
                    erlang:demonitor(MonitorRef, ['flush']),
                    cancel_and_flush(GuardianPid, Token, Budget, Error);
                {Token, Unexpected} ->
                    erlang:demonitor(MonitorRef, ['flush']),
                    cancel_and_flush(
                      GuardianPid, Token, Budget,
                      {'error', {'unexpected_request_result', Unexpected}});
                {'DOWN', MonitorRef, 'process', WorkerPid, Reason} ->
                    cancel_and_flush(
                      GuardianPid, Token, Budget,
                      {'error', {'request_worker_down', Reason}})
            after TimeoutMs ->
                    erlang:demonitor(MonitorRef, ['flush']),
                    cancel_and_flush(GuardianPid, Token, Budget,
                                     {'error', 'timeout'})
            end;
        _ ->
            erlang:demonitor(MonitorRef, ['flush']),
            cancel_and_flush(GuardianPid, Token, Budget, {'error', 'timeout'})
    end.

-spec adopt_bounded_request(reference(), pid(), reference(), pid(), pid(),
                            reference(), term(), request_budget()) ->
          {'ok', reference()} | {'error', term()}.
adopt_bounded_request(Token, WorkerPid, MonitorRef, LeasePid, GuardianPid, Ref,
                      HandoffHook, Budget) ->
    case before_handoff(HandoffHook, Ref, Budget) of
        'ok' -> adopt_bounded_request_now(
                  Token, WorkerPid, MonitorRef, LeasePid, GuardianPid, Ref,
                  Budget);
        {'error', 'timeout'} ->
            CleanupResult = cleanup_failed_handoff(
                              Token, WorkerPid, MonitorRef, LeasePid,
                              GuardianPid, Ref, Budget),
            cleanup_result(CleanupResult, {'error', 'timeout'})
    end.

-spec adopt_bounded_request_now(reference(), pid(), reference(), pid(), pid(),
                                reference(), request_budget()) ->
          {'ok', reference()} | {'error', term()}.
adopt_bounded_request_now(Token, WorkerPid, MonitorRef, LeasePid, GuardianPid,
                          Ref,
                          Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            %% hackney keeps the socket in its async stream process and
            %% tracks the request owner in `hackney_manager' (linked, so an
            %% owner exit closes the transport). Re-point that tracked owner
            %% at the lease before the temporary upload worker exits,
            %% otherwise the worker's exit would tear the request down.
            %% Verified against 1.25.0, which this repository pins: it
            %% links the new owner in
            %% `handle_call({controlling_process,…})'.
            OwnershipResult = try
                                  gen_server:call(
                                    'hackney_manager',
                                    {'controlling_process', Ref, LeasePid},
                                    TimeoutMs)
                              catch
                                  'exit':{'timeout', _} ->
                                      {'error', 'timeout'};
                                  Class:Reason ->
                                      {'error', {Class, Reason}}
                              end,
            case OwnershipResult of
                'ok' ->
                    remember_owned_request(
                      Ref, LeasePid, GuardianPid, Budget),
                    case release_request_worker(Token, WorkerPid, MonitorRef,
                                                Budget) of
                        'ok' ->
                            case budget_status(Budget) of
                                'ok' -> {'ok', Ref};
                                {'error', 'timeout'} ->
                                    close_request_result(
                                      Ref, {'error', 'timeout'})
                            end;
                        {'error', 'transport_cleanup_timeout'}=Error ->
                            close_request_result(Ref, Error)
                    end;
                {'error', 'timeout'} ->
                    CleanupResult = cleanup_failed_handoff(
                                      Token, WorkerPid, MonitorRef, LeasePid,
                                      GuardianPid, Ref, Budget),
                    cleanup_result(CleanupResult, {'error', 'timeout'});
                OwnershipError ->
                    %% Harvested before `cleanup_failed_handoff/7' flushes
                    %% the response messages.
                    Verdict = transport_verdict_before_handoff(
                                Ref, OwnershipError),
                    CleanupResult = cleanup_failed_handoff(
                                      Token, WorkerPid, MonitorRef, LeasePid,
                                      GuardianPid, Ref, Budget),
                    cleanup_result(CleanupResult, Verdict)
            end;
        _ ->
            CleanupResult = cleanup_failed_handoff(
                              Token, WorkerPid, MonitorRef, LeasePid,
                              GuardianPid, Ref, Budget),
            cleanup_result(CleanupResult, {'error', 'timeout'})
    end.

%% `badarg' from `controlling_process' means the manager no longer tracks
%% `Ref': the transport failed on its own before the hand-off. hackney's
%% stream process reports the reason to this process (`stream_to') before it
%% asks the manager to forget the ref (`hackney_stream:stream_loop/4', then
%% `hackney_manager:handle_error/1'), so by the time the manager answers
%% `badarg' the report is already in the mailbox — and it, not the ownership
%% error, is the verdict a caller can act on: the same peer close landing a
%% moment after the hand-off is reported as `closed' too. Any other ownership
%% error keeps its own label.
-spec transport_verdict_before_handoff(reference(), term()) ->
          {'error', term()}.
transport_verdict_before_handoff(Ref, 'badarg') ->
    receive
        {'hackney_response', Ref, {'error', Reason}} -> {'error', Reason}
    after 0 ->
            {'error', {'request_ownership', 'badarg'}}
    end;
transport_verdict_before_handoff(_Ref, OwnershipError) ->
    {'error', {'request_ownership', OwnershipError}}.

-spec cleanup_failed_handoff(reference(), pid(), reference(), pid(), pid(),
                             reference(), request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
cleanup_failed_handoff(Token, WorkerPid, MonitorRef, LeasePid, GuardianPid,
                       Ref, Budget) ->
    erlang:demonitor(MonitorRef, ['flush']),
    CleanupResult = request_guardian_cleanup_complete(
                      GuardianPid, [WorkerPid, LeasePid], Ref, Budget),
    flush_request_result(Token),
    flush_response_messages(Ref),
    CleanupResult.

-spec cleanup_result('ok' | {'error', 'transport_cleanup_timeout'}, term()) ->
          term().
cleanup_result('ok', Result) ->
    Result;
cleanup_result({'error', 'transport_cleanup_timeout'}=Error, _Result) ->
    Error.

%% Re-point the tracked owner of `Ref' at the (already killed) lease. Two
%% cases, one call: before the hand-off the worker is still the tracked owner,
%% and `hackney_manager' answers `controlling_process' by linking the new
%% owner — linking a dead pid delivers `{'EXIT', LeasePid, noproc}' to the
%% manager, whose owner-exit handling closes the socket and drops the
%% `hackney_manager_refs' row. After the hand-off the lease already is the
%% tracked owner, the manager answers `ok' without linking, and the socket is
%% closed by the lease's own `killed' exit; the call is then purely an
%% ordering barrier. Either way the synchronous call orders this guardian
%% behind everything the manager already had queued — except while the manager
%% is `sys'-suspended, where `sys:get_state/2' is answered by the suspend loop
%% and the cleanup falls back to waiting out its deadline.
-spec ownership_barrier(reference(), pid(), request_budget()) -> term().
ownership_barrier(Ref, LeasePid, Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            try gen_server:call(
                  'hackney_manager',
                  {'controlling_process', Ref, LeasePid}, TimeoutMs)
            catch
                'exit':{'timeout', _} -> {'error', 'timeout'};
                Class:Reason -> {'error', {Class, Reason}}
            end;
        _ ->
            {'error', 'timeout'}
    end.

-spec release_request_worker(reference(), pid(), reference(),
                             request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
release_request_worker(Token, WorkerPid, MonitorRef, Budget) ->
    WorkerPid ! {Token, 'release'},
    case require_worker_down(WorkerPid, MonitorRef, Budget) of
        'ok' -> require_owner_cleanup(WorkerPid, Budget);
        {'error', 'transport_cleanup_timeout'}=Error -> Error
    end.

-spec require_worker_down(pid(), reference(), request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
require_worker_down(WorkerPid, MonitorRef, Budget) ->
    case await_worker_down(
           WorkerPid, MonitorRef, cleanup_deadline(Budget)) of
        'ok' -> 'ok';
        {'error', 'timeout'} -> {'error', 'transport_cleanup_timeout'}
    end.

-spec require_owner_cleanup(pid(), request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
require_owner_cleanup(WorkerPid, Budget) ->
    case await_owned_transport_cleanup(
           WorkerPid, cleanup_deadline(Budget)) of
        'ok' -> 'ok';
        {'error', 'timeout'} -> {'error', 'transport_cleanup_timeout'}
    end.

-spec await_worker_down(pid(), reference(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
await_worker_down(WorkerPid, MonitorRef, Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'DOWN', MonitorRef, 'process', WorkerPid, _Reason} -> 'ok'
            after TimeoutMs ->
                    erlang:demonitor(MonitorRef, ['flush']),
                    {'error', 'timeout'}
            end;
        _ ->
            erlang:demonitor(MonitorRef, ['flush']),
            {'error', 'timeout'}
    end.

%% Drain every message tagged with `Token' (2-tuples from the worker,
%% `guardian_ready' / `guardian_started' / `known_ref_ack' from the guardian).
%% A late `{Token, {'ok', Ref}}' means `hackney' may already have streamed
%% the first response message to this process: drain that too.
-spec flush_request_result(reference()) -> 'ok'.
flush_request_result(Token) ->
    receive
        Message when is_tuple(Message), element(1, Message) =:= Token ->
            flush_late_response(Message),
            flush_request_result(Token)
    after 0 ->
            'ok'
    end.

-spec flush_late_response(tuple()) -> 'ok'.
flush_late_response({_Token, {'ok', Ref}}) when is_reference(Ref) ->
    flush_response_messages(Ref);
flush_late_response(_Message) ->
    'ok'.

-spec await_owned_transport_cleanup(pid(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
await_owned_transport_cleanup(OwnerPid, Budget) ->
    await_owned_transport_cleanup(OwnerPid, Budget, 'false').

-spec await_owned_transport_cleanup(pid(), request_budget(), boolean()) ->
          'ok' | {'error', 'timeout'}.
await_owned_transport_cleanup(OwnerPid, Budget, BarrierUsed) ->
    case catch ets:match_object(
                 'hackney_manager_refs', {'_', {OwnerPid, '_', '_'}}) of
        [] ->
            'ok';
        {'EXIT', {'badarg', _}} ->
            'ok';
        [_ | _] ->
            await_cleanup_barrier_or_deadline(
              fun() ->
                      await_owned_transport_cleanup(
                        OwnerPid, Budget, 'true')
              end, Budget, BarrierUsed)
    end.

-spec await_request_cleanup(reference(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
await_request_cleanup(Ref, Budget) ->
    await_request_cleanup(Ref, Budget, 'false').

-spec await_request_cleanup(reference(), request_budget(), boolean()) ->
          'ok' | {'error', 'timeout'}.
await_request_cleanup(Ref, Budget, BarrierUsed) ->
    case catch ets:lookup('hackney_manager_refs', Ref) of
        [] -> 'ok';
        {'EXIT', {'badarg', _}} -> 'ok';
        [_] ->
            await_cleanup_barrier_or_deadline(
              fun() -> await_request_cleanup(Ref, Budget, 'true') end,
              Budget, BarrierUsed)
    end.

-spec await_cleanup_barrier_or_deadline(
        fun(() -> 'ok' | {'error', 'timeout'}), request_budget(), boolean()) ->
          'ok' | {'error', 'timeout'}.
await_cleanup_barrier_or_deadline(ContinueFun, Budget, 'false') ->
    case manager_cleanup_barrier(Budget) of
        'ok' -> ContinueFun();
        {'error', 'timeout'}=Error -> Error
    end;
await_cleanup_barrier_or_deadline(ContinueFun, Budget, 'true') ->
    case remaining_timeout(Budget) of
        Remaining when Remaining > 0 ->
            receive
            after Remaining ->
                    ContinueFun()
            end;
        _ ->
            {'error', 'timeout'}
    end.

-spec manager_cleanup_barrier(request_budget()) ->
          'ok' | {'error', 'timeout'}.
manager_cleanup_barrier(Budget) ->
    case remaining_timeout(Budget) of
        Remaining when Remaining > 0 ->
            try sys:get_state('hackney_manager', Remaining) of
                _State -> 'ok'
            catch
                'exit':{'timeout', _} -> {'error', 'timeout'};
                'exit':_Reason -> {'error', 'timeout'}
            end;
        _ ->
            {'error', 'timeout'}
    end.

-spec cleanup_deadline(request_budget()) -> request_budget().
cleanup_deadline(#{'timeout_ms' := TimeoutMs}=Budget) ->
    Budget#{'deadline_ms' =>
                erlang:monotonic_time('millisecond') + TimeoutMs}.

-spec remember_owned_request(reference(), pid(), pid(), request_budget()) ->
          term().
remember_owned_request(Ref, LeasePid, GuardianPid, Budget) ->
    put({'bounded_request_owner', Ref},
        {LeasePid, GuardianPid, Budget}).

-spec flush_response_messages(reference()) -> 'ok'.
flush_response_messages(Ref) ->
    receive
        {'hackney_response', Ref, _} -> flush_response_messages(Ref)
    after 0 ->
            'ok'
    end.

%% The guardian may finish an autonomous cleanup — the lifecycle owner died —
%% while this process waits for its acknowledgement. A monitor for the length
%% of the wait turns that exit into a verdict instead of a stall until the
%% deadline.
-spec guardian_register_ref(pid(), reference(), reference(),
                            'undefined' | pid(), request_budget()) ->
          'ok' | {'error', 'timeout'} | {'error', {'guardian_down', term()}}.
guardian_register_ref(GuardianPid, Token, Ref, RefAckHook, Budget) ->
    GuardianMonitor = erlang:monitor('process', GuardianPid),
    GuardianPid ! {Token, 'known_ref', Ref, self()},
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {Token, 'known_ref_ack', GuardianPid, Ref} ->
                    erlang:demonitor(GuardianMonitor, ['flush']),
                    'ok';
                {'DOWN', GuardianMonitor, 'process', GuardianPid, Reason} ->
                    {'error', {'guardian_down', Reason}}
            after TimeoutMs ->
                    erlang:demonitor(GuardianMonitor, ['flush']),
                    after_ref_ack_timeout(
                      RefAckHook, GuardianPid, Ref, Budget),
                    {'error', 'timeout'}
            end;
        _ ->
            erlang:demonitor(GuardianMonitor, ['flush']),
            after_ref_ack_timeout(RefAckHook, GuardianPid, Ref, Budget),
            {'error', 'timeout'}
    end.

%% A guardian that exited `normal' proved its cleanup before leaving (see
%% `guardian_cleanup_ack_relay/4'). After any other exit the manager table is
%% the only witness, and the proof primitive the guardian itself uses answers
%% it — but only once the guardian's dependents are known to be dead: their
%% guards kill them asynchronously, and a table check that raced the guards
%% would find the row still owned by a live worker and sleep out the cleanup
%% budget. So they are stopped here first and their `'DOWN'' awaited.
%% `'noproc'' — the guardian was gone before it could be watched — carries no
%% reason and is treated like any other unproven exit.
-spec recover_after_guardian_exit(term(), [pid()], reference(),
                                  request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
recover_after_guardian_exit('normal', _Dependents, _Ref, _Budget) ->
    'ok';
recover_after_guardian_exit(_Reason, Dependents, Ref, Budget) ->
    CleanupBudget = cleanup_deadline(Budget),
    case stop_and_await(Dependents, CleanupBudget) of
        'ok' ->
            case await_request_cleanup(Ref, CleanupBudget) of
                'ok' -> 'ok';
                {'error', 'timeout'} -> {'error', 'transport_cleanup_timeout'}
            end;
        {'error', 'timeout'} ->
            {'error', 'transport_cleanup_timeout'}
    end.

%% Kill each pid and wait until it is known dead, within `Budget'. A pid that
%% is already gone answers the monitor with `noproc' at once.
-spec stop_and_await([pid()], request_budget()) -> 'ok' | {'error', 'timeout'}.
stop_and_await([], _Budget) ->
    'ok';
stop_and_await([Pid | Rest], Budget) ->
    MonitorRef = erlang:monitor('process', Pid),
    exit(Pid, 'kill'),
    case await_worker_down(Pid, MonitorRef, Budget) of
        'ok' -> stop_and_await(Rest, Budget);
        {'error', 'timeout'}=Error -> Error
    end.

-spec flush_guardian_ref_ack(reference(), pid(), reference()) -> 'ok'.
flush_guardian_ref_ack(Token, GuardianPid, Ref) ->
    receive
        {Token, 'known_ref_ack', GuardianPid, Ref} ->
            flush_guardian_ref_ack(Token, GuardianPid, Ref)
    after 0 ->
            'ok'
    end.

%% The cancel path has no request reference to verify against the manager
%% table, so a guardian that died instead of answering
%% (`{'guardian_down', _}') stays an unproven cleanup.
-spec guardian_cancel_result(pid(), request_budget(), term()) -> term().
guardian_cancel_result(GuardianPid, Budget, Result) ->
    CleanupBudget = cleanup_deadline(Budget),
    case request_guardian_cleanup_ack(
           GuardianPid, 'cancel', 'undefined', CleanupBudget) of
        'ok' -> Result;
        {'error', _Unproven} -> {'error', 'transport_cleanup_timeout'}
    end.

%% `Dependents' are the guardian's worker and lease as far as this caller
%% knows them; they matter only when the guardian turns out to be gone.
-spec request_guardian_cleanup_complete(pid(), [pid()], reference(),
                                        request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
request_guardian_cleanup_complete(GuardianPid, Dependents, Ref, Budget) ->
    CleanupBudget = cleanup_deadline(Budget),
    case request_guardian_cleanup_ack(
           GuardianPid, 'complete', Ref, CleanupBudget) of
        'ok' -> 'ok';
        {'error', {'guardian_down', Reason}} ->
            %% Gone before the relay could watch it (`noproc') or died while
            %% the request was in flight (`killed', a crash): stop what it
            %% would have stopped, then ask the table it would have asked.
            recover_after_guardian_exit(Reason, Dependents, Ref, Budget);
        {'error', 'timeout'} -> {'error', 'transport_cleanup_timeout'}
    end.

-spec request_guardian_cleanup_ack(
        pid(), 'cancel' | 'complete', 'undefined' | reference(),
        request_budget()) ->
          'ok' | {'error', 'timeout' | {'guardian_down', term()}}.
request_guardian_cleanup_ack(GuardianPid, Kind, Ref, CleanupBudget) ->
    Parent = self(),
    CleanupToken = make_ref(),
    AckBudget = cleanup_ack_deadline(CleanupBudget),
    {RelayPid, RelayMonitor} = spawn_monitor(
                               fun() ->
                                       guardian_cleanup_ack_relay(
                                         Parent, GuardianPid, CleanupToken,
                                         AckBudget)
                               end),
    send_guardian_cleanup_request(
      Kind, GuardianPid, Ref, RelayPid, CleanupToken, CleanupBudget),
    await_guardian_cleanup_relay(
      RelayPid, RelayMonitor, CleanupToken, AckBudget).

-spec send_guardian_cleanup_request(
        'cancel' | 'complete', pid(), 'undefined' | reference(), pid(),
        reference(), request_budget()) -> 'ok'.
send_guardian_cleanup_request('cancel', GuardianPid, _Ref, RelayPid,
                              CleanupToken, CleanupBudget) ->
    GuardianPid ! {'bounded_guardian_cancel', RelayPid, CleanupToken,
                   CleanupBudget},
    'ok';
send_guardian_cleanup_request('complete', GuardianPid, Ref, RelayPid,
                              CleanupToken, CleanupBudget) ->
    GuardianPid ! {'bounded_guardian_cleanup_complete', Ref, RelayPid,
                   CleanupToken, CleanupBudget},
    'ok'.

%% A guardian that leaves while the relay watches it may have done the
%% cleanup on its own: it exits `normal' only from `request_guardian_cleanup/4'
%% after `guardian_confirm_cleanup/6' answered `ok', or from
%% `request_guardian_await_start/5' before any worker, lease or request
%% existed, while an unproven cleanup keeps it alive in
%% `request_guardian_retry/1'. So a `normal' `'DOWN'' is the proof itself —
%% the case whenever the lifecycle owner died first and the guardian cleaned
%% up before the stream asked it to. Every other `'DOWN'' — `noproc' for a
%% guardian gone before the monitor was placed, `killed' or a crash reason
%% for one that died while the request was in flight — is reported with its
%% reason as `{'guardian_down', Reason}': the guardian proved nothing, but
%% its dependents die with it and the manager table can still be asked
%% (`recover_after_guardian_exit/4'). Only the acknowledgement deadline
%% itself is a `timeout'.
-spec guardian_cleanup_ack_relay(
        pid(), pid(), reference(), request_budget()) -> 'ok'.
guardian_cleanup_ack_relay(Parent, GuardianPid, CleanupToken, Budget) ->
    GuardianMonitor = erlang:monitor('process', GuardianPid),
    Result = case remaining_timeout(Budget) of
                 TimeoutMs when TimeoutMs > 0 ->
                     receive
                         {'bounded_guardian_cleanup_ack', GuardianPid,
                          CleanupToken, 'ok'} -> 'ok';
                         {'bounded_guardian_cleanup_ack', GuardianPid,
                          CleanupToken, {'error', 'timeout'}} ->
                             {'error', 'timeout'};
                         {'DOWN', GuardianMonitor, 'process', GuardianPid,
                          'normal'} ->
                             'ok';
                         {'DOWN', GuardianMonitor, 'process', GuardianPid,
                          Reason} ->
                             {'error', {'guardian_down', Reason}}
                     after TimeoutMs ->
                             {'error', 'timeout'}
                     end;
                 _ ->
                     {'error', 'timeout'}
             end,
    erlang:demonitor(GuardianMonitor, ['flush']),
    Parent ! {CleanupToken, 'guardian_cleanup_result', Result},
    'ok'.

-spec await_guardian_cleanup_relay(
        pid(), reference(), reference(), request_budget()) ->
          'ok' | {'error', 'timeout' | {'guardian_down', term()}}.
await_guardian_cleanup_relay(RelayPid, RelayMonitor, CleanupToken, Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {CleanupToken, 'guardian_cleanup_result', Result} ->
                    await_cleanup_relay_down(RelayPid, RelayMonitor),
                    flush_cleanup_relay_result(CleanupToken),
                    Result;
                {'DOWN', RelayMonitor, 'process', RelayPid, _Reason} ->
                    flush_cleanup_relay_result(CleanupToken),
                    {'error', 'timeout'}
            after TimeoutMs ->
                    exit(RelayPid, 'kill'),
                    await_cleanup_relay_down(RelayPid, RelayMonitor),
                    flush_cleanup_relay_result(CleanupToken),
                    {'error', 'timeout'}
            end;
        _ ->
            exit(RelayPid, 'kill'),
            await_cleanup_relay_down(RelayPid, RelayMonitor),
            flush_cleanup_relay_result(CleanupToken),
            {'error', 'timeout'}
    end.

%% Unconditional for the same reason as in `decode_with_watchdog/3': the
%% relay either finished on its own or was just killed, so `DOWN' is certain.
-spec await_cleanup_relay_down(pid(), reference()) -> 'ok'.
await_cleanup_relay_down(RelayPid, RelayMonitor) ->
    receive
        {'DOWN', RelayMonitor, 'process', RelayPid, _Reason} -> 'ok'
    end.

-spec flush_cleanup_relay_result(reference()) -> 'ok'.
flush_cleanup_relay_result(CleanupToken) ->
    receive
        {CleanupToken, 'guardian_cleanup_result', _Result} ->
            flush_cleanup_relay_result(CleanupToken)
    after 0 ->
            'ok'
    end.

-spec cleanup_ack_deadline(request_budget()) -> request_budget().
cleanup_ack_deadline(#{'deadline_ms' := CleanupDeadline,
                       'timeout_ms' := TimeoutMs}=Budget) ->
    Budget#{'deadline_ms' => CleanupDeadline + TimeoutMs}.

-spec notify_lifecycle_guardian(pid(), pid(), pid()) -> 'ok'.
notify_lifecycle_guardian(Parent, Parent, _GuardianPid) ->
    'ok';
notify_lifecycle_guardian(LifecycleOwner, Parent, GuardianPid) ->
    LifecycleOwner ! {'bounded_transport_guardian', Parent, GuardianPid},
    'ok'.

-spec notify_lifecycle_cleanup(pid(), pid(), 'undefined' | reference()) ->
          'ok'.
notify_lifecycle_cleanup(Parent, Parent, _Ref) ->
    'ok';
notify_lifecycle_cleanup(LifecycleOwner, Parent, Ref) ->
    LifecycleOwner ! {'bounded_transport_cleanup', Parent, Ref},
    'ok'.

-spec notify_lifecycle_cleanup_started(pid(), pid(), request_budget()) -> 'ok'.
notify_lifecycle_cleanup_started(Parent, Parent, _Budget) ->
    'ok';
notify_lifecycle_cleanup_started(LifecycleOwner, Parent, Budget) ->
    LifecycleOwner ! {'bounded_transport_cleanup_started', Parent, Budget},
    'ok'.

%% Test seams travel as one map keyed by role; production builds carry an
%% empty map and every `hook/2' lookup answers `'undefined''.
-type test_hooks() :: #{atom() => pid()}.

-spec hook(atom(), test_hooks()) -> 'undefined' | pid().
hook(Key, Hooks) ->
    maps:get(Key, Hooks, 'undefined').

%% TEST seam: hold the upload worker between its finished request and the
%% report to the stream, so a scenario can act while the transport exists but
%% the ref has not been handed over yet.
-spec hold_worker('undefined' | pid()) -> 'ok'.
hold_worker('undefined') ->
    'ok';
hold_worker(HookPid) ->
    HookPid ! {'bounded_worker_held', self()},
    receive
        {'bounded_worker_continue', WorkerPid} when WorkerPid =:= self() ->
            'ok'
    end.

-ifdef(TEST).
-spec test_hook_options() -> [{atom(), atom()}].
test_hook_options() ->
    [{'handoff', 'bounded_handoff_test_hook'},
     {'upload', 'bounded_upload_test_hook'},
     {'upload_context', 'bounded_upload_context_test_hook'},
     {'guardian_ready', 'bounded_guardian_ready_test_hook'},
     {'guardian_started', 'bounded_guardian_started_test_hook'},
     {'guardian_ref_ack', 'bounded_guardian_ref_ack_test_hook'},
     {'guardian_cleanup_hold', 'bounded_guardian_cleanup_hold_test_hook'},
     {'worker_hold', 'bounded_worker_hold_test_hook'}].

-spec test_hooks(list()) -> test_hooks().
test_hooks(Options) ->
    maps:from_list(
      [{Key, Pid} || {Key, Option} <- test_hook_options(),
                     Pid <- [proplists:get_value(Option, Options)],
                     is_pid(Pid)]).

-spec encode_test_delay(list()) -> non_neg_integer().
encode_test_delay(Options) ->
    case proplists:get_value('bounded_encode_test_delay_ms', Options, 0) of
        Delay when is_integer(Delay), Delay >= 0 -> Delay;
        _ -> 0
    end.

-spec strip_test_options(list()) -> list().
strip_test_options(Options) ->
    lists:foldl(fun({_Key, Option}, Acc) -> proplists:delete(Option, Acc) end,
                proplists:delete('bounded_encode_test_delay_ms', Options),
                test_hook_options()).
-else.
-spec test_hooks(list()) -> test_hooks().
test_hooks(_Options) ->
    #{}.

-spec encode_test_delay(list()) -> 0.
encode_test_delay(_Options) ->
    0.

-spec strip_test_options(list()) -> list().
strip_test_options(Options) ->
    Options.
-endif.

-spec notify_upload_worker('undefined' | pid(), pid()) -> 'ok'.
notify_upload_worker('undefined', _WorkerPid) ->
    'ok';
notify_upload_worker(HookPid, WorkerPid) ->
    HookPid ! {'bounded_upload_worker', WorkerPid},
    'ok'.

-ifdef(TEST).
-spec notify_upload_context('undefined' | pid(), pid(), pid(), pid(), pid()) ->
          'ok'.
notify_upload_context('undefined', _WorkerPid, _LeasePid, _GuardianPid,
                      _Parent) ->
    'ok';
notify_upload_context(HookPid, WorkerPid, LeasePid, GuardianPid, Parent) ->
    HookPid ! {'bounded_upload_context', WorkerPid, LeasePid,
               GuardianPid, Parent},
    'ok'.
-else.
-spec notify_upload_context(term(), pid(), pid(), pid(), pid()) -> 'ok'.
notify_upload_context(_Hook, _WorkerPid, _LeasePid, _GuardianPid, _Parent) ->
    'ok'.
-endif.

-spec before_guardian_resources('undefined' | pid(), pid(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
before_guardian_resources('undefined', _GuardianPid, _Budget) ->
    'ok';
before_guardian_resources(HookPid, GuardianPid, Budget) ->
    HookPid ! {'bounded_guardian_ready', GuardianPid, self()},
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'bounded_guardian_continue', GuardianPid} -> 'ok'
            after TimeoutMs ->
                    {'error', 'timeout'}
            end;
        _ ->
            {'error', 'timeout'}
    end.

-spec maybe_test_delay(non_neg_integer()) -> 'ok'.
maybe_test_delay(0) ->
    'ok';
maybe_test_delay(Delay) ->
    timer:sleep(Delay).

%% The document-side decoder has no option list to carry a test delay, so the
%% test process plants it in its own dictionary before the call; it is read in
%% the calling process and handed to the decoder at spawn time.
-ifdef(TEST).
-spec caller_decode_test_delay() -> non_neg_integer().
caller_decode_test_delay() ->
    case erlang:get('bounded_decode_test_delay_ms') of
        Delay when is_integer(Delay), Delay >= 0 -> Delay;
        _ -> 0
    end.
-else.
-spec caller_decode_test_delay() -> 0.
caller_decode_test_delay() ->
    0.
-endif.

-spec before_handoff('undefined' | pid(), reference(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
before_handoff('undefined', _Ref, _Budget) ->
    'ok';
before_handoff(HookPid, Ref, Budget) ->
    HookPid ! {'bounded_handoff_ready', Ref, self()},
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'bounded_handoff_continue', Ref} -> 'ok'
            after TimeoutMs ->
                    {'error', 'timeout'}
            end;
        _ ->
            {'error', 'timeout'}
    end.

-ifdef(TEST).
-spec after_ref_ack_timeout('undefined' | pid(), pid(), reference(),
                            request_budget()) -> 'ok'.
after_ref_ack_timeout('undefined', _GuardianPid, _Ref, _Budget) ->
    'ok';
after_ref_ack_timeout(HookPid, GuardianPid, Ref, Budget) ->
    HookPid ! {'bounded_guardian_ref_ack_timeout', GuardianPid, Ref, self()},
    HookBudget = cleanup_deadline(Budget),
    case remaining_timeout(HookBudget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'bounded_guardian_ref_ack_timeout_continue', GuardianPid,
                 Ref} -> 'ok'
            after TimeoutMs ->
                    'ok'
            end;
        _ ->
            'ok'
    end.
-else.
-spec after_ref_ack_timeout('undefined', pid(), reference(),
                            request_budget()) -> 'ok'.
after_ref_ack_timeout('undefined', _GuardianPid, _Ref, _Budget) ->
    'ok'.
-endif.

%% TEST seam: hold the guardian at the start of its cleanup, after the
%% cleanup request reached it and before it stops anything, so a scenario can
%% kill it while the request is in flight and the relay already watches it.
%% Bounded by the cleanup budget: a scenario that never continues does not
%% keep a guardian alive past its own deadline.
-ifdef(TEST).
-spec hold_guardian_cleanup('undefined' | pid(), 'undefined' | reference(),
                            request_budget()) -> 'ok'.
hold_guardian_cleanup('undefined', _Ref, _Budget) ->
    'ok';
hold_guardian_cleanup(HookPid, Ref, Budget) ->
    HookPid ! {'bounded_guardian_cleanup_held', self(), Ref},
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'bounded_guardian_cleanup_continue', GuardianPid}
                  when GuardianPid =:= self() -> 'ok'
            after TimeoutMs ->
                    'ok'
            end;
        _ ->
            'ok'
    end.
-else.
-spec hold_guardian_cleanup('undefined', 'undefined' | reference(),
                            request_budget()) -> 'ok'.
hold_guardian_cleanup('undefined', _Ref, _Budget) ->
    'ok'.
-endif.

-spec budget_status(request_budget()) -> 'ok' | {'error', 'timeout'}.
budget_status(Budget) ->
    case remaining_timeout(Budget) of
        Remaining when Remaining > 0 -> 'ok';
        _ -> {'error', 'timeout'}
    end.

-spec bounded_response_status(reference(), request_budget()) -> term().
bounded_response_status(Ref, Budget) ->
    bounded_response_receive(
      Ref, Budget,
      fun({'status', Status, _Reason}) ->
              case hackney:stream_next(Ref) of
                  'ok' -> bounded_response_headers(Ref, Status, Budget);
                  {'error', Reason} ->
                      close_request_result(Ref, {'error', Reason})
              end;
         ({'error', Reason}) ->
              close_request_result(Ref, {'error', Reason});
         (Unexpected) ->
              close_request_result(
                Ref, {'error', {'unexpected_response_message', Unexpected}})
      end).

-spec bounded_response_headers(reference(), integer(), request_budget()) ->
          term().
bounded_response_headers(Ref, Status, Budget) ->
    bounded_response_receive(
      Ref, Budget,
      fun({'headers', Headers}) -> {'ok', Status, Headers, Ref};
         ({'error', Reason}) ->
              close_request_result(Ref, {'error', Reason});
         (Unexpected) ->
              close_request_result(
                Ref, {'error', {'unexpected_response_message', Unexpected}})
      end).

-spec bounded_response_receive(reference(), request_budget(), fun((term()) -> term())) ->
          term().
bounded_response_receive(Ref, Budget, Handler) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'hackney_response', Ref, Message} -> Handler(Message)
            after TimeoutMs ->
                    close_request_result(Ref, {'error', 'timeout'})
            end;
        _ ->
            close_request_result(Ref, {'error', 'timeout'})
    end.

%% The only place a budget turns into transport timeouts, and it takes the
%% budget as an argument. An entry of the same name inside `Options' is inert
%% caller data and travels no further than `hackney', which ignores it.
-spec request_options(list(), 'undefined' | request_budget()) ->
          {'ok', list()} | {'error', 'timeout' | 'invalid_request_budget'}.
request_options(Options, 'undefined') ->
    {'ok', Options};
request_options(Options, #{'deadline_ms' := _, 'timeout_ms' := _,
                           'max_response_bytes' := _}=Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            %% Strictly above the remaining budget, deliberately. The bounded
            %% reader enforces the deadline itself with `receive … after';
            %% a hackney timer allowed to fire at the same instant would
            %% report that instant as `{closed, timeout}' or
            %% `connect_timeout' instead of the budget's `timeout'. hackney's
            %% timers stay a backstop for a transport the reader has already
            %% given up on, and the caller's own `connect_timeout' /
            %% `recv_timeout' / `checkout_timeout' are replaced, not merged:
            %% the budget is the single source of the deadline. The third
            %% timer is the pool's: `hackney_pool:do_checkout/5' falls back
            %% to `connect_timeout' only when `checkout_timeout' is unset, so
            %% a caller's shorter value would otherwise win the race with
            %% `{error, checkout_timeout}'. The pool matters here even though a
            %% bounded call never gives its connection back to it -- every
            %% terminal path closes the transport through `close_request/1' --
            %% because `hackney_pool:do_checkout/5' queues the checkout once
            %% `max_connections' is reached and answers from that queue: without
            %% the replacement the wait for a free slot would be the caller's,
            %% not the budget's.
            Backstop = 2 * TimeoutMs,
            {'ok', [{'connect_timeout', Backstop},
                    {'recv_timeout', Backstop},
                    {'checkout_timeout', Backstop}
                    | proplists:delete(
                        'checkout_timeout',
                        proplists:delete('connect_timeout',
                                         proplists:delete('recv_timeout',
                                                          Options)))]};
        _ ->
            {'error', 'timeout'}
    end;
request_options(_Options, _Budget) ->
    {'error', 'invalid_request_budget'}.

-spec remaining_timeout(request_budget()) -> integer().
remaining_timeout(#{'deadline_ms' := DeadlineMs}) ->
    DeadlineMs - erlang:monotonic_time('millisecond').

-spec close_request(reference()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
close_request(Ref) ->
    case erase({'bounded_request_owner', Ref}) of
        {LeasePid, GuardianPid, Budget} ->
            exit(LeasePid, 'kill'),
            CleanupResult = request_guardian_cleanup_complete(
                              GuardianPid, [LeasePid], Ref, Budget),
            flush_response_messages(Ref),
            CleanupResult;
        'undefined' ->
            close_unowned_request(Ref)
    end.

-spec close_request_result(reference(), term()) -> term().
close_request_result(Ref, Result) ->
    cleanup_result(close_request(Ref), Result).

-spec close_unowned_request(reference()) -> 'ok'.
close_unowned_request(Ref) ->
    case catch hackney:cancel_request(Ref) of
        {'ok', {Transport, Socket, _Buffer, _ResponseState}}
          when Socket =/= 'nil' ->
            _ = catch Transport:close(Socket);
        _ ->
            _ = catch hackney:close(Ref)
    end,
    'ok'.

%% `bounded_request/6' yields `{'ok', Status, Headers, Ref}' or an error
%% tuple only: the response is always streamed, never a bodiless 3-tuple.
-spec db_resp_bounded({'ok', integer(), list(), reference()} |
                      {'error', term()},
                      [integer()], request_budget()) ->
          {'ok', integer(), list(), reference()} | {'error', term()}.
db_resp_bounded({'ok', Status, _Headers, Ref}, _Expect, _Budget)
  when Status =:= 401; Status =:= 403; Status =:= 404;
       Status =:= 409; Status =:= 412 ->
    cleanup_result(cancel_request(Ref), db_resp_bounded_status(Status));
%% Official parity: `db_resp/2' answers an empty `Expect' with the response
%% itself, whatever the status, and does so after the status mapping above --
%% a 404 with an empty `Expect' is still `not_found' there and here. The
%% bounded twin hands back the same live `Ref' the matching-status clause
%% below does, so the caller reads the body with `bounded_json_body/2' under
%% the same budget. Every door of this module passes a non-empty list; the
%% clause exists because `db_request_bounded/7' is exported, and a caller of
%% the primitive must not see a 200 turn into `bad_response'.
db_resp_bounded({'ok', _Status, _Headers, _Ref}=Resp, [], _Budget) ->
    Resp;
db_resp_bounded({'ok', Status, Headers, Ref}=Resp, Expect, Budget) ->
    case lists:member(Status, Expect) of
        'true' -> Resp;
        'false' ->
            case bounded_binary_body(Ref, Budget) of
                {'ok', ErrorBody, _Bytes} ->
                    {'error', {'bad_response', {Status, Headers, ErrorBody}}};
                {'error', _}=Error ->
                    Error
            end
    end;
db_resp_bounded(Error, _Expect, _Budget) ->
    Error.

-spec db_resp_bounded_status(integer()) -> {'error', atom()}.
db_resp_bounded_status(401) -> {'error', 'unauthenticated'};
db_resp_bounded_status(403) -> {'error', 'forbidden'};
db_resp_bounded_status(404) -> {'error', 'not_found'};
db_resp_bounded_status(409) -> {'error', 'conflict'};
db_resp_bounded_status(412) -> {'error', 'precondition_failed'}.

make_headers(Method, Url, Headers, Options) ->
   {Headers1, Options1} = make_headers(Headers, Options),
   {Headers2, Options2} = maybe_oauth_header(Method, Url, Headers1, Options1),
   maybe_proxyauth_header(Headers2, Options2).

get_kz_application() ->
    case erlang:get(kz_application) of
        undefined -> application:get_application();
        App -> {ok, App}
    end.

kz_application() ->
    case get_kz_application() of
        {ok, App} -> App;
        _Other -> undefined
    end.

get_kz_log_id() ->
    case kz_log:get_callid() of
        <<"00000000000">> -> undefined;
        Other -> Other
    end.

kz_log_id() ->
    get_kz_log_id().

make_kazoo_headers(Headers) ->
    HeaderFuns = [{<<"X-Kazoo-Application">>, fun kz_application/0}
                 ,{<<"X-Kazoo-Log-ID">>, fun kz_log_id/0}
                 ],
    lists:foldl(fun make_kazoo_header/2, Headers, HeaderFuns).

make_kazoo_header({Header, Fun}, Headers) ->
    case Fun() of
        undefined -> Headers;
        Value -> [{Header, kz_term:to_binary(Value)} | Headers]
    end.

maybe_oauth_header(Method, Url, Headers, Options) ->
    case couchbeam_util:get_value(oauth, Options) of
        undefined ->
            {Headers, Options};
        OauthProps ->
            Hdr = couchbeam_util:oauth_header(Url, Method, OauthProps),
            {[Hdr|Headers], proplists:delete(oauth, Options)}
    end.

maybe_proxyauth_header(Headers, Options) ->
  case couchbeam_util:get_value(proxyauth, Options) of
    undefined ->
      {Headers, Options};
    ProxyauthProps ->
      {lists:append([ProxyauthProps,Headers]), proplists:delete(proxyauth, Options)}
  end.

db_resp({ok, Ref}=Resp, _Expect) when is_reference(Ref) ->
    Resp;
db_resp({ok, 401, _}, _Expect) ->
    {error, unauthenticated};
db_resp({ok, 403, _}, _Expect) ->
    {error, forbidden};
db_resp({ok, 404, _}, _Expect) ->
    {error, not_found};
db_resp({ok, 409, _}, _Expect) ->
    {error, conflict};
db_resp({ok, 412, _}, _Expect) ->
    {error, precondition_failed};
db_resp({ok, _, _}=Resp, []) ->
    Resp;
db_resp({ok, Status, Headers}=Resp, Expect) ->
    case lists:member(Status, Expect) of
        true -> Resp;
        false ->
            {error, {bad_response, {Status, Headers, <<>>}}}
    end;
db_resp({ok, 401, _, Ref}, _Expect) ->
    hackney:skip_body(Ref),
    {error, unauthenticated};
db_resp({ok, 403, _, Ref}, _Expect) ->
    hackney:skip_body(Ref),
    {error, forbidden};
db_resp({ok, 404, _, Ref}, _Expect) ->
    hackney:skip_body(Ref),
    {error, not_found};
db_resp({ok, 409, _, Ref}, _Expect) ->
    hackney:skip_body(Ref),
    {error, conflict};
db_resp({ok, 412, _, Ref}, _Expect) ->
    hackney:skip_body(Ref),
    {error, precondition_failed};
db_resp({ok, _, _, _}=Resp, []) ->
    Resp;
db_resp({ok, Status, Headers, Ref}=Resp, Expect) ->
    case lists:member(Status, Expect) of
        true -> Resp;
        false -> {error, {bad_response, {Status, Headers, db_resp_body(Ref)}}}
    end;
db_resp(Error, _Expect) ->
    Error.

db_resp_body(Ref) ->
    case hackney:body(Ref) of
        {ok, Body} -> Body;
        {error, _} -> 
            %% Try to close the connection to prevent leaks
            catch hackney:close(Ref),
            <<>>
    end.

%% @doc Asemble the server URL for the given client
%% @spec server_url({Host, Port}) -> iolist()
server_url(#server{url=Url}) ->
    Url.

db_url(#db{name=DbName}) ->
    DbName.

doc_url(Db, DocId) ->
    iolist_to_binary([db_url(Db), <<"/">>, DocId]).


%% attachments handling

%% @hidden
reply_att(ok) ->
    ok;
reply_att(done) ->
    done;
reply_att({ok, 404, _, Ref}) ->
    hackney:skip_body(Ref),
    {error, not_found};
reply_att({ok, 409, _, Ref}) ->
    hackney:skip_body(Ref),
    {error, conflict};
reply_att({ok, Status, _, Ref}) when Status =:= 200 orelse Status =:= 201 ->
  case couchbeam_httpc:json_body(Ref) of
      {[{<<"ok">>, true}|R]} ->
          {ok, {R}};
      {error, _} = Error ->
          Error
  end;
reply_att({ok, Status, Headers, Ref}) ->
    {ok, Body} = hackney:body(Ref),
    {error, {bad_response, {Status, Headers, Body}}};
reply_att(Error) ->
    Error.

%% @hidden
wait_mp_doc(Ref, Buffer) ->
    %% we are always waiting for the full doc
    case hackney:stream_multipart(Ref) of
        {headers, _} ->
            wait_mp_doc(Ref, Buffer);
        {body, Data} ->
            NBuffer = << Buffer/binary, Data/binary >>,
            wait_mp_doc(Ref, NBuffer);
        end_of_part when Buffer =:= <<>> ->
            %% end of part in multipart/mixed
            wait_mp_doc(Ref, Buffer);
        end_of_part ->
            %% decode the doc
            {Props} = Doc = couchbeam_ejson:decode(Buffer),
            case couchbeam_util:get_value(<<"_attachments">>, Props, {[]}) of
                {[]} ->
                    %% not attachments wait for the eof or the next doc
                    NState = {Ref, fun() -> wait_mp_doc(Ref, <<>>) end},
                    {doc, Doc, NState};
                {Atts} ->
                    %% start to receive the attachments
                    %% we get the list of attnames for the versions of
                    %% couchdb that don't provide the att name in the
                    %% header.
                    AttNames = [AttName || {AttName, _} <- Atts],
                    NState = {Ref, fun() ->
                                    wait_mp_att(Ref, {nil, AttNames})
                            end},
                    {doc, Doc, NState}
            end;
        mp_mixed ->
            %% we are starting a multipart/mixed (probably on revs)
            %% continue
            wait_mp_doc(Ref, Buffer);
        mp_mixed_eof ->
            %% end of multipar/mixed wait for the next doc
            wait_mp_doc(Ref, Buffer);
        eof ->
            eof
    end.

%% @hidden
wait_mp_att(Ref, {AttName, AttNames}) ->
    case hackney:stream_multipart(Ref) of
        {headers, Headers} ->
            %% old couchdb api doesn't give the content-disposition
            %% header so we have to use the list of att names given in
            %% the doc. Hopefully the erlang parser keeps it in order.
            case hackney_headers:get_value(<<"content-disposition">>,
                                           hackney_headers:new(Headers)) of
                undefined ->
                    [Name | Rest] = AttNames,
                    NState = {Ref, fun() ->
                                    wait_mp_att(Ref, {Name, Rest})
                            end},
                    {att, Name, NState};
                CDisp ->
                    {_, Props} = content_disposition(CDisp),
                    Name = proplists:get_value(<<"filename">>, Props),
                    [_ | Rest] = AttNames,
                    NState = {Ref, fun() ->
                                    wait_mp_att(Ref, {Name, Rest})
                            end},
                    {att, Name, NState}
            end;
        {body, Data} ->
            %% return the attachment par
            NState = {Ref, fun() -> wait_mp_att(Ref, {AttName, AttNames}) end},
            {att_body, AttName, Data, NState};
        end_of_part ->
            %% wait for the next attachment
            NState = {Ref, fun() -> wait_mp_att(Ref, {nil, AttNames}) end},
            {att_eof, AttName, NState};
        mp_mixed_eof ->
            %% wait for the next doc
            wait_mp_doc(Ref, <<>>);
        eof ->
            %% we are done with the multipart request
            eof
    end.

%% @hidden
content_disposition(Data) ->
    hackney_bstr:token_ci(Data, fun
            (_Rest, <<>>) ->
                {error, badarg};
            (Rest, Disposition) ->
                hackney_bstr:params(Rest, fun
                        (<<>>, Params) -> {Disposition, Params};
                        (_Rest2, _) -> {error, badarg}
                    end)
        end).

%% @hidden
len_doc_to_mp_stream(Atts, Boundary, {Props}) ->
    {AttsSize, Stubs} = lists:foldl(fun(Att, {AccSize, AccAtts}) ->
                    {AttLen, Name, Type, Encoding, _Msg} = att_info(Att),
                    AccSize1 = AccSize +
                               4 + %% \r\n\r\n
                               AttLen +
                               byte_size(hackney_bstr:to_binary(AttLen)) +
                               4 +  %% "\r\n--"
                               byte_size(Boundary) +
                               byte_size(Name) +
                               byte_size(Type) +
                               byte_size(<<"\r\nContent-Disposition: attachment; filename=\"\"">> ) +
                               byte_size(<<"\r\nContent-Type: ">>) +
                               byte_size(<<"\r\nContent-Length: ">>) +
                               case Encoding of
                                   <<"identity">> ->
                                       0;
                                   _ ->
                                       byte_size(Encoding) +
                                       byte_size(<<"\r\nContent-Encoding: ">>)
                               end,
                    AccAtts1 = [{Name, {[{<<"content_type">>, Type},
                                         {<<"length">>, AttLen},
                                         {<<"follows">>, true},
                                         {<<"encoding">>, Encoding}]}}
                                | AccAtts],
                    {AccSize1, AccAtts1}
            end, {0, []}, Atts),

    Doc1 = case couchbeam_util:get_value(<<"_attachments">>, Props) of
        undefined ->
            {Props ++ [{<<"_attachments">>, {Stubs}}]};
        {OldAtts} ->
            %% remove updated attachments from the old list of
            %% attachments
            OldAtts1 = lists:foldl(fun({Name, AttProps}, Acc) ->
                            case couchbeam_util:get_value(Name, Stubs) of
                                undefined ->
                                    [{Name, AttProps} | Acc];
                                _ ->
                                    Acc
                            end
                    end, [], OldAtts),
            %% update the list of the attachnebts with the attachments
            %% that will be sent in the multipart
            FinalAtts = lists:reverse(OldAtts1) ++ Stubs,
            {lists:keyreplace(<<"_attachments">>, 1, Props,
                             {<<"_attachments">>, {FinalAtts}})}
    end,

    %% eencode the doc
    JsonDoc = couchbeam_ejson:encode(Doc1),

    %% calculate the final size with the doc part
    FinalSize = 2 + % "--"
                byte_size(Boundary) +
                36 + % "\r\ncontent-type: application/json\r\n\r\n"
                byte_size(JsonDoc) +
                4 + % "\r\n--"
                byte_size(Boundary) +
                + AttsSize +
                2, % "--"

    {FinalSize, JsonDoc, Doc1}.

%% @hidden
send_mp_doc(Atts, Ref, Boundary, JsonDoc, Doc) ->
    %% send the doc
    DocParts = [<<"--", Boundary/binary >>,
                <<"Content-Type: application/json">>,
                <<>>, JsonDoc, <<>>],
    DocBin = hackney_bstr:join(DocParts, <<"\r\n">>),
    case hackney:send_body(Ref, DocBin) of
        ok ->
            send_mp_doc_atts(Atts, Ref, Doc, Boundary);
        Error ->
            Error
    end.

%% @hidden
send_mp_doc_atts([], Ref, Doc, Boundary) ->
    %% no more attachments, send the final boundary (eof)
    case hackney:send_body(Ref, <<"\r\n--", Boundary/binary, "--" >>) of
        ok ->
            %% collect the response.
            mp_doc_reply(Ref, Doc);
        Error ->
            Error
    end;

send_mp_doc_atts([Att | Rest], Ref, Doc, Boundary) ->
    {AttLen, Name, Type, Encoding, Msg} = att_info(Att),
    BinAttLen = hackney_bstr:to_binary(AttLen),
    AttHeadersParts = [<<"--", Boundary/binary >>,
                       <<"Content-Disposition: attachment; filename=\"", Name/binary, "\"" >>,
                      <<"Content-Type: ", Type/binary >>,
                      <<"Content-Length: ", BinAttLen/binary >>],

    AttHeadersParts1 = AttHeadersParts ++
            case Encoding of
                <<"identity">> ->
                    [<<>>, <<>>];
                _ ->
                    [<<"Content-Encoding: ", Encoding/binary >>, <<>>,
                     <<>>]
            end,
    AttHeadersBin = hackney_bstr:join(AttHeadersParts1, <<"\r\n">>),

    %% first send the att headers
    case hackney:send_body(Ref, AttHeadersBin) of
        ok ->
            %% send the attachment by itself
            case hackney:send_body(Ref, Msg) of
                ok ->
                    %% everything is OK continue to the next attachment
                    send_mp_doc_atts(Rest, Ref, Doc, Boundary);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @hidden
mp_doc_reply(Ref, Doc) ->
    Resp = hackney:start_response(Ref),
    case couchbeam_httpc:db_resp(Resp, [200, 201]) of
        {ok, _, _, Ref} ->
            {JsonProp} = couchbeam_httpc:json_body(Ref),
            NewRev = couchbeam_util:get_value(<<"rev">>, JsonProp),
            NewDocId = couchbeam_util:get_value(<<"id">>, JsonProp),
            %% set the new doc ID
            Doc1 = couchbeam_doc:set_value(<<"_id">>, NewDocId, Doc),
            %% set the new rev
            FinalDoc = couchbeam_doc:set_value(<<"_rev">>, NewRev, Doc1),
            %% return the updated document
            {ok, FinalDoc};
        Error ->
            Error
    end.

%% @hidden
att_info({Name, {file, Path}=Msg}) ->
    CType = mimerl:filename(hackney_bstr:to_binary(Name)),
    Len = filelib:file_size(Path),
    {Len, Name, CType, <<"identity">>, Msg};
att_info({Name, Bin}) when is_list(Bin) ->
    att_info({Name, iolist_to_binary(Bin)});
att_info({Name, Bin}) when is_binary(Bin) ->
    CType = mimerl:filename(hackney_bstr:to_binary(Name)),
    {byte_size(Bin), Name, CType, <<"identity">>, Bin};
att_info({Name, {file, Path}=Msg, Encoding}) ->
    CType = mimerl:filename(hackney_bstr:to_binary(Name)),
    Len = filelib:file_size(Path),
    {Len, Name, CType, Encoding, Msg};
att_info({Name, {Fun, _Acc0}=Msg, Len}) when is_function(Fun) ->
    {Len, Name, <<"application/octet-stream">>, <<"identity">>, Msg};
att_info({Name, Fun, Len}) when is_function(Fun) ->
    {Len, Name, <<"application/octet-stream">>, <<"identity">>, Fun};
att_info({Name, Bin, Encoding}) when is_binary(Bin) ->
    CType = mimerl:filename(hackney_bstr:to_binary(Name)),
    {byte_size(Bin), Name, CType, Encoding, Bin};
att_info({Name, {Fun, _Acc0}=Msg, Len, Encoding}) when is_function(Fun) ->
    {Len, Name, <<"application/octet-stream">>, Encoding, Msg};
att_info({Name, Fun, Len, Encoding}) when is_function(Fun) ->
    {Len, Name, <<"application/octet-stream">>, Encoding, Fun};
att_info({Name, Bin, CType, Encoding}) when is_binary(Bin) ->
    {byte_size(Bin), Name, CType, Encoding, Bin};
att_info({Name, {Fun, _Acc0}=Msg, Len, CType, Encoding})
                when is_function(Fun) ->
    {Len, Name, CType, Encoding, Msg};
att_info({Name, Fun, Len, CType, Encoding}) when is_function(Fun) ->
    {Len, Name, CType, Encoding, Fun}.
