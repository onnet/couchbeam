%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_view).
-author('Benoît Chesneau <benoitc@e-engura.org>').

-include("couchbeam.hrl").

-export([fetch_bounded_v2/4]).
-bounded_reader_v2_capability({'bounded_reader', 2}).

-export([stream/2, stream/3,
         cancel_stream/1, stream_next/1,
         fetch/1, fetch/2, fetch/3,
         fetch_bounded/4,
         count/1, count/2, count/3,
         first/1, first/2, first/3,
         all/1, all/2,
         fold/4, fold/5,
         foreach/3, foreach/4,
         parse_view_options/1,
         show/2, show/3, show/4
        ]).

-ifdef(TEST).
-export([bounded_cleanup_delivery_budget/1, bounded_cleanup_wait_budget/2]).
-endif.

-define(COLLECT_TIMEOUT, 10000).

-spec all(Db::db()) ->
          {ok, Rows::list(ejson_object())} |
          {error, term()} |
          {error, term(), Rows::list(ejson_object())}.
%% @doc fetch all docs
%% @equiv fetch(Db, 'all_docs', [])
all(Db) ->
    fetch(Db, 'all_docs', []).

-spec all(Db::db(), Options::view_options()) ->
          {ok, Rows::list(ejson_object())} |
          {error, term()} |
          {error, term(), Rows::list(ejson_object())}.
%% @doc fetch all docs
%% @equiv fetch(Db, 'all_docs', Options)
all(Db, Options) ->
    fetch(Db, 'all_docs', Options).

-spec fetch(Db::db()) ->
          {ok, Rows::list(ejson_object())} |
          {error, term()} |
          {error, term(), Rows::list(ejson_object())}.
%% @equiv fetch(Db, 'all_docs', [])
fetch(Db) ->
    fetch(Db, 'all_docs', []).

-spec fetch(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                              ViewName::view_name()})
           -> {ok, Rows::list(ejson_object())} | {error, term()}.
%% @equiv fetch(Db, ViewName, [])
fetch(Db, ViewName) ->
    fetch(Db, ViewName,[]).


-spec fetch(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                              ViewName::view_name()}, Options::view_options())
           -> {ok, Rows::list(ejson_object())} | {error, term()}.
%% @doc Collect view results
%%  <p>Db: a db record</p>
%%  <p>ViewName: <code>'all_docs'</code> to get all docs or <code>{DesignName,
%%  ViewName}</code></p>
%%  <pre>Options :: view_options() [{key, binary()}
%%    | {start_docid, binary()} | {startkey_docid, binary()}
%%    | {end_docid, binary()} | {endkey_docid, binary()}
%%    | {start_key, binary()} | {end_key, binary()}
%%    | {limit, integer()}
%%    | {stale, stale()}
%%    | descending
%%    | {skip, integer()}
%%    | group | {group_level, integer()}
%%    | {inclusive_end, boolean()} | {reduce, boolean()} | reduce | include_docs | conflicts
%%    | {keys, list(binary())}
%%    | async_query</pre>
%% <p>See {@link couchbeam_view:stream/4} for more information about
%% options.</p>
%% <p>Return: {ok, Rows} or {error, Error}</p>
fetch(Db, ViewName, Options) ->
    case proplists:is_defined(sync_query, Options) of
        true -> fetch_sync(Db, ViewName, Options);
        false -> fetch_async(Db, ViewName, Options)
    end.

fetch_async(Db, ViewName, Options) ->
    case stream(Db, ViewName, Options) of
        {ok, Ref} ->
            Timeout = proplists:get_value(collect_timeout, Options, ?COLLECT_TIMEOUT),
            collect_view_results(Ref, [], Timeout);
        Error ->
            Error
    end.

%% @doc Fetch a view with a typed request budget `{TimeoutMs, MaxBytes}'.
%% The whole view is collected in the calling process and returned together
%% with the cumulative raw response bytes only after the transport cleanup has
%% been proven; rows already received are discarded on any error. The absolute
%% deadline covers transport, JSON decode and cleanup proof; a completed view
%% whose cleanup proof lands past the deadline is reported as
%% `{'error', 'timeout'}'. `async'/`stream_to' options are ignored: the caller
%% is the only consumer. See `couchbeam_httpc:new_request_budget/1' for the
%% worst-case latency.
%%
%% Unlike the three document doors, this one needs the `couchbeam' application
%% running: the view stream is a child of `couchbeam_view_sup', and
%% `make_view/4' reaches `supervisor:start_child/2', which exits `noproc' when
%% the supervisor is not there. That is a precondition of the door -- the same
%% one legacy `stream/3' has -- and not one of the typed refusals below.
-spec fetch_bounded(db(), 'all_docs' | {binary(), binary()}, list(),
                    couchbeam_httpc:request_budget_spec()) ->
          {'ok', [ejson_object()], non_neg_integer()} | {'error', term()}.
fetch_bounded(Db, ViewName, Options, BudgetSpec) ->
    case usable_view_name(ViewName) of
        'true' -> fetch_bounded_options(Db, ViewName, Options, BudgetSpec);
        'false' -> {'error', {'invalid_view_name', ViewName}}
    end.

%% A view name the bounded door can address before any budget: `all_docs',
%% or `{DesignName, ViewName}' with both halves a binary or a Latin-1 string
%% — what `hackney_url:make_url/3' renders as path segments. A float or a
%% tuple half would crash `hackney_url' after the budget clock started; an
%% atom other than `all_docs' is refused by `make_view/4' only after the
%% budget was created and the stream started; a string with code points
%% above 255 crashes `hackney_bstr:to_binary/1'.
-spec usable_view_name(term()) -> boolean().
usable_view_name('all_docs') ->
    'true';
usable_view_name({DesignName, ViewName}) ->
    usable_view_name_part(DesignName) andalso usable_view_name_part(ViewName);
usable_view_name(_ViewName) ->
    'false'.

-spec usable_view_name_part(term()) -> boolean().
usable_view_name_part(Part) when is_binary(Part) -> 'true';
usable_view_name_part(Part) when is_list(Part) -> io_lib:latin1_char_list(Part);
usable_view_name_part(_Part) -> 'false'.

%% `length/1' in the guard refuses an improper list too: the exception it
%% raises makes the guard false, and `parse_view_options/1' has a clause
%% neither for a non-list nor for an improper tail.
-spec fetch_bounded_options(db(), 'all_docs' | {binary(), binary()}, term(),
                            couchbeam_httpc:request_budget_spec()) ->
          {'ok', [ejson_object()], non_neg_integer()} | {'error', term()}.
fetch_bounded_options(Db, ViewName, Options, BudgetSpec)
  when is_list(Options), length(Options) >= 0 ->
    case bounded_view_query_args(Options) of
        {'ok', #view_query_args{}} ->
            fetch_bounded_parsed(Db, ViewName, Options, BudgetSpec);
        {'error', Entry} ->
            {'error', {'invalid_param', Entry}}
    end;
fetch_bounded_options(_Db, _ViewName, Options, _BudgetSpec) ->
    {'error', {'invalid_param', Options}}.

%% Every refusal a bounded fetch makes before any budget or connection, in
%% the order the failures would otherwise surface: the official parser's own
%% refusal (`{stale, bogus}' — `make_view/4' would crash on it; named by the
%% entry, see `unparsable_view_option/2'), a value the parser cannot encode
%% (`{key, self()}' — `couchbeam_ejson:encode/1' raises inside the parser, in
%% the calling process), a parsed pair `hackney_url:qs/1' cannot render
%% (`{limit, 1.5}' — `make_view/4' would crash while building the URL, after
%% the budget clock started), and a POST body the encoder refuses (`{keys,
%% [self()]}' — the one piece the parser stores unencoded, which would
%% otherwise fail only inside the encoder worker of a stream already spawned
%% under a running budget; see `encodable_view_keys/1'). The renderability
%% scan runs on the parsed pairs, not on the raw list: the parser turns `key'
%% into an encoded binary and `descending' into a pair. An exception the
%% parser raises is an input error only when one of the encoded entries is
%% its cause (jsx raises `function_clause', not `badarg', on a pid, so the
%% class cannot be narrowed instead); anything else is a defect of the parser
%% or the encoder and is re-raised with its stack rather than relabelled as
%% the caller's fault.
%% A fifth outcome is not a refusal and is worth naming: an unrecognised
%% atom-keyed pair is neither refused nor sent -- the official parser's
%% catch-all drops it (parity with legacy `fetch/3'), so a typo in an option
%% name is silently ignored. Whether the bounded door should narrow that is
%% an owner decision recorded with the port's deferred work.
-spec bounded_view_query_args(list()) ->
          {'ok', view_query_args()} | {'error', term()}.
bounded_view_query_args(Options) ->
    try parse_view_options(Options) of
        #view_query_args{options=Parsed}=Args ->
            case couchbeam_httpc:invalid_query_param(Parsed) of
                'undefined' -> encodable_view_keys(Args);
                {'invalid_param', Entry} -> {'error', Entry}
            end;
        {'error', Reason} ->
            {'error', unparsable_view_option(Options, Reason)}
    catch
        Class:Reason:Stacktrace ->
            case unencodable_view_option(Options) of
                'undefined' -> erlang:raise(Class, Reason, Stacktrace);
                Entry -> {'error', Entry}
            end
    end.

%% The parser raised: name the entry whose value `couchbeam_ejson:encode/1'
%% refuses (the parser encodes `key', `startkey'/`start_key' and
%% `endkey'/`end_key' in place), or `'undefined'' when none of them is the
%% cause.
%% The official parser refuses exactly one thing — `{stale, V}' outside
%% `ok'/`update_after'/`false' — and answers with its message string. The
%% door names the entry instead, so every refusal it makes has the one shape
%% `{invalid_param, Entry}'. Should the parser ever refuse something this
%% walk does not recognise, its own reason is passed on rather than guessed.
-spec unparsable_view_option(list(), term()) -> term().
unparsable_view_option([{'stale', Value}=Entry | _Rest], _Reason)
  when Value =/= 'ok', Value =/= 'update_after', Value =/= 'false' ->
    Entry;
unparsable_view_option([_Entry | Rest], Reason) ->
    unparsable_view_option(Rest, Reason);
unparsable_view_option([], Reason) ->
    Reason.

%% Encoded once here for the verdict; the killable worker under the budget
%% encodes the body again. That second pass is the price of refusing before
%% any budget exists: carrying an encoded body to the stream would mean a
%% new field in the official `#view_query_args{}'.
-spec encodable_view_keys(view_query_args()) ->
          {'ok', view_query_args()} | {'error', {'keys', term()}}.
encodable_view_keys(#view_query_args{method='post', keys=Keys}=Args) ->
    try couchbeam_ejson:encode({[{<<"keys">>, Keys}]}) of
        _Encoded -> {'ok', Args}
    catch
        _Class:_Reason -> {'error', {'keys', Keys}}
    end;
encodable_view_keys(#view_query_args{}=Args) ->
    {'ok', Args}.

-spec unencodable_view_option(list()) -> 'undefined' | {atom(), term()}.
unencodable_view_option([{Key, Value}=Entry | Rest])
  when Key =:= 'key'; Key =:= 'startkey'; Key =:= 'start_key';
       Key =:= 'endkey'; Key =:= 'end_key' ->
    try couchbeam_ejson:encode(Value) of
        _Encoded -> unencodable_view_option(Rest)
    catch
        _Class:_Reason -> Entry
    end;
unencodable_view_option([_Entry | Rest]) ->
    unencodable_view_option(Rest);
unencodable_view_option([]) ->
    'undefined'.

-spec fetch_bounded_parsed(db(), 'all_docs' | {binary(), binary()}, list(),
                           couchbeam_httpc:request_budget_spec()) ->
          {'ok', [ejson_object()], non_neg_integer()} | {'error', term()}.
fetch_bounded_parsed(Db, ViewName, Options, BudgetSpec) ->
    case couchbeam_httpc:new_request_budget(BudgetSpec) of
        {'ok', Budget} ->
            FetchOptions = bounded_fetch_options(Options),
            case stream_with_budget(Db, ViewName, FetchOptions, Budget) of
                {'ok', Ref, StreamPid} ->
                    MonitorRef = erlang:monitor('process', StreamPid),
                    collect_bounded_view_results(
                      Ref, StreamPid, MonitorRef, Budget, [], 'undefined');
                {'error', _}=Error ->
                    Error
            end;
        {'error', _}=Error ->
            Error
    end.

%% A bounded fetch is the sole consumer of its stream: `async'/`stream_to'
%% would send rows to a third party and park the decoder waiting for
%% `stream_next' messages the collector never sends. This is the one door a
%% budget enters through, so this is the one place they are stripped.
-spec bounded_fetch_options(list()) -> list().
bounded_fetch_options(Options) ->
    proplists:delete('async', proplists:delete('stream_to', Options)).

-spec collect_bounded_view_results(reference(), pid(), reference(),
                                   couchbeam_httpc:request_budget(),
                                   [ejson_object()],
                                   'undefined' | pid() | 'complete' |
                                   {'pending', couchbeam_httpc:request_budget()}) ->
          {'ok', [ejson_object()], non_neg_integer()} | {'error', term()}.
collect_bounded_view_results(Ref, StreamPid, MonitorRef,
                             #{'deadline_ms' := DeadlineMs}=Budget, Acc,
                             GuardianPid) ->
    case DeadlineMs - erlang:monotonic_time('millisecond') of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {Ref, {'done', Bytes}} ->
                    bounded_view_terminal_after_cleanup(
                      Ref, StreamPid, MonitorRef, GuardianPid, Budget,
                      {'ok', lists:reverse(Acc), Bytes});
                {Ref, {'row', Row}} ->
                    collect_bounded_view_results(
                      Ref, StreamPid, MonitorRef, Budget, [Row | Acc],
                      GuardianPid);
                {'bounded_transport_guardian', StreamPid, NewGuardianPid} ->
                    collect_bounded_view_results(
                      Ref, StreamPid, MonitorRef, Budget, Acc,
                      NewGuardianPid);
                {'bounded_transport_cleanup', StreamPid, _ClientRef} ->
                    collect_bounded_view_results(
                      Ref, StreamPid, MonitorRef, Budget, Acc, 'complete');
                {'bounded_transport_cleanup_started', StreamPid,
                 CleanupBudget} ->
                    collect_bounded_view_results(
                      Ref, StreamPid, MonitorRef, Budget, Acc,
                      {'pending', CleanupBudget});
                {Ref, {'error', 'transport_cleanup_timeout'}=Error} ->
                    %% The stream's own cleanup verdict, and a final one: it
                    %% is reported either past the guardian's acknowledgement
                    %% deadline (the first attempt failed, retries do not
                    %% notify) or because the guardian is already dead
                    %% (`couchbeam_httpc:await_guardian_resources/4',
                    %% `guardian_cancel_result/3'). No lifecycle message can
                    %% follow either way, and the second case arrives well
                    %% inside this deadline — waiting a cleanup budget for
                    %% one would only delay the same verdict by `TimeoutMs'.
                    bounded_view_terminal(Ref, StreamPid, MonitorRef, Error);
                {Ref, {'error', Error}} ->
                    bounded_view_terminal_after_cleanup(
                      Ref, StreamPid, MonitorRef, GuardianPid, Budget,
                      {'error', Error});
                {'DOWN', MonitorRef, 'process', StreamPid, Reason} ->
                    bounded_view_terminal_after_cleanup(
                      Ref, StreamPid, MonitorRef, GuardianPid, Budget,
                      {'error', {'stream_down', Reason}})
            after TimeoutMs ->
                    bounded_view_timeout(
                      Ref, StreamPid, MonitorRef, GuardianPid, Budget)
            end;
        _ ->
            bounded_view_timeout(
              Ref, StreamPid, MonitorRef, GuardianPid, Budget)
    end.

-spec await_stream_transport_cleanup(
        pid(), 'undefined' | pid() | 'complete' |
        {'pending', couchbeam_httpc:request_budget()},
        couchbeam_httpc:request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
await_stream_transport_cleanup(_StreamPid, 'complete', _Budget) ->
    'ok';
await_stream_transport_cleanup(StreamPid, {'pending', CleanupBudget},
                               _Budget) ->
    await_stream_transport_cleanup_pending(StreamPid, CleanupBudget);
await_stream_transport_cleanup(_StreamPid, 'undefined', _Budget) ->
    %% No guardian was ever announced, so there is no transport to prove
    %% closed. The announcement is sent by the stream process itself right
    %% after it spawns the guardian (`couchbeam_httpc:start_request_guardian/4'),
    %% and a stream's messages are ordered before its `'DOWN'', its `done'
    %% and its error report: whichever terminal message brought the collector
    %% here, an announcement would already have been consumed by
    %% `collect_bounded_view_results/6'. Waiting out the cleanup budget here
    %% would only turn a `{'stream_down', _}' verdict into a false
    %% `transport_cleanup_timeout'.
    'ok';
await_stream_transport_cleanup(StreamPid, _GuardianPid, Budget) ->
    case bounded_remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'bounded_transport_cleanup_started', StreamPid,
                 CleanupBudget} ->
                    await_stream_transport_cleanup_pending(
                      StreamPid, CleanupBudget);
                {'bounded_transport_cleanup', StreamPid, _Ref} -> 'ok'
            after TimeoutMs ->
                    {'error', 'transport_cleanup_timeout'}
            end;
        _ ->
            {'error', 'transport_cleanup_timeout'}
    end.

%% The guardian may legitimately finish right at its own cleanup deadline;
%% the collector grants the same delivery allowance on every path
%% (`bounded_cleanup_wait_budget/2' does it for the cancel path).
-spec await_stream_transport_cleanup_pending(
        pid(), couchbeam_httpc:request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
await_stream_transport_cleanup_pending(StreamPid, CleanupBudget) ->
    Budget = bounded_cleanup_delivery_budget(CleanupBudget),
    case bounded_remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'bounded_transport_cleanup', StreamPid, _Ref} -> 'ok'
            after TimeoutMs ->
                    {'error', 'transport_cleanup_timeout'}
            end;
        _ ->
            {'error', 'transport_cleanup_timeout'}
    end.

-spec bounded_remaining_timeout(couchbeam_httpc:request_budget()) -> integer().
bounded_remaining_timeout(#{'deadline_ms' := DeadlineMs}) ->
    DeadlineMs - erlang:monotonic_time('millisecond').

-spec bounded_view_timeout(reference(), pid(), reference(),
                           'undefined' | pid() | 'complete' |
                           {'pending', couchbeam_httpc:request_budget()},
                           couchbeam_httpc:request_budget()) ->
          {'error', term()}.
bounded_view_timeout(Ref, StreamPid, MonitorRef, GuardianState, Budget) ->
    StreamPid ! {Ref, 'budget_timeout'},
    await_bounded_view_cancel(
      Ref, StreamPid, MonitorRef, GuardianState,
      bounded_cleanup_budget(Budget), 'undefined').

-spec await_bounded_view_cancel(reference(), pid(), reference(),
                                'undefined' | pid() | 'complete' |
                                {'pending', couchbeam_httpc:request_budget()},
                                couchbeam_httpc:request_budget(),
                                'undefined' | {'error', term()}) ->
          {'error', term()}.
await_bounded_view_cancel(Ref, StreamPid, MonitorRef, 'complete', _Budget,
                          {'error', _}=Result) ->
    bounded_view_terminal(Ref, StreamPid, MonitorRef, Result);
await_bounded_view_cancel(Ref, StreamPid, MonitorRef, GuardianState, Budget,
                          Result) ->
    WaitBudget = bounded_cleanup_wait_budget(GuardianState, Budget),
    case bounded_remaining_timeout(WaitBudget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {Ref, {'row', _Row}} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, GuardianState, Budget,
                      Result);
                {'bounded_transport_guardian', StreamPid, GuardianPid} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, GuardianPid, Budget,
                      Result);
                {'bounded_transport_cleanup', StreamPid, _ClientRef} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, 'complete', Budget,
                      Result);
                {'bounded_transport_cleanup_started', StreamPid,
                 CleanupBudget} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef,
                      {'pending', CleanupBudget}, Budget, Result);
                {Ref, {'done', _Bytes}} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, GuardianState, Budget,
                      {'error', 'timeout'});
                {Ref, {'error', 'transport_cleanup_timeout'}=Error} ->
                    %% Final at once, for the reasons given in
                    %% `collect_bounded_view_results/6'.
                    bounded_view_terminal(Ref, StreamPid, MonitorRef, Error);
                {Ref, {'error', Error}} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, GuardianState, Budget,
                      {'error', Error});
                {'DOWN', MonitorRef, 'process', StreamPid, _Reason}
                  when GuardianState =:= 'undefined' ->
                    %% Same reasoning as `await_stream_transport_cleanup/3':
                    %% the stream announced no guardian before it died, so
                    %% nothing can ever send a cleanup verdict. The deadline
                    %% verdict is final.
                    bounded_view_terminal(
                      Ref, StreamPid, MonitorRef, {'error', 'timeout'});
                {'DOWN', MonitorRef, 'process', StreamPid, _Reason} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, GuardianState, Budget,
                      {'error', 'timeout'})
            after TimeoutMs ->
                    bounded_view_terminal(
                      Ref, StreamPid, MonitorRef,
                      {'error', 'transport_cleanup_timeout'})
            end;
        _ ->
            bounded_view_terminal(
              Ref, StreamPid, MonitorRef,
              {'error', 'transport_cleanup_timeout'})
    end.

-spec bounded_view_terminal_after_cleanup(
        reference(), pid(), reference(),
        'undefined' | pid() | 'complete' |
        {'pending', couchbeam_httpc:request_budget()},
        couchbeam_httpc:request_budget(), term()) -> term().
%% A stream's `transport_cleanup_timeout' report never reaches this function:
%% `collect_bounded_view_results/6' and `await_bounded_view_cancel/6' both
%% treat it as final the moment it arrives, so every result that comes here
%% still owes the caller a cleanup proof.
bounded_view_terminal_after_cleanup(Ref, StreamPid, MonitorRef, GuardianState,
                                    Budget, Result) ->
    CleanupBudget = bounded_cleanup_budget(Budget),
    case await_stream_transport_cleanup(
           StreamPid, GuardianState, CleanupBudget) of
        'ok' -> bounded_view_terminal(Ref, StreamPid, MonitorRef, Result);
        {'error', 'transport_cleanup_timeout'}=Error ->
            bounded_view_terminal(Ref, StreamPid, MonitorRef, Error)
    end.

-spec bounded_cleanup_budget(couchbeam_httpc:request_budget()) ->
          couchbeam_httpc:request_budget().
bounded_cleanup_budget(#{'timeout_ms' := TimeoutMs}=Budget) ->
    Budget#{'deadline_ms' =>
                erlang:monotonic_time('millisecond') + TimeoutMs}.

-spec bounded_cleanup_wait_budget(
        'undefined' | pid() | 'complete' |
        {'pending', couchbeam_httpc:request_budget()},
        couchbeam_httpc:request_budget()) -> couchbeam_httpc:request_budget().
bounded_cleanup_wait_budget({'pending', CleanupBudget}, _FallbackBudget) ->
    bounded_cleanup_delivery_budget(CleanupBudget);
bounded_cleanup_wait_budget(GuardianPid, FallbackBudget)
  when is_pid(GuardianPid) ->
    %% A guardian that never announced its cleanup may be dead: the stream
    %% then proves the cleanup itself against the manager table, on a
    %% cleanup budget that starts strictly after this one, and reports the
    %% outcome one budget later. The same allowance as the `pending' case,
    %% or that report would land right after the terminal flush and stay in
    %% the caller's mailbox. What it costs: when neither the stream nor the
    %% guardian can answer (both dead), the cancel path waits 3T, not T.
    bounded_cleanup_delivery_budget(FallbackBudget);
bounded_cleanup_wait_budget(_CleanupState, FallbackBudget) ->
    FallbackBudget.

%% One `TimeoutMs' above the stream's own acknowledgement deadline
%% (`couchbeam_httpc:cleanup_ack_deadline/1' = cleanup deadline + T): the
%% stream reports `transport_cleanup_timeout' only after that deadline, and an
%% allowance that expired at the same instant would let the report land after
%% the terminal flush and stay in the caller's mailbox.
-spec bounded_cleanup_delivery_budget(couchbeam_httpc:request_budget()) ->
          couchbeam_httpc:request_budget().
bounded_cleanup_delivery_budget(#{'deadline_ms' := CleanupDeadline,
                                  'timeout_ms' := TimeoutMs}=Budget) ->
    Budget#{'deadline_ms' => CleanupDeadline + 2 * TimeoutMs}.

%% Nothing tagged with this view may outlive the API call in the caller's
%% mailbox: neither stream messages `{Ref, _}' nor guardian lifecycle
%% messages `{'bounded_transport_*', StreamPid, _}' that arrive after a
%% cleanup timeout was already reported.
-spec bounded_view_terminal(reference(), pid(), reference(), term()) -> term().
bounded_view_terminal(Ref, StreamPid, MonitorRef, Result) ->
    erlang:demonitor(MonitorRef, ['flush']),
    flush_view_messages(Ref),
    flush_lifecycle_messages(StreamPid),
    Result.

-spec flush_view_messages(reference()) -> 'ok'.
flush_view_messages(Ref) ->
    receive
        {Ref, _Message} -> flush_view_messages(Ref)
    after 0 ->
            'ok'
    end.

-spec flush_lifecycle_messages(pid()) -> 'ok'.
flush_lifecycle_messages(StreamPid) ->
    receive
        {'bounded_transport_guardian', StreamPid, _} ->
            flush_lifecycle_messages(StreamPid);
        {'bounded_transport_cleanup_started', StreamPid, _} ->
            flush_lifecycle_messages(StreamPid);
        {'bounded_transport_cleanup', StreamPid, _} ->
            flush_lifecycle_messages(StreamPid)
    after 0 ->
            'ok'
    end.

fetch_sync(Db, ViewName, Options) ->
    make_view(Db, ViewName, Options, fetch_sync_fun(Db)).

fetch_sync_fun(Db) ->
    fun(Args, Url) ->
        case view_request(Db, Url, Args) of
            {ok, _, _, Ref} ->
                {Props} = couchbeam_httpc:json_body(Ref),
                {ok, couchbeam_util:get_value(<<"rows">>, Props)};
            Error ->
                Error
        end
    end.

-spec show(db(), {binary(), binary()}) ->
          {'ok', ejson_object()} |
          {'error', term()}.
show(Db, ShowName) ->
    show(Db, ShowName, <<>>).

-spec show(db(), {binary(), binary()}, binary()) ->
          {'ok', ejson_object()} |
          {'error', term()}.
show(Db, ShowName, DocId) ->
    show(Db, ShowName, DocId, []).

-type show_option() :: {'query_string', binary()}. % "foo=bar&baz=biz"
-type show_options() :: [show_option()].

-spec show(db(), {binary(), binary()}, 'null' | binary(), show_options()) ->
          {'ok', ejson_object()} |
          {'error', term()}.
show(#db{server=Server, options=DBOptions}=Db
    ,{<<DesignName/binary>>, <<ShowName/binary>>}
    ,DocId
    ,Options
    ) ->
    URL = hackney_url:make_url(couchbeam_httpc:server_url(Server)
                              ,iolist_to_binary([couchbeam_httpc:db_url(Db)
                                                ,<<"/_design/">>
                                                ,DesignName
                                                ,<<"/_show/">>
                                                ,ShowName
                                                ,show_doc_id(DocId)
                                                ,get_query_string(Options)
                                                ]
                                               )
                              ,Options
                              ),

    case couchbeam_httpc:db_request('get', URL, [], <<>>, DBOptions, [200]) of
        {ok, _, _, Ref} ->
            {'ok', couchbeam_httpc:json_body(Ref)};
        Error -> Error
    end.

get_query_string(Options) ->
    case proplists:get_value('query_string', Options) of
        'undefined' -> <<>>;
        <<>> -> <<>>;
        <<KVs/binary>> -> <<"?", KVs/binary>>
    end.

show_doc_id('null') -> <<>>;
show_doc_id(<<>>) -> <<>>;
show_doc_id(<<DocId/binary>>) -> [<<"/">>, couchbeam_util:encode_docid(DocId)].

-spec stream(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                               ViewName::view_name()}) -> {ok, StartRef::term(),
                                                                           ViewPid::pid()} | {error, term()}.
%% @equiv stream(Db, ViewName, Client, [])
stream(Db, ViewName) ->
    stream(Db, ViewName, []).

-spec stream(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                               ViewName::view_name()}, Options::view_options())
            -> {ok, StartRef::term()} | {error, term()}.
%% @doc stream view results to a pid
%%  <p>Db: a db record</p>
%%  <p>ViewName: 'all_docs' to get all docs or {DesignName,
%%  ViewName}</p>
%%  <p>Client: pid where to send view events where events are:
%%  <dl>
%%      <dt>{row, StartRef, done}</dt>
%%          <dd>All view results have been fetched</dd>
%%      <dt>{row, StartRef, Row :: ejson_object()}</dt>
%%          <dd>A row in the view</dd>
%%      <dt>{error, StartRef, Error}</dt>
%%          happend.</dd>
%%  </dl></p>
%%  <p><pre>Options :: view_options() [{key, binary()}
%%    | descending
%%    | {skip, integer()}
%%    | group | {group_level, integer()}
%%    | {inclusive_end, boolean()} | {reduce, boolean()} | reduce | include_docs | conflicts
%%    | {keys, list(binary())}
%%    | `{stream_to, Pid}': the pid where the changes will be sent,
%%      by default the current pid. Used for continuous and longpoll
%%      connections</pre>
%%
%%  <ul>
%%      <li><code>{key, Key}</code>: key value</li>
%%      <li><code>{start_docid, DocId}</code> | <code>{startkey_docid, DocId}</code>: document id to start with (to allow pagination
%%          for duplicate start keys</li>
%%      <li><code>{end_docid, DocId}</code> | <code>{endkey_docid, DocId}</code>: last document id to include in the result (to
%%          allow pagination for duplicate endkeys)</li>
%%      <li><code>{start_key, Key}</code>: start result from key value</li>
%%      <li><code>{end_key, Key}</code>: end result from key value</li>
%%      <li><code>{limit, Limit}</code>: Limit the number of documents in the result</li>
%%      <li><code>{stale, Stale}</code>: If stale=ok is set, CouchDB will not refresh the view
%%      even if it is stale, the benefit is a an improved query latency. If
%%      stale=update_after is set, CouchDB will update the view after the stale
%%      result is returned. If stale=false is set, CouchDB will update the view before
%%      the query. The default value of this parameter is update_after.</li>
%%      <li><code>descending</code>: reverse the result</li>
%%      <li><code>{skip, N}</code>: skip n number of documents</li>
%%      <li><code>group</code>: the reduce function reduces to a single result
%%      row.</li>
%%      <li><code>{group_level, Level}</code>: the reduce function reduces to a set
%%      of distinct keys.</li>
%%      <li><code>{reduce, boolean()}</code>: whether to use the reduce function of the view. It defaults to
%%      true, if a reduce function is defined and to false otherwise.</li>
%%      <li><code>include_docs</code>: automatically fetch and include the document
%%      which emitted each view entry</li>
%%      <li><code>{inclusive_end, boolean()}</code>: Controls whether the endkey is included in
%%      the result. It defaults to true.</li>
%%      <li><code>conflicts</code>: include conflicts</li>
%%      <li><code>{keys, [Keys]}</code>: to pass multiple keys to the view query</li>
%%  </ul></p>
%%
%% <p> Return <code>{ok, StartRef, ViewPid}</code> or <code>{error,
                                                %Error}</code>. Ref can be
%% used to disctint all changes from this pid. ViewPid is the pid of
%% the view loop process. Can be used to monitor it or kill it
%% when needed.</p>
stream(Db, ViewName, Options) ->
    stream_with_budget(Db, ViewName, Options, 'undefined').

-spec stream_with_budget(db(), 'all_docs' | {binary(), binary()}, list(),
                         'undefined' | couchbeam_httpc:request_budget()) ->
          {'ok', reference()} | {'ok', reference(), pid()} | {'error', term()}.
stream_with_budget(Db, ViewName, Options0, Budget) ->
    {To, Options1} = case proplists:get_value(stream_to, Options0) of
                         undefined ->
                             {self(), Options0};
                         StreamOwner ->
                             {StreamOwner,
                              proplists:delete(stream_to, Options0)}
                     end,
    Options = view_options(Options0),
    StreamOptions = stream_options(Options, Budget),
    make_view(Db, ViewName, Options1, fun(Args, Url) ->
                                              Ref = make_ref(),
                                              Req = {Db, Url, Args},
                                              case supervisor:start_child(couchbeam_view_sup, [To,
                                                                                               Ref,
                                                                                               Req,
                                                                                               StreamOptions]) of
                                                  {'ok', ViewPid} ->
                                                      stream_result(
                                                        Ref, ViewPid, Budget);
                                                  Error ->
                                                      Error
                                              end
                                      end).

-spec stream_result(reference(), pid(),
                    'undefined' | couchbeam_httpc:request_budget()) ->
          {'ok', reference()} | {'ok', reference(), pid()}.
stream_result(Ref, _Pid, 'undefined') ->
    {'ok', Ref};
stream_result(Ref, Pid, _Budget) ->
    {'ok', Ref, Pid}.

%% The legacy stream is the official lifecycle: a stray `request_budget'
%% option must not turn it into a bounded stream (its `{Ref, {'done', Bytes}}'
%% and lifecycle messages would never be consumed by legacy collectors).
-spec stream_options(list(),
                     'undefined' | couchbeam_httpc:request_budget()) -> list().
stream_options(Options, 'undefined') ->
    proplists:delete('request_budget', Options);
stream_options(Options, Budget) ->
    [{'request_budget', Budget} | proplists:delete('request_budget', Options)].

view_options(Options) ->
    Funs = [fun kz_log_id/0
           ,fun kz_application/0
           ],
    lists:foldl(fun view_options_fold/2, Options, Funs).

view_options_fold(Fun, Acc) ->
    case Fun() of
        undefined -> Acc;
        Value -> [Value | Acc]
    end.

kz_log_id() ->
    case kz_log:get_callid() of
        <<"00000000000">> -> undefined;
        LogId -> {kz_log_id, LogId}
    end.

kz_application() ->
    case get_kz_application() of
        {ok, App} -> {kz_application, App};
        _Other -> undefined
    end.

get_kz_application() ->
    case erlang:get(kz_application) of
        undefined -> application:get_application();
        App -> {ok, App}
    end.

cancel_stream(Ref) ->
    with_view_stream(Ref, fun(Pid) ->
                                  case supervisor:terminate_child(couchbeam_view_sup, Pid) of
                                      ok ->
                                          case supervisor:delete_child(couchbeam_view_sup,
                                                                       Pid) of
                                              ok ->
                                                  ok;
                                              {error, not_found} ->
                                                  ok;
                                              Error ->
                                                  Error
                                          end;
                                      Error ->
                                          Error
                                  end
                          end).

stream_next(Ref) ->
    with_view_stream(Ref, fun(Pid) ->
                                  Pid ! {Ref, stream_next}
                          end).

-spec count(Db::db()) -> integer() | {error, term()}.
%% @equiv count(Db, 'all_docs', [])
count(Db) ->
    count(Db, 'all_docs', []).

-spec count(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                              ViewName::view_name()}) -> integer() | {error, term()}.
%% @equiv count(Db, ViewName, [])
count(Db, ViewName) ->
    count(Db, ViewName, []).

-spec count(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                              ViewName::view_name()}, Options::view_options())
           -> integer() | {error, term()}.
%% @doc count number of doc in a view (or all docs)
count(Db, ViewName, Options)->
    %% make sure we set the limit to 0 here so we don't have to get all
    %% the results to count them...
    Options1 = couchbeam_util:force_param(limit, 0, Options),

    %% make the request
    make_view(Db, ViewName, Options1, fun(Args, Url) ->
                                              case view_request(Db, Url, Args) of
                                                  {ok, _, _, Ref} ->
                                                      {Props} = couchbeam_httpc:json_body(Ref),
                                                      couchbeam_util:get_value(<<"total_rows">>, Props);
                                                  Error ->
                                                      Error
                                              end
                                      end).

-spec first(Db::db()) -> {ok, Row::ejson_object()} | {error, term()}.
first(Db) ->
    first(Db, 'all_docs', []).

-spec first(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                              ViewName::view_name()})
           -> {ok, Row::ejson_object()} | {error, term()}.
%% @equiv first(Db, ViewName, [])
first(Db, ViewName) ->
    first(Db, ViewName,[]).


-spec first(Db::db(), ViewName::'all_docs' | {DesignName::design_name(),
                                              ViewName::view_name()}, Options::view_options())
           -> {ok, Rows::ejson_object()} | {error, term()}.
%% @doc get first result of a view
%%  <p>Db: a db record</p>
%%  <p>ViewName: 'all_docs' to get all docs or {DesignName,
%%  ViewName}</p>
%%  <pre>Options :: view_options() [{key, binary()}
%%    | {start_docid, binary()} | {startkey_docid, binary()}
%%    | {end_docid, binary()} | {endkey_docid, binary()}
%%    | {start_key, binary()} | {end_key, binary()}
%%    | {limit, integer()}
%%    | {stale, stale()}
%%    | descending
%%    | {skip, integer()}
%%    | group | {group_level, integer()}
%%    | {inclusive_end, boolean()} | {reduce, boolean()} | reduce | include_docs | conflicts
%%    | {keys, list(binary())}</pre>
%% <p>See {@link couchbeam_view:stream/4} for more information about
%% options.</p>
%% <p>Return: {ok, Row} or {error, Error}</p>
first(Db, ViewName, Options) ->
    %% we only want 1 result so force the limit to 1. no need to fetch
    %% all the results
    Options1 = couchbeam_util:force_param(limit, 1, Options),

    %% make the request
    make_view(Db, ViewName, Options1, fun(Args, Url) ->
                                              case view_request(Db, Url, Args) of
                                                  {ok, _, _, Ref} ->
                                                      {Props} = couchbeam_httpc:json_body(Ref),
                                                      case couchbeam_util:get_value(<<"rows">>, Props) of
                                                          [] ->
                                                              {ok, nil};
                                                          [Row] ->
                                                              {ok, Row}
                                                      end;
                                                  Error ->
                                                      Error
                                              end
                                      end).

-spec fold(Function::function(), Acc::any(), Db::db(),
           ViewName::'all_docs' | {DesignName::design_name(), ViewName::view_name()})
          -> list(term()) | {error, term()}.
%% @equiv fold(Function, Acc, Db, ViewName, [])
fold(Function, Acc, Db, ViewName) ->
    fold(Function, Acc, Db, ViewName, []).

-spec fold(Function::function(), Acc::any(), Db::db(),
           ViewName::'all_docs' | {DesignName::design_name(),
                                   ViewName::view_name()}, Options::view_options())
          -> list(term()) | {error, term()}.
%% @doc call Function(Row, AccIn) on succesive row, starting with
%% AccIn == Acc. Function/2 must return a new list accumultator or the
%% atom <em>done</em> to stop fetching results. Acc0 is returned if the
%% list is empty. For example:
%% ```
%% couchbeam_view:fold(fun(Row, Acc) -> [Row|Acc] end, [], Db, 'all_docs').
%% '''
fold(Function, Acc, Db, ViewName, Options) ->
    %% make sure we stream item by item so we can stop at any time.
    Options1 = couchbeam_util:force_param(async, once, Options),
    %% start iterrating the view results
    case stream(Db, ViewName, Options1) of
        {ok, Ref} ->
            fold_view_results(Ref, Function, Acc);
        Error ->
            Error
    end.

-spec foreach(Function::function(), Db::db(),
              ViewName::'all_docs' | {DesignName::design_name(), ViewName::view_name()})
             -> list(term()) | {error, term()}.
%% @equiv foreach(Function, Db, ViewName, [])
foreach(Function, Db, ViewName) ->
    foreach(Function, Db, ViewName, []).

-spec foreach(Function::function(),  Db::db(),
              ViewName::'all_docs' | {DesignName::design_name(),
                                      ViewName::view_name()}, Options::view_options())
             -> list(term()) | {error, term()}.
%% @doc call Function(Row) on succesive row. Example:
%% ```
%% couchbeam_view:foreach(fun(Row) -> io:format("got row ~p~n", [Row]) end, Db, 'all_docs').
%% '''
foreach(Function, Db, ViewName, Options) ->
    FunWrapper = fun(Row, _Acc) ->
                         Function(Row),
                         ok
                 end,
    fold(FunWrapper, ok, Db, ViewName, Options).


%% ----------------------------------
%% utilities functions
%% ----------------------------------

-spec parse_view_options(Options::list()) -> view_query_args().
%% @doc parse view options
parse_view_options(Options) ->
    parse_view_options(Options, #view_query_args{}).

parse_view_options([], Args) ->
    Args;
parse_view_options([{key, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{key, couchbeam_ejson:encode(Value)}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{start_docid, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{startkey_docid, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{startkey_docid, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{startkey_docid, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{end_docid, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{end_docid, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{endkey_docid, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{endkey_docid, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{start_key, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{start_key, couchbeam_ejson:encode(Value)}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{end_key, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{end_key, couchbeam_ejson:encode(Value)}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{startkey, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{startkey, couchbeam_ejson:encode(Value)}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{endkey, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{endkey, couchbeam_ejson:encode(Value)}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{limit, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{limit, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{stale, ok}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{stale, "ok"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{stale, update_after}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{stale, "update_after"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{stale, false}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{stale, "false"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{stale, _}|_Rest], _Args) ->
    {error, "invalid stale value"};
parse_view_options([{stable, true}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{stable, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{stable, false}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{stable, "false"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{update, true}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{update, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{update, false}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{update, "false"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{update, lazy}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{update, "lazy"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([descending|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{descending, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([group|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{group, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{group_level, Level}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{group_level, Level}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([inclusive_end|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{inclusive_end, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{inclusive_end, true}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{inclusive_end, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{inclusive_end, false}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{inclusive_end, "false"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([reduce|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{reduce, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{reduce, true}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{reduce, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{reduce, false}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{reduce, "false"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([include_docs|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{include_docs, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([conflicts|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{conflicts, "true"}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{skip, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{skip, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{list, Value}|Rest], #view_query_args{options=Opts}=Args) ->
    Opts1 = [{list, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([{keys, Value}|Rest], Args) ->
    parse_view_options(Rest, Args#view_query_args{method=post,
                                                  keys=Value});
parse_view_options([{Key, Value}|Rest], #view_query_args{options=Opts}=Args)
  when is_list(Key) ->
    Opts1 = [{Key, Value}|Opts],
    parse_view_options(Rest, Args#view_query_args{options=Opts1});
parse_view_options([_|Rest], Args) ->
    parse_view_options(Rest, Args).

%% @private

make_view(#db{server=Server}=Db, ViewName, Options, Fun) ->
    Args = parse_view_options(Options),
    ListName = proplists:get_value(list, Options),
    case ViewName of
        'all_docs' ->
            Url = hackney_url:make_url(couchbeam_httpc:server_url(Server),
                                       [couchbeam_httpc:db_url(Db), <<"_all_docs">>],
                                       Args#view_query_args.options),
            Fun(Args, Url);
        {DName, VName} when ListName =:= 'undefined' ->
            Url = hackney_url:make_url(couchbeam_httpc:server_url(Server),
                                       [couchbeam_httpc:db_url(Db), <<"_design">>,
                                        DName, <<"_view">>, VName],
                                       Args#view_query_args.options),
            Fun(Args, Url);
        {DName, VName} ->
            Url = hackney_url:make_url(couchbeam_httpc:server_url(Server),
                                       [couchbeam_httpc:db_url(Db), <<"_design">>,
                                        DName, <<"_list">>, ListName, VName],
                                       Args#view_query_args.options),
            Fun(Args, Url);
        _ ->
            {error, invalid_view_name}
    end.

fold_view_results(Ref, Fun, Acc) ->
    receive
        {Ref, done} ->
            Acc;
        {Ref, {row, Row}} ->
            case Fun(Row, Acc) of
                stop ->
                    cancel_stream(Ref),
                    Acc;
                Acc1 ->
                    stream_next(Ref),
                    fold_view_results(Ref, Fun, Acc1)
            end;
        {Ref, Error} ->
            {error, Acc, Error}
    end.

-spec collect_view_results(reference(), Rows::list(ejson_object()), Timeout::timeout()) ->
          {ok, Rows::list(ejson_object())} |
          {error, term()} |
          {error, term(), Rows::list(ejson_object())}.
collect_view_results(Ref, Acc, Timeout) ->
    receive
        {Ref, done} ->
            Rows = lists:reverse(Acc),
            {ok, Rows};
        {Ref, {row, Row}} ->
            collect_view_results(Ref, [Row|Acc], Timeout);
        {Ref, {error, Error}}
          when Acc =:= []->
            {error, Error};
        {Ref, {error, Error}} ->
            %% in case we got some results
            Rows = lists:reverse(Acc),
            {error, Error, Rows}
    after Timeout ->
            {error, timeout}
    end.

view_request(#db{options=Opts}, Url, #view_query_args{method=get}) ->
    couchbeam_httpc:db_request(get, Url, [], <<>>,
                               Opts, [200]);
view_request(#db{options=Opts}, Url, #view_query_args{method=post, keys=Keys}) ->
    Body = couchbeam_ejson:encode(
             {[{<<"keys">>, Keys}]}
            ),

    Hdrs = [{<<"Content-Type">>, <<"application/json">>}],
    couchbeam_httpc:db_request(post, Url, Hdrs, Body,
                               Opts, [200]).

with_view_stream(Ref, Fun) ->
    case ets:lookup(couchbeam_view_streams, Ref) of
        [] ->
            {error, stream_undefined};
        [{Ref, Pid}] ->
            %% Check if the process is still alive to avoid race conditions
            case is_process_alive(Pid) of
                true ->
                    Fun(Pid);
                false ->
                    %% Clean up the stale entry
                    ets:delete(couchbeam_view_streams, Ref),
                    {error, stream_undefined}
            end
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


clean_dbs() ->
    Server = couchbeam:server_connection(),
    catch couchbeam:delete_db(Server, "couchbeam_testdb"),
    ok.

start_couchbeam_tests() ->
    {ok, _} = application:ensure_all_started(couchbeam),
    clean_dbs().


basic_test() ->
    start_couchbeam_tests(),
    Server = couchbeam:server_connection(),

    {ok, Db} = couchbeam:create_db(Server, "couchbeam_testdb"),

    DesignDoc = {[
                  {<<"_id">>, <<"_design/couchbeam">>},
                  {<<"language">>,<<"javascript">>},
                  {<<"views">>,
                   {[{<<"test">>,
                      {[{<<"map">>,
                         <<"function (doc) {\n if (doc.type == \"test\") {\n emit(doc._id, doc);\n}\n}">>
                        }]}
                     },{<<"test2">>,
                        {[{<<"map">>,
                           <<"function (doc) {\n if (doc.type == \"test2\") {\n emit(doc._id, null);\n}\n}">>
                          }]}
                       }]}
                  }
                 ]},

    Doc = {[
            {<<"type">>, <<"test">>}
           ]},

    couchbeam:save_docs(Db, [DesignDoc, Doc, Doc]),
    couchbeam:ensure_full_commit(Db),

    {ok, AllDocs} = couchbeam_view:fetch(Db),
    ?assertEqual(3, length(AllDocs)),

    {ok, Rst2} = couchbeam_view:fetch(Db, {"couchbeam", "test"}),
    ?assertEqual(2, length(Rst2)),

    Count = couchbeam_view:count(Db, {"couchbeam", "test"}),
    ?assertEqual(2, Count),

    {ok, {FirstRow}} = couchbeam_view:first(Db, {"couchbeam", "test"},  [include_docs]),
    {Doc1} = proplists:get_value(<<"doc">>, FirstRow),
    ?assertEqual(<<"test">>, proplists:get_value(<<"type">>, Doc1)),

    Docs = [
            {[{<<"_id">>, <<"test1">>}, {<<"type">>, <<"test">>}, {<<"value">>, 1}]},
            {[{<<"_id">>, <<"test2">>}, {<<"type">>, <<"test">>}, {<<"value">>, 2}]},
            {[{<<"_id">>, <<"test3">>}, {<<"type">>, <<"test">>}, {<<"value">>, 3}]},
            {[{<<"_id">>, <<"test4">>}, {<<"type">>, <<"test">>}, {<<"value">>, 4}]}
           ],

    couchbeam:save_docs(Db, Docs),
    couchbeam:ensure_full_commit(Db),

    {ok, Rst3} = couchbeam_view:fetch(Db, {"couchbeam", "test"}, [{start_key, <<"test">>}]),
    ?assertEqual(4, length(Rst3)),

    {ok, Rst4} = couchbeam_view:fetch(Db, {"couchbeam", "test"}, [{start_key, <<"test">>}, {end_key, <<"test3">>}]),
    ?assertEqual(3, length(Rst4)),

    AccFun = fun(Row, Acc) -> [Row | Acc] end,
    Rst5 = couchbeam_view:fold(AccFun, [], Db, {"couchbeam", "test"}, [{start_key, <<"test">>},   {end_key,<<"test3">>}]),
    ?assertEqual(3, length(Rst5)).

view_notfound_test() ->
    start_couchbeam_tests(),
    Server = couchbeam:server_connection(),

    {ok, Db} = couchbeam:create_db(Server, "couchbeam_testdb"),

    {error, not_found} = couchbeam_view:fetch(Db, {"couchbeam", "test"}, []),
    ok.


-endif.

-spec fetch_bounded_v2(db(), 'all_docs' | {binary(), binary()}, list(),
                       couchbeam_receipt:spec()) -> couchbeam_receipt:result().
fetch_bounded_v2(Db, View, Options, Spec) ->
    couchbeam_receipt:call(fun(B) -> fetch_bounded(Db, View, Options, B) end,
                           Spec, 'stream').
