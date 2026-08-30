%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_view).
-author('Benoît Chesneau <benoitc@e-engura.org>').

-include("couchbeam.hrl").

-export([stream/2, stream/3,
         cancel_stream/1, stream_next/1,
         fetch/1, fetch/2, fetch/3,
         fetch_bounded/4,
         count/1, count/2, count/3,
         first/1, first/2, first/3,
         all/1, all/2,
         fold/4, fold/5,
         foreach/3, foreach/4,
         parse_view_options/1]).

-spec all(Db::db()) -> {ok, Rows::list(ejson_object())} | {error, term()}.
%% @doc fetch all docs
%% @equiv fetch(Db, 'all_docs', [])
all(Db) ->
    fetch(Db, 'all_docs', []).

-spec all(Db::db(), Options::view_options())
        -> {ok, Rows::list(ejson_object())} | {error, term()}.
%% @doc fetch all docs
%% @equiv fetch(Db, 'all_docs', Options)
all(Db, Options) ->
    fetch(Db, 'all_docs', Options).

-spec fetch(Db::db()) -> {ok, Rows::list(ejson_object())} | {error, term()}.
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
%%    | {keys, list(binary())}</pre>
%% <p>See {@link couchbeam_view:stream/4} for more information about
%% options.</p>
%% <p>Return: {ok, Rows} or {error, Error}</p>
fetch(Db, ViewName, Options) ->
    case stream(Db, ViewName, Options) of
        {ok, Ref} ->
            collect_view_results(Ref, []);
        Error ->
            Error
    end.

-spec fetch_bounded(db(), 'all_docs' | {binary(), binary()}, list(),
                    couchbeam_httpc:request_budget_spec()) ->
          {'ok', [ejson_object()], non_neg_integer()} | {'error', term()}.
fetch_bounded(Db, ViewName, Options, BudgetSpec) ->
    case couchbeam_httpc:new_request_budget(BudgetSpec) of
        {'ok', Budget} ->
            FetchOptions = bounded_fetch_options(Options),
            case stream_with_budget(Db, ViewName, FetchOptions, Budget) of
                {'ok', Ref, StreamPid} ->
                    MonitorRef = erlang:monitor('process', StreamPid),
                    bounded_view_result(
                      collect_bounded_view_results(
                        Ref, StreamPid, MonitorRef, Budget, [], 'undefined'));
                {'error', _}=Error ->
                    Error
            end;
        {'error', _}=Error ->
            Error
    end.

-spec bounded_fetch_options(list()) -> list().
bounded_fetch_options(Options) ->
    proplists:delete('async', proplists:delete('stream_to', Options)).

-spec bounded_view_result({'ok', [ejson_object()], non_neg_integer()} |
                          {'error', term()} |
                          {'error', term(), [ejson_object()]}) ->
          {'ok', [ejson_object()], non_neg_integer()} | {'error', term()}.
bounded_view_result({'error', Error, _Rows}) ->
    {'error', Error};
bounded_view_result(Result) ->
    Result.

-spec collect_bounded_view_results(reference(), pid(), reference(),
                                   couchbeam_httpc:request_budget(),
                                   [ejson_object()],
                                   'undefined' | pid() | 'complete' |
                                   {'pending', couchbeam_httpc:request_budget()}) ->
          {'ok', [ejson_object()], non_neg_integer()} |
          {'error', term()} | {'error', term(), [ejson_object()]}.
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
                {Ref, {'error', Error}} when Acc =:= [] ->
                    bounded_view_terminal_after_cleanup(
                      Ref, StreamPid, MonitorRef, GuardianPid, Budget,
                      {'error', Error});
                {Ref, {'error', Error}} ->
                    bounded_view_terminal_after_cleanup(
                      Ref, StreamPid, MonitorRef, GuardianPid, Budget,
                      {'error', Error, lists:reverse(Acc)});
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
await_stream_transport_cleanup(StreamPid, 'undefined', Budget) ->
    case bounded_remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'bounded_transport_guardian', StreamPid, GuardianPid} ->
                    await_stream_transport_cleanup(
                      StreamPid, GuardianPid, Budget);
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
    end;
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

-spec await_stream_transport_cleanup_pending(
        pid(), couchbeam_httpc:request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
await_stream_transport_cleanup_pending(StreamPid, Budget) ->
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
await_bounded_view_cancel(Ref, _StreamPid, MonitorRef, 'complete', _Budget,
                          {'error', _}=Result) ->
    bounded_view_terminal(Ref, MonitorRef, Result);
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
                {Ref, {'error', Error}} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, GuardianState, Budget,
                      {'error', Error});
                {'DOWN', MonitorRef, 'process', StreamPid, _Reason} ->
                    await_bounded_view_cancel(
                      Ref, StreamPid, MonitorRef, GuardianState, Budget,
                      {'error', 'timeout'})
            after TimeoutMs ->
                    bounded_view_terminal(
                      Ref, MonitorRef,
                      {'error', 'transport_cleanup_timeout'})
            end;
        _ ->
            bounded_view_terminal(
              Ref, MonitorRef, {'error', 'transport_cleanup_timeout'})
    end.

-spec bounded_view_terminal_after_cleanup(
        reference(), pid(), reference(),
        'undefined' | pid() | 'complete' |
        {'pending', couchbeam_httpc:request_budget()},
        couchbeam_httpc:request_budget(), term()) -> term().
bounded_view_terminal_after_cleanup(Ref, StreamPid, MonitorRef, GuardianState,
                                    Budget, Result) ->
    CleanupBudget = bounded_cleanup_budget(Budget),
    case await_stream_transport_cleanup(
           StreamPid, GuardianState, CleanupBudget) of
        'ok' -> bounded_view_terminal(Ref, MonitorRef, Result);
        {'error', 'transport_cleanup_timeout'}=Error ->
            bounded_view_terminal(Ref, MonitorRef, Error)
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
bounded_cleanup_wait_budget(_CleanupState, FallbackBudget) ->
    FallbackBudget.

-spec bounded_cleanup_delivery_budget(couchbeam_httpc:request_budget()) ->
          couchbeam_httpc:request_budget().
bounded_cleanup_delivery_budget(#{'deadline_ms' := CleanupDeadline,
                                  'timeout_ms' := TimeoutMs}=Budget) ->
    Budget#{'deadline_ms' => CleanupDeadline + TimeoutMs}.

-spec bounded_view_terminal(reference(), reference(), term()) -> term().
bounded_view_terminal(Ref, MonitorRef, Result) ->
    erlang:demonitor(MonitorRef, ['flush']),
    flush_view_messages(Ref),
    Result.

-spec flush_view_messages(reference()) -> 'ok'.
flush_view_messages(Ref) ->
    receive
        {Ref, _Message} -> flush_view_messages(Ref)
    after 0 ->
            'ok'
    end.

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
%%          <dd>Got an error, connection is closed when an error
%%          happend.</dd>
%%  </dl></p>
%%  <p><pre>Options :: view_options() [{key, binary()}
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
stream_with_budget(Db, ViewName, Options, Budget) ->
    {To, Options1} = case proplists:get_value(stream_to, Options) of
                         undefined ->
                             {self(), Options};
                         StreamOwner ->
                             {StreamOwner, proplists:delete(stream_to, Options)}
                     end,
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

-spec stream_options(list(),
                     'undefined' | couchbeam_httpc:request_budget()) -> list().
stream_options(Options, 'undefined') ->
    Options;
stream_options(Options, Budget) ->
    [{'request_budget', Budget} | Options].


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
%% @equiv first(Db, 'all_docs', [])
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



collect_view_results(Ref, Acc) ->
    receive
        {Ref, done} ->
            Rows = lists:reverse(Acc),
            {ok, Rows};
        {Ref, {row, Row}} ->
            collect_view_results(Ref, [Row|Acc]);
        {Ref, {error, Error}}
          when Acc =:= []->
            {error, Error};
        {Ref, {error, Error}} ->
            %% in case we got some results
            Rows = lists:reverse(Acc),
            {error, Error, Rows}
    after 10000 ->
              {error, timeout}
    end.

view_request(#db{options=Opts}, Url, Args) ->
    case Args#view_query_args.method of
        get ->
            couchbeam_httpc:db_request(get, Url, [], <<>>,
                                       Opts, [200]);
        post ->
            Body = couchbeam_ejson:encode(
                    {[{<<"keys">>, Args#view_query_args.keys}]}
                    ),

            Hdrs = [{<<"Content-Type">>, <<"application/json">>}],
            couchbeam_httpc:db_request(post, Url, Hdrs, Body,
                                       Opts, [200])
    end.

with_view_stream(Ref, Fun) ->
    case ets:lookup(couchbeam_view_streams, Ref) of
        [] ->
            {error, stream_undefined};
        [{Ref, Pid}] ->
            Fun(Pid)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").


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
