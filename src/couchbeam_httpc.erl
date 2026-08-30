%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_httpc).

-export([request/5, request_bounded/6,
         db_request/5, db_request/6,
         json_body/1,
         bounded_json_body/2,
         bounded_encode_json/2, bounded_encode_json/3,
         new_request_budget/1,
         cancel_request/1,
         db_resp/2,
         make_headers/4,
         maybe_oauth_header/4]).
-export_type([request_budget_spec/0, request_budget/0]).

-ifdef(TEST).
-export([decode_bounded_json/3]).
-endif.
%% urls utils
-export([server_url/1, db_url/1, doc_url/2]).
%% atts utols
-export([reply_att/1, wait_mp_doc/2, len_doc_to_mp_stream/3, send_mp_doc/5]).

-include("couchbeam.hrl").

-type request_budget_spec() :: {pos_integer(), pos_integer()}.
-type request_budget() :: #{'deadline_ms' := integer(),
                            'timeout_ms' := pos_integer(),
                            'max_response_bytes' := pos_integer()}.

request(Method, Url, Headers, Body, Options) ->
    {FinalHeaders, FinalOpts0} = make_headers(Method, Url, Headers,
                                              Options),
    case request_options(FinalOpts0) of
        {'ok', FinalOpts} ->
            hackney:request(Method, Url , FinalHeaders, Body, FinalOpts);
        {'error', _}=Error ->
            Error
    end.

db_request(Method, Url, Headers, Body, Options) ->
    db_request(Method, Url, Headers, Body, Options, []).

db_request(Method, Url, Headers, Body, Options, Expect) ->
    case couchbeam_util:get_value('request_budget', Options) of
        #{'deadline_ms' := _, 'max_response_bytes' := _}=Budget ->
            Resp = bounded_request(Method, Url, Headers, Body,
                                   Options, Budget),
            db_resp_bounded(Resp, Expect, Budget);
        _ ->
            db_resp(request(Method, Url, Headers, Body, Options), Expect)
    end.

json_body(Ref) ->
    {ok, Body} = hackney:body(Ref),
    couchbeam_ejson:decode(Body).

-spec new_request_budget(request_budget_spec()) ->
          {'ok', request_budget()} | {'error', 'invalid_request_budget'}.
new_request_budget({TimeoutMs, MaxResponseBytes})
  when is_integer(TimeoutMs), TimeoutMs > 0,
       is_integer(MaxResponseBytes), MaxResponseBytes > 0 ->
    {'ok', #{'deadline_ms' => erlang:monotonic_time('millisecond') + TimeoutMs,
             'timeout_ms' => TimeoutMs,
             'max_response_bytes' => MaxResponseBytes}};
new_request_budget(_) ->
    {'error', 'invalid_request_budget'}.

-spec bounded_json_body(reference(), request_budget()) ->
          {'ok', term(), non_neg_integer()} |
          {'error', 'timeout' | 'response_too_large' |
                    'invalid_request_budget' | term()}.
bounded_json_body(Ref, #{'deadline_ms' := _,
                         'max_response_bytes' := _}=Budget) ->
    case bounded_binary_body(Ref, Budget) of
        {'ok', Body, Bytes} -> decode_bounded_json(Body, Bytes, Budget);
        {'error', _}=Error -> Error
    end;
bounded_json_body(Ref, _) ->
    close_request(Ref),
    {'error', 'invalid_request_budget'}.

-spec bounded_encode_json(term(), request_budget()) ->
          {'ok', binary()} | {'error', 'timeout' | term()}.
bounded_encode_json(Term, Budget) ->
    bounded_encode_json(Term, Budget, []).

-spec bounded_encode_json(term(), request_budget(), list()) ->
          {'ok', binary()} | {'error', 'timeout' | term()}.
bounded_encode_json(Term, Budget, Options) ->
    Delay = encode_test_delay(Options),
    run_bounded_worker(
      fun() ->
              maybe_delay_encode(Delay),
              couchbeam_ejson:encode(Term)
      end,
      fun(Encoded) when is_binary(Encoded) -> {'ok', Encoded};
         (Unexpected) -> {'error', {'invalid_json_encoding', Unexpected}}
      end,
      Budget).

-spec run_bounded_worker(fun(() -> term()), fun((term()) -> term()),
                         request_budget()) -> term().
run_bounded_worker(WorkFun, ResultFun, Budget) ->
    Parent = self(),
    Token = make_ref(),
    {WorkerPid, MonitorRef} = spawn_monitor(
                                fun() -> Parent ! {Token, WorkFun()} end),
    guard_request_worker(Parent, WorkerPid),
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
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
            end;
        _ ->
            exit(WorkerPid, 'kill'),
            _ = await_worker_down(
                  WorkerPid, MonitorRef, cleanup_deadline(Budget)),
            flush_request_result(Token),
            {'error', 'timeout'}
    end.

-spec cancel_request(reference()) -> 'ok'.
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
                            close_request(Ref),
                            {'error', 'timeout'}
                    end;
                {'error', Reason} ->
                    close_request(Ref),
                    {'error', Reason}
            end;
        _ ->
            close_request(Ref),
            {'error', 'timeout'}
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
                    close_request(Ref),
                    {'error', 'response_too_large'}
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
    close_request(Ref),
    {'error', Reason};
bounded_body_control(Unexpected, Ref, _Budget, _Acc, _Bytes) ->
    close_request(Ref),
    {'error', {'unexpected_response_message', Unexpected}}.

-spec bounded_body_done(reference(), request_budget(), [binary()],
                        non_neg_integer()) ->
          {'ok', binary(), non_neg_integer()} | {'error', 'timeout'}.
bounded_body_done(Ref, Budget, Acc, Bytes) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            close_request(Ref),
            {'ok', iolist_to_binary(lists:reverse(Acc)), Bytes};
        _ ->
            close_request(Ref),
            {'error', 'timeout'}
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
    {DecoderPid, MonitorRef} = spawn_monitor(
                                 fun() ->
                                         Parent ! {Token,
                                                   decode_json_result(Body)}
                                 end),
    WatchdogMs = erlang:max(0, remaining_timeout(Budget)),
    receive
        {Token, {'ok', Json}} ->
            erlang:demonitor(MonitorRef, ['flush']),
            guarded_decode_result({'ok', Json}, Bytes, Budget);
        {Token, {'error', _}=Error} ->
            erlang:demonitor(MonitorRef, ['flush']),
            guarded_decode_result(Error, Bytes, Budget);
        {'DOWN', MonitorRef, 'process', DecoderPid, Reason} ->
            flush_decode_result(Token),
            {'error', {'invalid_json', Reason}}
    after WatchdogMs ->
            exit(DecoderPid, 'kill'),
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
                      request_budget()) -> term().
bounded_request(Method, Url, Headers, Body, Options, Budget) ->
    case request_bounded(Method, Url, Headers, Body, Options, Budget) of
        {'ok', Ref} -> bounded_response_status(Ref, Budget);
        Error -> Error
    end.

-spec request_bounded(term(), term(), list(), term(), list(), request_budget()) ->
          {'ok', reference()} | {'error', term()}.
request_bounded(Method, Url, Headers, Body, Options, Budget) ->
    Parent = self(),
    Token = make_ref(),
    LeasePid = spawn(fun transport_lease/0),
    HandoffHook = handoff_test_hook(Options),
    UploadHook = upload_test_hook(Options),
    RequestOptions = [{'async', 'once'}, {'stream_to', Parent}
                      | proplists:delete(
                          'async', proplists:delete(
                                     'stream_to', strip_test_options(Options)))],
    {WorkerPid, MonitorRef} = spawn_monitor(
                                fun() ->
                                        Result = request(Method, Url, Headers,
                                                         Body, RequestOptions),
                                        Parent ! {Token, Result},
                                        receive
                                            {Token, 'release'} -> 'ok'
                                        end
                                end),
    notify_upload_worker(UploadHook, WorkerPid),
    guard_request_worker(Parent, WorkerPid),
    guard_transport_lease(Parent, LeasePid),
    await_bounded_request(Token, WorkerPid, MonitorRef, LeasePid,
                          HandoffHook, Budget).

-spec transport_lease() -> no_return().
transport_lease() ->
    receive
        'stop' -> exit('normal')
    end.

-spec guard_request_worker(pid(), pid()) -> 'ok'.
guard_request_worker(Parent, WorkerPid) ->
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

-spec guard_transport_lease(pid(), pid()) -> 'ok'.
guard_transport_lease(Parent, LeasePid) ->
    _ = spawn(fun() ->
                      ParentRef = erlang:monitor('process', Parent),
                      LeaseRef = erlang:monitor('process', LeasePid),
                      receive
                          {'DOWN', ParentRef, 'process', Parent, _Reason} ->
                              exit(LeasePid, 'kill');
                          {'DOWN', LeaseRef, 'process', LeasePid, _Reason} ->
                              'ok'
                      end
              end),
    'ok'.

-spec await_bounded_request(reference(), pid(), reference(), pid(), term(),
                            request_budget()) ->
          {'ok', reference()} | {'error', term()}.
await_bounded_request(Token, WorkerPid, MonitorRef, LeasePid, HandoffHook,
                      Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {Token, {'ok', Ref}} when is_reference(Ref) ->
                    adopt_bounded_request(
                      Token, WorkerPid, MonitorRef, LeasePid, Ref,
                      HandoffHook, Budget);
                {Token, {'error', _}=Error} ->
                    exit(LeasePid, 'kill'),
                    release_request_worker(Token, WorkerPid, MonitorRef,
                                           Budget),
                    Error;
                {Token, Unexpected} ->
                    exit(LeasePid, 'kill'),
                    release_request_worker(Token, WorkerPid, MonitorRef,
                                           Budget),
                    {'error', {'unexpected_request_result', Unexpected}};
                {'DOWN', MonitorRef, 'process', WorkerPid, Reason} ->
                    exit(LeasePid, 'kill'),
                    flush_request_result(Token),
                    require_owner_cleanup(WorkerPid, Budget),
                    {'error', {'request_worker_down', Reason}}
            after TimeoutMs ->
                    exit(LeasePid, 'kill'),
                    abort_request_worker(Token, WorkerPid, MonitorRef, Budget),
                    {'error', 'timeout'}
            end;
        _ ->
            exit(LeasePid, 'kill'),
            abort_request_worker(Token, WorkerPid, MonitorRef, Budget),
            {'error', 'timeout'}
    end.

-spec adopt_bounded_request(reference(), pid(), reference(), pid(), reference(),
                            term(), request_budget()) ->
          {'ok', reference()} | {'error', term()}.
adopt_bounded_request(Token, WorkerPid, MonitorRef, LeasePid, Ref,
                      HandoffHook, Budget) ->
    case before_handoff(HandoffHook, Ref, Budget) of
        'ok' -> adopt_bounded_request_now(
                  Token, WorkerPid, MonitorRef, LeasePid, Ref, Budget);
        {'error', 'timeout'} ->
            cleanup_failed_handoff(
              Token, WorkerPid, MonitorRef, LeasePid, Ref, Budget),
            {'error', 'timeout'}
    end.

-spec adopt_bounded_request_now(reference(), pid(), reference(), pid(),
                                reference(), request_budget()) ->
          {'ok', reference()} | {'error', term()}.
adopt_bounded_request_now(Token, WorkerPid, MonitorRef, LeasePid, Ref,
                          Budget) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            %% Hackney 1.6 transfers the socket to its async stream process,
            %% while the manager continues to track the request owner. Change
            %% that tracked owner before the temporary upload owner exits.
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
                    remember_owned_request(Ref, LeasePid, Budget),
                    release_request_worker(Token, WorkerPid, MonitorRef,
                                           Budget),
                    case budget_status(Budget) of
                        'ok' -> {'ok', Ref};
                        {'error', 'timeout'} ->
                            close_request(Ref),
                            {'error', 'timeout'}
                    end;
                {'error', 'timeout'} ->
                    cleanup_failed_handoff(
                      Token, WorkerPid, MonitorRef, LeasePid, Ref, Budget),
                    {'error', 'timeout'};
                OwnershipError ->
                    cleanup_failed_handoff(
                      Token, WorkerPid, MonitorRef, LeasePid, Ref, Budget),
                    {'error', {'request_ownership', OwnershipError}}
            end;
        _ ->
            cleanup_failed_handoff(
              Token, WorkerPid, MonitorRef, LeasePid, Ref, Budget),
            {'error', 'timeout'}
    end.

-spec cleanup_failed_handoff(reference(), pid(), reference(), pid(),
                             reference(), request_budget()) -> 'ok'.
cleanup_failed_handoff(Token, WorkerPid, MonitorRef, LeasePid, Ref, Budget) ->
    exit(LeasePid, 'kill'),
    stop_request_worker(Token, WorkerPid, MonitorRef, Budget),
    CleanupBudget = cleanup_deadline(Budget),
    require_ownership_barrier(Ref, LeasePid, CleanupBudget),
    require_request_cleanup(Ref, CleanupBudget),
    flush_response_messages(Ref).

-spec require_ownership_barrier(reference(), pid(), request_budget()) -> 'ok'.
require_ownership_barrier(Ref, LeasePid, Budget) ->
    case ownership_barrier(Ref, LeasePid, Budget) of
        {'error', 'timeout'} -> exit({'transport_cleanup_timeout', Ref});
        _ -> 'ok'
    end.

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
                             request_budget()) -> 'ok'.
release_request_worker(Token, WorkerPid, MonitorRef, Budget) ->
    WorkerPid ! {Token, 'release'},
    require_worker_down(WorkerPid, MonitorRef, Budget),
    require_owner_cleanup(WorkerPid, Budget),
    'ok'.

-spec abort_request_worker(reference(), pid(), reference(), request_budget()) ->
          'ok'.
abort_request_worker(Token, WorkerPid, MonitorRef, Budget) ->
    stop_request_worker(Token, WorkerPid, MonitorRef, Budget),
    require_owner_cleanup(WorkerPid, Budget),
    'ok'.

-spec stop_request_worker(reference(), pid(), reference(), request_budget()) ->
          'ok'.
stop_request_worker(Token, WorkerPid, MonitorRef, Budget) ->
    exit(WorkerPid, 'kill'),
    require_worker_down(WorkerPid, MonitorRef, Budget),
    flush_request_result(Token),
    'ok'.

-spec require_worker_down(pid(), reference(), request_budget()) -> 'ok'.
require_worker_down(WorkerPid, MonitorRef, Budget) ->
    case await_worker_down(
           WorkerPid, MonitorRef, cleanup_deadline(Budget)) of
        'ok' -> 'ok';
        {'error', 'timeout'} ->
            exit({'transport_cleanup_timeout', WorkerPid})
    end.

-spec require_owner_cleanup(pid(), request_budget()) -> 'ok'.
require_owner_cleanup(WorkerPid, Budget) ->
    case await_owned_transport_cleanup(
           WorkerPid, cleanup_deadline(Budget)) of
        'ok' -> 'ok';
        {'error', 'timeout'} ->
            exit({'transport_cleanup_timeout', WorkerPid})
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

-spec flush_request_result(reference()) -> 'ok'.
flush_request_result(Token) ->
    receive
        {Token, _} -> flush_request_result(Token)
    after 0 ->
            'ok'
    end.

-spec await_owned_transport_cleanup(pid(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
await_owned_transport_cleanup(OwnerPid, Budget) ->
    case catch ets:match_object(
                 'hackney_manager_refs', {'_', {OwnerPid, '_', '_'}}) of
        [] ->
            'ok';
        {'EXIT', {'badarg', _}} ->
            'ok';
        [_ | _] ->
            case remaining_timeout(Budget) of
                Remaining when Remaining > 0 ->
                    erlang:yield(),
                    await_owned_transport_cleanup(OwnerPid, Budget);
                _ ->
                    {'error', 'timeout'}
            end
    end.

-spec await_request_cleanup(reference(), request_budget()) ->
          'ok' | {'error', 'timeout'}.
await_request_cleanup(Ref, Budget) ->
    case catch ets:lookup('hackney_manager_refs', Ref) of
        [] -> 'ok';
        {'EXIT', {'badarg', _}} -> 'ok';
        [_] ->
            case remaining_timeout(Budget) of
                Remaining when Remaining > 0 ->
                    erlang:yield(),
                    await_request_cleanup(Ref, Budget);
                _ -> {'error', 'timeout'}
            end
    end.

-spec require_request_cleanup(reference(), request_budget()) -> 'ok'.
require_request_cleanup(Ref, Budget) ->
    case await_request_cleanup(Ref, Budget) of
        'ok' -> 'ok';
        {'error', 'timeout'} -> exit({'transport_cleanup_timeout', Ref})
    end.

-spec cleanup_deadline(request_budget()) -> request_budget().
cleanup_deadline(#{'timeout_ms' := TimeoutMs}=Budget) ->
    Budget#{'deadline_ms' =>
                erlang:monotonic_time('millisecond') + TimeoutMs}.

-spec remember_owned_request(reference(), pid(), request_budget()) -> term().
remember_owned_request(Ref, LeasePid, Budget) ->
    put({'bounded_request_owner', Ref}, {LeasePid, Budget}).

-spec flush_response_messages(reference()) -> 'ok'.
flush_response_messages(Ref) ->
    receive
        {'hackney_response', Ref, _} -> flush_response_messages(Ref)
    after 0 ->
            'ok'
    end.

-ifdef(TEST).
-spec handoff_test_hook(list()) -> 'undefined' | pid().
handoff_test_hook(Options) ->
    proplists:get_value('bounded_handoff_test_hook', Options, 'undefined').

-spec upload_test_hook(list()) -> 'undefined' | pid().
upload_test_hook(Options) ->
    proplists:get_value('bounded_upload_test_hook', Options, 'undefined').

-spec encode_test_delay(list()) -> non_neg_integer().
encode_test_delay(Options) ->
    case proplists:get_value('bounded_encode_test_delay_ms', Options, 0) of
        Delay when is_integer(Delay), Delay >= 0 -> Delay;
        _ -> 0
    end.

-spec strip_test_options(list()) -> list().
strip_test_options(Options) ->
    proplists:delete(
      'bounded_handoff_test_hook',
      proplists:delete(
        'bounded_upload_test_hook',
        proplists:delete('bounded_encode_test_delay_ms', Options))).
-else.
-spec handoff_test_hook(list()) -> 'undefined'.
handoff_test_hook(_Options) ->
    'undefined'.

-spec upload_test_hook(list()) -> 'undefined'.
upload_test_hook(_Options) ->
    'undefined'.

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

-spec maybe_delay_encode(non_neg_integer()) -> 'ok'.
maybe_delay_encode(0) ->
    'ok';
maybe_delay_encode(Delay) ->
    timer:sleep(Delay).

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
                      close_request(Ref),
                      {'error', Reason}
              end;
         ({'error', Reason}) ->
              close_request(Ref),
              {'error', Reason};
         (Unexpected) ->
              close_request(Ref),
              {'error', {'unexpected_response_message', Unexpected}}
      end).

-spec bounded_response_headers(reference(), integer(), request_budget()) ->
          term().
bounded_response_headers(Ref, Status, Budget) ->
    bounded_response_receive(
      Ref, Budget,
      fun({'headers', Headers}) -> {'ok', Status, Headers, Ref};
         ({'error', Reason}) ->
              close_request(Ref),
              {'error', Reason};
         (Unexpected) ->
              close_request(Ref),
              {'error', {'unexpected_response_message', Unexpected}}
      end).

-spec bounded_response_receive(reference(), request_budget(), fun((term()) -> term())) ->
          term().
bounded_response_receive(Ref, Budget, Handler) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            receive
                {'hackney_response', Ref, Message} -> Handler(Message)
            after TimeoutMs ->
                    close_request(Ref),
                    {'error', 'timeout'}
            end;
        _ ->
            close_request(Ref),
            {'error', 'timeout'}
    end.

-spec request_options(list()) -> {'ok', list()} | {'error', term()}.
request_options(Options) ->
    case couchbeam_util:get_value('request_budget', Options) of
        'undefined' ->
            {'ok', Options};
        #{'deadline_ms' := _, 'max_response_bytes' := _}=Budget ->
            case remaining_timeout(Budget) of
                TimeoutMs when TimeoutMs > 0 ->
                    Options1 = proplists:delete('request_budget', Options),
                    {'ok', [{'connect_timeout', TimeoutMs},
                            {'recv_timeout', TimeoutMs}
                            | proplists:delete('connect_timeout',
                                               proplists:delete('recv_timeout',
                                                                Options1))]};
                _ ->
                    {'error', 'timeout'}
            end;
        _ ->
            {'error', 'invalid_request_budget'}
    end.

-spec remaining_timeout(request_budget()) -> integer().
remaining_timeout(#{'deadline_ms' := DeadlineMs}) ->
    DeadlineMs - erlang:monotonic_time('millisecond').

-spec close_request(reference()) -> 'ok'.
close_request(Ref) ->
    case erase({'bounded_request_owner', Ref}) of
        {LeasePid, Budget} ->
            exit(LeasePid, 'kill'),
            require_request_cleanup(Ref, cleanup_deadline(Budget)),
            flush_response_messages(Ref);
        'undefined' ->
            close_unowned_request(Ref)
    end,
    'ok'.

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

-spec db_resp_bounded(term(), [integer()], request_budget()) -> term().
db_resp_bounded({'ok', Ref}=Resp, _Expect, _Budget) when is_reference(Ref) ->
    Resp;
db_resp_bounded({'ok', 401, _}, _Expect, _Budget) ->
    {'error', 'unauthenticated'};
db_resp_bounded({'ok', 403, _}, _Expect, _Budget) ->
    {'error', 'forbidden'};
db_resp_bounded({'ok', 404, _}, _Expect, _Budget) ->
    {'error', 'not_found'};
db_resp_bounded({'ok', 409, _}, _Expect, _Budget) ->
    {'error', 'conflict'};
db_resp_bounded({'ok', 412, _}, _Expect, _Budget) ->
    {'error', 'precondition_failed'};
db_resp_bounded({'ok', _, _}=Resp, [], _Budget) ->
    Resp;
db_resp_bounded({'ok', Status, Headers}=Resp, Expect, _Budget) ->
    case lists:member(Status, Expect) of
        'true' -> Resp;
        'false' -> {'error', {'bad_response', {Status, Headers, <<>>}}}
    end;
db_resp_bounded({'ok', Status, _Headers, Ref}, _Expect, _Budget)
  when Status =:= 401; Status =:= 403; Status =:= 404;
       Status =:= 409; Status =:= 412 ->
    cancel_request(Ref),
    db_resp_bounded_status(Status);
db_resp_bounded({'ok', _, _, _}=Resp, [], _Budget) ->
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
    Headers1 = case couchbeam_util:get_value(<<"Accept">>, Headers) of
        undefined ->
            [{<<"Accept">>, <<"application/json, */*;q=0.9">>} | Headers];
        _ ->
            Headers
    end,
   {Headers2, Options1} = maybe_oauth_header(Method, Url, Headers1, Options),
   maybe_proxyauth_header(Headers2, Options1).


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
        _ -> <<>>
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
  {[{<<"ok">>, true}|R]} = couchbeam_httpc:json_body(Ref),
  {ok, {R}};
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
