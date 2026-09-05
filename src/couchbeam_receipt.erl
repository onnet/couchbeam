%%% Raw payload accounting for one bounded transport attempt.
-module(couchbeam_receipt).
-export([call/3, dispatch/1, add_bytes/2, complete/1]).
-export_type([spec/0, receipt/0, result/0]).
-type spec() :: {'bounded_reader', 2, integer(), pos_integer()}.
-type receipt() :: #{'version' := 2, 'dispatch_count' := 0 | 1,
                     'body_bytes' := non_neg_integer(),
                     'certainty' := 'known' | 'unknown'}.
-type result() :: {'ok', term(), receipt()} | {'error', term(), receipt()}.

%% The counters belong to this attempt, including its upload and VIEW workers.
%% Bytes mean payload binaries delivered to the raw consumer, not socket bytes.
%% Certainty describes accounting, never whether a remote write committed.
-spec call(fun((term()) -> term()), spec(), 'direct' | 'stream') -> result().
call(Fun, {'bounded_reader', 2, Deadline, Cap}, Mode)
  when is_integer(Deadline), is_integer(Cap), Cap > 0 ->
    Remaining = Deadline - erlang:monotonic_time('millisecond'),
    case Remaining > 0 andalso Remaining =< (16#FFFFFFFF div 3) of
        'true' ->
            Counters = atomics:new(3, [{'signed', 'false'}]),
            Budget = #{'deadline_ms' => Deadline, 'timeout_ms' => Remaining,
                       'max_response_bytes' => Cap, 'receipt' => Counters},
            run(Fun, Budget, Mode);
        'false' when Remaining =< 0 ->
            {'error', 'timeout', empty()};
        'false' -> {'error', 'invalid_request_budget', empty()}
    end;
call(_Fun, _Spec, _Mode) ->
    {'error', 'invalid_request_budget', empty()}.

-spec run(fun((term()) -> term()), map(), 'direct' | 'stream') -> result().
run(Fun, #{'deadline_ms' := Deadline, 'timeout_ms' := Timeout}=Budget, Mode) ->
    Parent = self(),
    Token = make_ref(),
    {Worker, Monitor} = spawn_monitor(
                          fun() ->
                                  couchbeam_httpc:guard_ephemeral_worker(Parent, self()),
                                  Parent ! {Token, invoke(Fun, Budget)}
                          end),
    %% Allow the existing transport cleanup protocol to finish after its raw
    %% deadline. This does not extend the deadline used by any I/O or decoder.
    Wait = erlang:max(0, Deadline - erlang:monotonic_time('millisecond')) + 2 * Timeout,
    receive
        {Token, {'outcome', Result}} ->
            receive {'DOWN', Monitor, 'process', Worker, _} -> 'ok' end,
            finish(Result, Budget, Mode);
        {Token, 'exception'} ->
            receive {'DOWN', Monitor, 'process', Worker, _} -> 'ok' end,
            {'error', 'bounded_transport_failure', snapshot(Budget, 'unknown')};
        {'DOWN', Monitor, 'process', Worker, _} ->
            {'error', 'bounded_transport_failure', snapshot(Budget, 'unknown')}
    after Wait ->
            exit(Worker, 'kill'),
            receive {'DOWN', Monitor, 'process', Worker, _} -> 'ok' end,
            receive {Token, _} -> 'ok' after 0 -> 'ok' end,
            {'error', 'timeout', snapshot(Budget, 'unknown')}
    end.

-spec invoke(fun((term()) -> term()), map()) -> {'outcome', term()} | 'exception'.
invoke(Fun, #{'deadline_ms' := Deadline}=Budget) ->
    try
        case Deadline > erlang:monotonic_time('millisecond') of
            'true' -> {'outcome', Fun({'receipt_budget', Budget})};
            'false' ->
                complete(Budget),
                {'outcome', {'error', 'timeout'}}
        end
    catch _:_ -> 'exception'
    end.

-spec finish(term(), map(), 'direct' | 'stream') -> result().
finish(Result, #{'receipt' := Counters}=Budget, Mode) ->
    Preflight = atomics:get(Counters, 1) =:= 0
        andalso Result =/= {'error', 'timeout'},
    Known = (Mode =:= 'direct' orelse Preflight
             orelse atomics:get(Counters, 3) =:= 1)
        andalso not uncertain(Result),
    Certainty = case Known of 'true' -> 'known'; 'false' -> 'unknown' end,
    Receipt = snapshot(Budget, Certainty),
    case Result of
        {'ok', Value, _Bytes} -> {'ok', Value, Receipt};
        {'error', Reason} -> {'error', Reason, Receipt};
        _ -> {'error', 'invalid_transport_result', snapshot(Budget, 'unknown')}
    end.

-spec uncertain(term()) -> boolean().
uncertain({'error', 'transport_cleanup_timeout'}) -> 'true';
uncertain({'error', 'request_guardian_down'}) -> 'true';
uncertain({'error', {'request_worker_down', _}}) -> 'true';
uncertain({'error', {'unexpected_request_result', _}}) -> 'true';
uncertain({'error', {'stream_down', _}}) -> 'true';
uncertain(_) -> 'false'.

%% Called in the upload worker immediately before entering Hackney, after its
%% own deadline check. No retry or second transport entry is permitted.
-spec dispatch(term()) -> 'ok'.
dispatch(#{'receipt' := Counters}) ->
    case atomics:compare_exchange(Counters, 1, 0, 1) of
        'ok' -> 'ok';
        _ -> error('duplicate_transport_dispatch')
    end;
dispatch(_) -> 'ok'.

%% Count the cap-crossing chunk too; the caller must refuse it before decode.
-spec add_bytes(term(), binary()) -> 'ok'.
add_bytes(#{'receipt' := Counters}, Chunk) ->
    atomics:add(Counters, 2, byte_size(Chunk));
add_bytes(_, _Chunk) -> 'ok'.

-spec complete(term()) -> 'ok'.
complete(#{'receipt' := Counters}) -> atomics:put(Counters, 3, 1);
complete(_) -> 'ok'.

-spec snapshot(map(), 'known' | 'unknown') -> receipt().
snapshot(#{'receipt' := Counters}, Certainty) ->
    Bytes = atomics:get(Counters, 2),
    Dispatch = atomics:get(Counters, 1),
    #{'version' => 2, 'dispatch_count' => Dispatch,
      'body_bytes' => Bytes, 'certainty' => Certainty}.

-spec empty() -> receipt().
empty() ->
    #{'version' => 2, 'dispatch_count' => 0,
      'body_bytes' => 0, 'certainty' => 'known'}.
