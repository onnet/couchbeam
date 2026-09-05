%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_view_stream).

-export([start_link/4]).

-export([init_stream/5,
         maybe_continue/1,
         system_continue/3,
         system_terminate/4,
         system_code_change/4]).


-export([init/1,
         handle_event/2,
         wait_rows/2,
         wait_rows1/2,
         wait_val/2,
         collect_object/2,
         maybe_continue_decoding/1]).


-include("couchbeam.hrl").

-record(state, {parent,
                owner,
                req,
                ref,
                mref,
                client_ref=nil,
                decoder,
                async='normal',
                budget='undefined',
                response_bytes=0,
                decode_test_delay_ms=0}).

-record(viewst, {parent,
                 owner,
                 ref,
                 mref,
                 client_ref,
                 async='false'}).


-define(TIMEOUT, 10000).
-define(DEFAULT_CHANGES_POLL, 5000). % we check every 5secs



start_link(Owner, StreamRef, {Db, Url, Args}, StreamOptions) ->
    proc_lib:start_link(?MODULE, init_stream, [self(), Owner, StreamRef,
                                               {Db, Url, Args},
                                               StreamOptions]).

kz_application(Pid, Options) ->
    case proplists:get_value(kz_application, Options, undefined) of
        undefined -> application:get_application(Pid);
        App -> {ok, App}
    end.

init_stream(Parent, Owner, StreamRef, {_Db, _Url, _Args}=Req,
            StreamOptions) ->

    _ = case kz_application(Owner, StreamOptions) of
            {ok, App} -> erlang:put(kz_application, App);
            _Other -> ok
        end,

    _ = case proplists:get_value(kz_log_id, StreamOptions, undefined) of
            undefined -> ok;
            LogId -> kz_log:put_callid(LogId)
        end,

    Async = proplists:get_value('async', StreamOptions, 'normal'),
    Budget = proplists:get_value('request_budget', StreamOptions, 'undefined'),

    %% monitor the process receiving the messages
    MRef = erlang:monitor(process, Owner),

    %% tell to the parent that we are ok
    proc_lib:init_ack(Parent, {ok, self()}),

    InitState = #state{parent=Parent,
                       owner=Owner,
                       req=Req,
                       ref=StreamRef,
                       mref=MRef,
                       async=Async,
                       budget=Budget,
                       decode_test_delay_ms=decode_test_delay(StreamOptions)},

    %% connect to the view
    try
        case do_init_stream(Req, InitState) of
            {ok, State} ->
                %% register the stream
                ets:insert(couchbeam_view_streams, [{StreamRef, self()}]),
                %% start the loop
                loop(State);
            Error ->
                couchbeam_receipt:complete(Budget),
                report_error(Error, StreamRef, Owner)
        end
    after
        %% Always clean up the monitor reference
        erlang:demonitor(MRef, [flush])
    end,
    ok.

do_init_stream({#db{options=Opts}, Url, Args},
               #state{owner=LifecycleOwner, ref=StreamRef,
                      mref=MRef, budget=Budget}=State) ->
    %% we are doing the request asynchronously
    FinalOpts = [{'async', 'once'} | Opts],
    Reply = case Args#view_query_args.method of
        get ->
            start_view_request(get, Url, [], <<>>, FinalOpts, Budget,
                               LifecycleOwner);
        post ->
            Headers = [{<<"Content-Type">>, <<"application/json">>}],
            start_view_post_request(Url, Headers,
                                    Args#view_query_args.keys,
                                    FinalOpts, Budget, LifecycleOwner)
    end,

    case Reply of
        {ok, Ref} ->
            take_stream_ownership(Ref, Budget),
            Timeout = receive_timeout(State),
            receive
                {'DOWN', MRef, _, _, _} ->
                    %% parent exited there is no need to continue.
                    %% Official verbatim, and correct under a budget too,
                    %% unlike the three other owner-death paths that close
                    %% first (`loop_receive/5', `bounded_loop_receive/4',
                    %% `await_bounded_decode/4'): here the stream owns no
                    %% transport to close. `take_stream_ownership/2' is a
                    %% no-op while a budget is set -- the lease owns the
                    %% socket -- `#state.client_ref' is still `nil' at this
                    %% point, and the guardian monitors both this process and
                    %% the lifecycle owner (`couchbeam_httpc:monitor_request_owners/2'),
                    %% so the cleanup is its work either way.
                    exit(normal);
                {StreamRef, 'budget_timeout'} ->
                    cancel_result(Ref, {'error', 'timeout'}, State);
                {hackney_response, Ref, {status, 200, _}} ->
                    #state{parent=Parent,
                           ref=StreamRef,
                           async=Async} = State,

                    DecoderOwner = decoder_owner(State),
                    DecoderFun = jsx:decoder(?MODULE, [Parent, DecoderOwner,
                                                       StreamRef, MRef, Ref,
                                                       Async], [stream]),
                    {ok, State#state{client_ref=Ref,
                                     decoder=DecoderFun}};

                {hackney_response, Ref, {status, 404, _}} ->
                    cancel_result(Ref, {error, not_found}, State);
                {hackney_response, Ref, {status, Status, Reason}} ->
                    cancel_result(
                      Ref, {error, {http_error, Status, Reason}}, State);
                {hackney_response, Ref, {error, Reason}} ->
                    cancel_result(Ref, {error, Reason}, State)
            after Timeout ->
                    cancel_result(Ref, {'error', 'timeout'}, State)
            end;
        {'error', 'transport_cleanup_timeout'}=Error
          when Budget =/= 'undefined' ->
            %% No lifecycle message here. The stream reports this failure to
            %% its owner itself through `report_error/3', and a second
            %% notification from this process would outlive the collector's
            %% terminal flush and sit in the caller's mailbox.
            Error;
        {'error', _}=Error when Budget =/= 'undefined' ->
            LifecycleOwner ! {'bounded_transport_cleanup', self(),
                              'undefined'},
            Error;
        Error ->
            {error, Error}
    end.

%% Official behaviour, kept verbatim: the legacy stream takes the socket over
%% so a dying owner closes it. The return is `hackney''s transport reply, not
%% always `'ok'', and the spec says so rather than inventing a guarantee.
-spec take_stream_ownership(reference(),
                            'undefined' | couchbeam_httpc:request_budget()) ->
          'ok' | {'error', term()}.
take_stream_ownership(Ref, 'undefined') ->
    Req = hackney:request_info(Ref),
    Mod = proplists:get_value(transport, Req),
    Socket = proplists:get_value(socket, Req),
    Mod:controlling_process(Socket, self());
take_stream_ownership(_Ref, _Budget) ->
    'ok'.



loop(#state{owner=Owner,
            ref=StreamRef,
            mref=MRef,
            client_ref=ClientRef}=State) ->
    case budget_status(State) of
        'ok' ->
            loop_receive(State, Owner, StreamRef, MRef, ClientRef);
        {'error', Reason} ->
            fail_stream(Reason, State)
    end.

-spec loop_receive(#state{}, pid(), reference(), reference(), reference()) ->
          'ok' | no_return().
loop_receive(#state{budget='undefined'}=State, _Owner, _StreamRef, MRef,
             ClientRef) ->
    hackney:stream_next(ClientRef),
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            maybe_close_after_owner_down(State),
            exit(normal);
        {hackney_response, ClientRef, {headers, _Headers}} ->
            loop(State);
        {hackney_response, ClientRef, done} ->
            finish_stream(State);
        {hackney_response, ClientRef, Data} when is_binary(Data) ->
            case add_response_bytes(Data, State) of
                {'ok', State1} -> decode_data(Data, State1);
                {'error', Reason} -> fail_stream(Reason, State)
            end;
        {hackney_response, ClientRef, Error} ->
            fail_or_report_stream(Error, State)
    end;
loop_receive(State, _Owner, StreamRef, MRef, ClientRef) ->
    case hackney:stream_next(ClientRef) of
        'ok' -> bounded_loop_receive(State, StreamRef, MRef, ClientRef);
        {'error', Reason} ->
            %% The document-side twin (`couchbeam_httpc:bounded_body/4') also
            %% reports this reason instead of waiting out the deadline.
            fail_stream(Reason, State)
    end.

-spec bounded_loop_receive(#state{}, reference(), reference(), reference()) ->
          'ok' | no_return().
bounded_loop_receive(State, StreamRef, MRef, ClientRef) ->
    Timeout = receive_timeout(State),
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            maybe_close_after_owner_down(State),
            exit(normal);
        {StreamRef, 'budget_timeout'} ->
            fail_stream('timeout', State);
        {hackney_response, ClientRef, {headers, _Headers}} ->
            loop(State);
        {hackney_response, ClientRef, done} ->
            finish_stream(State);
        {hackney_response, ClientRef, Data} when is_binary(Data) ->
            case add_response_bytes(Data, State) of
                {'ok', State1} -> decode_data(Data, State1);
                {'error', Reason} -> fail_stream(Reason, State)
            end;
        {hackney_response, ClientRef, Error} ->
            fail_or_report_stream(Error, State)
    after Timeout ->
            fail_stream('timeout', State)
    end.

-spec finish_stream(#state{}) -> 'ok' | no_return().
finish_stream(#state{budget='undefined'}=State) ->
    complete_stream(State);
finish_stream(#state{decoder='complete'}=State) ->
    case budget_status(State) of
        'ok' -> complete_stream(State);
        {'error', Reason} -> fail_stream(Reason, State)
    end;
finish_stream(State) ->
    fail_stream({'invalid_json', 'incomplete'}, State).

-spec complete_stream(#state{}) -> 'ok'.
complete_stream(#state{owner=Owner, ref=StreamRef,
                       client_ref=ClientRef, budget=Budget}=State) ->
    CleanupResult = maybe_cancel_request(ClientRef, Budget),
    ets:delete(couchbeam_view_streams, StreamRef),
    couchbeam_receipt:complete(State#state.budget),
    case CleanupResult of
        'ok' ->
            notify_owner_cleanup_proven(State),
            Owner ! done_message(State);
        {'error', 'transport_cleanup_timeout'} ->
            report_error('transport_cleanup_timeout', StreamRef, Owner)
    end,
    'ok'.

%% A proven cleanup is announced to the owner by this process, before the
%% terminal message, whoever proved it. The guardian sends the same
%% lifecycle message when it is the one that proved the cleanup, but a
%% guardian that died after `guardian_started' — killed from outside while
%% the request was in flight — proves nothing and sends nothing, and the
%% cleanup is then established by `couchbeam_httpc:close_request/1' against
%% the manager table (`recover_after_guardian_exit/4'). Without this message
%% the collector, which learnt the guardian's pid from the announcement,
%% would wait a whole cleanup budget for a verdict nobody can send and
%% report `transport_cleanup_timeout' on a cleanup that was proven. With a
%% live guardian the message is a duplicate, and a harmless one: a sender's
%% messages are ordered, so the collector consumes it before the `done' or
%% the error that follows it, and its terminal flush drains the rest.
-spec notify_owner_cleanup_proven(#state{}) -> 'ok'.
notify_owner_cleanup_proven(#state{budget='undefined'}) ->
    'ok';
notify_owner_cleanup_proven(#state{owner=Owner, client_ref=ClientRef}) ->
    Owner ! {'bounded_transport_cleanup', self(), ClientRef},
    'ok'.

decode_data(Data, #state{owner=Owner,
                         ref=StreamRef,
                         client_ref=ClientRef,
                         decoder=DecodeFun,
                         budget='undefined'}=State) ->
    try
        {incomplete, DecodeFun2} = DecodeFun(Data),
        try DecodeFun2('end_stream') of 'done' ->
            %% stop the request
            {ok, _} = hackney:stop_async(ClientRef),
            %% skip the rest of the body so the socket is
            %% replaced in the pool
            catch hackney:skip_body(ClientRef),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, StreamRef),
            %% tell to the owner that we are done and exit,
            Owner ! done_message(State)
        catch 'error':'badarg' ->
            maybe_continue(State#state{decoder=DecodeFun2})
        end
    catch 'error':'badarg' ->
        maybe_close(State),
        exit('badarg')
    end;
decode_data(Data, #state{}=State) ->
    decode_data_bounded(Data, State).

-spec decode_data_bounded(binary(), #state{}) -> 'ok' | no_return().
decode_data_bounded(Data, #state{decoder=Decoder,
                                 decode_test_delay_ms=Delay}=State) ->
    Parent = self(),
    Token = make_ref(),
    {DecoderPid, MonitorRef} = spawn_monitor(
                                 fun() ->
                                         maybe_delay_decode(Delay),
                                         Parent ! {Token,
                                                   bounded_decode_result(
                                                     Data, Decoder)}
                                 end),
    %% Same rule as the two killable workers in `couchbeam_httpc': a decoder
    %% whose stream process died has nobody to answer and must not keep
    %% chewing through a chunk that may be as large as `max_response_bytes'.
    couchbeam_httpc:guard_ephemeral_worker(Parent, DecoderPid),
    await_bounded_decode(Token, DecoderPid, MonitorRef, State).

-spec bounded_decode_result(binary(), 'complete' | fun((term()) -> term())) ->
          {'ok', 'complete' | fun((term()) -> term())} | {'error', term()}.
bounded_decode_result(Data, 'complete') ->
    case only_json_whitespace(Data) of
        'true' -> {'ok', 'complete'};
        'false' -> {'error', {'invalid_json', 'trailing_data'}}
    end;
bounded_decode_result(Data, DecodeFun) ->
    try DecodeFun(Data) of
        {'incomplete', DecodeFun2} ->
            bounded_decoder_result(DecodeFun2);
        Unexpected ->
            {'error', {'malformed_view',
                       {'unexpected_decoder_state', Unexpected}}}
    catch
        'error':'badarg' ->
            {'error', {'invalid_json', 'badarg'}};
        Class:Reason ->
            {'error', {'malformed_view', {Class, Reason}}}
    end.

-spec bounded_decoder_result(fun((term()) -> term())) ->
          {'ok', 'complete' | fun((term()) -> term())} | {'error', term()}.
bounded_decoder_result(DecodeFun) ->
    try DecodeFun('end_stream') of
        'done' ->
            {'ok', 'complete'};
        Unexpected ->
            {'error', {'malformed_view',
                       {'unexpected_decoder_state', Unexpected}}}
    catch
        'error':'badarg' ->
            {'ok', DecodeFun};
        Class:Reason ->
            {'error', {'malformed_view', {Class, Reason}}}
    end.

-spec await_bounded_decode(reference(), pid(), reference(), #state{}) ->
          'ok' | no_return().
await_bounded_decode(Token, DecoderPid, MonitorRef,
                     #state{ref=StreamRef, mref=OwnerRef}=State) ->
    Timeout = receive_timeout(State),
    receive
        {Token, Result} ->
            erlang:demonitor(MonitorRef, ['flush']),
            Rows = take_decoder_rows(StreamRef, []),
            finish_bounded_decode(Result, Rows, State);
        {'DOWN', OwnerRef, 'process', _Owner, _Reason} ->
            %% The owner died while the decoder was busy: the same exit the
            %% between-chunks wait takes (`bounded_loop_receive/4'), without
            %% first waiting for a chunk nobody will read.
            exit(DecoderPid, 'kill'),
            receive
                {'DOWN', MonitorRef, 'process', DecoderPid, _Down} -> 'ok'
            end,
            flush_bounded_decode_result(Token),
            discard_decoder_rows(StreamRef),
            _ = maybe_close_after_owner_down(State),
            exit('normal');
        {'DOWN', MonitorRef, 'process', DecoderPid, Reason} ->
            %% `bounded_decode_result/2' catches its own decoding errors; a
            %% `'DOWN'' is a decoder killed from outside — transient, not a
            %% malformed view. Same label as the document door.
            flush_bounded_decode_result(Token),
            discard_decoder_rows(StreamRef),
            fail_stream({'json_decoding_failed', Reason}, State)
    after Timeout ->
            exit(DecoderPid, 'kill'),
            receive
                {'DOWN', MonitorRef, 'process', DecoderPid, _Reason} -> 'ok'
            end,
            flush_bounded_decode_result(Token),
            discard_decoder_rows(StreamRef),
            fail_stream('timeout', State)
    end.

-spec finish_bounded_decode(
        {'ok', 'complete' | fun((term()) -> term())} | {'error', term()},
        [term()], #state{}) -> 'ok' | no_return().
finish_bounded_decode(Result, Rows, State) ->
    case budget_status(State) of
        {'error', 'timeout'} ->
            fail_stream('timeout', State);
        'ok' ->
            finish_bounded_decode_result(Result, Rows, State)
    end.

-spec finish_bounded_decode_result(
        {'ok', 'complete' | fun((term()) -> term())} | {'error', term()},
        [term()], #state{}) -> 'ok' | no_return().
finish_bounded_decode_result({'error', Reason}, _Rows, State) ->
    fail_stream(Reason, State);
finish_bounded_decode_result({'ok', Decoder}, Rows,
                             #state{owner=Owner}=State) ->
    lists:foreach(fun(Message) -> Owner ! Message end, Rows),
    case Decoder of
        'complete' ->
            %% JSON may finish before the HTTP body. Keep reading raw chunks so
            %% the response byte cap covers trailing bytes as well.
            loop(State#state{decoder='complete'});
        DecodeFun ->
            %% Back to `loop/1', never to `maybe_continue/1': the latter is
            %% the official between-chunks wait for async consumers and has no
            %% clause for the collector's `budget_timeout', which would fall
            %% through to `Else' and be reported as an unexpected message
            %% instead of a timeout. In bounded mode there is no async
            %% consumer to pause, cancel or step.
            loop(State#state{decoder=DecodeFun})
    end.

-spec take_decoder_rows(reference(), [term()]) -> [term()].
take_decoder_rows(StreamRef, Acc) ->
    receive
        {StreamRef, {'row', _}=Row} ->
            take_decoder_rows(StreamRef, [{StreamRef, Row} | Acc])
    after 0 ->
            lists:reverse(Acc)
    end.

-spec discard_decoder_rows(reference()) -> 'ok'.
discard_decoder_rows(StreamRef) ->
    _ = take_decoder_rows(StreamRef, []),
    'ok'.

-spec flush_bounded_decode_result(reference()) -> 'ok'.
flush_bounded_decode_result(Token) ->
    receive
        {Token, _} -> flush_bounded_decode_result(Token)
    after 0 ->
            'ok'
    end.

-spec only_json_whitespace(binary()) -> boolean().
only_json_whitespace(<<>>) ->
    'true';
only_json_whitespace(<<Char, Rest/binary>>)
  when Char =:= $\s; Char =:= $\t; Char =:= $\n; Char =:= $\r ->
    only_json_whitespace(Rest);
only_json_whitespace(_) ->
    'false'.

-spec start_view_request(term(), term(), list(), term(), list(),
                         'undefined' | couchbeam_httpc:request_budget(),
                         pid()) ->
          term().
start_view_request(Method, Url, Headers, Body, Options, 'undefined',
                   _LifecycleOwner) ->
    couchbeam_httpc:request(Method, Url, Headers, Body, Options);
start_view_request(Method, Url, Headers, Body, Options, Budget,
                   LifecycleOwner) ->
    couchbeam_httpc:request_bounded(
      Method, Url, Headers, Body, Options, Budget, LifecycleOwner).

-spec start_view_post_request(term(), list(), list(), list(),
                              'undefined' |
                              couchbeam_httpc:request_budget(), pid()) -> term().
start_view_post_request(Url, Headers, Keys, Options, 'undefined',
                        _LifecycleOwner) ->
    Body = couchbeam_ejson:encode({[{<<"keys">>, Keys}]}),
    couchbeam_httpc:request(post, Url, Headers, Body, Options);
start_view_post_request(Url, Headers, Keys, Options, Budget,
                        LifecycleOwner) ->
    case couchbeam_httpc:bounded_encode_json(
           {[{<<"keys">>, Keys}]}, Budget, Options) of
        {'ok', Body} ->
            start_view_request(post, Url, Headers, Body, Options, Budget,
                               LifecycleOwner);
        {'error', _}=Error ->
            Error
    end.

-spec decoder_owner(#state{}) -> pid().
decoder_owner(#state{owner=Owner, budget='undefined'}) ->
    Owner;
decoder_owner(#state{}) ->
    self().

-ifdef(TEST).
-spec decode_test_delay(list()) -> non_neg_integer().
decode_test_delay(Options) ->
    case proplists:get_value('bounded_decode_test_delay_ms', Options, 0) of
        Delay when is_integer(Delay), Delay >= 0 -> Delay;
        _ -> 0
    end.
-else.
-spec decode_test_delay(list()) -> 0.
decode_test_delay(_Options) ->
    0.
-endif.

-spec maybe_delay_decode(non_neg_integer()) -> 'ok'.
maybe_delay_decode(0) ->
    'ok';
maybe_delay_decode(Delay) ->
    timer:sleep(Delay).

-spec receive_timeout(#state{}) -> non_neg_integer().
receive_timeout(#state{budget='undefined'}) ->
    ?TIMEOUT;
receive_timeout(#state{budget=#{'deadline_ms' := DeadlineMs}}) ->
    erlang:max(0, DeadlineMs - erlang:monotonic_time('millisecond')).

-spec budget_status(#state{}) -> 'ok' | {'error', 'timeout'}.
budget_status(#state{budget='undefined'}) ->
    'ok';
budget_status(#state{budget=#{'deadline_ms' := DeadlineMs}}) ->
    case DeadlineMs > erlang:monotonic_time('millisecond') of
        'true' -> 'ok';
        'false' -> {'error', 'timeout'}
    end.

-spec add_response_bytes(binary(), #state{}) ->
          {'ok', #state{}} |
          {'error', 'timeout' | 'response_too_large'}.
add_response_bytes(_Data, #state{budget='undefined'}=State) ->
    {'ok', State};
add_response_bytes(Data, #state{budget=Budget,
                                response_bytes=Bytes}=State) ->
    couchbeam_receipt:add_bytes(Budget, Data),
    case budget_status(State) of
        {'error', 'timeout'}=Error ->
            Error;
        'ok' ->
            #{'max_response_bytes' := MaxBytes} = Budget,
            NewBytes = Bytes + byte_size(Data),
            case NewBytes =< MaxBytes of
                'true' -> {'ok', State#state{response_bytes=NewBytes}};
                'false' -> {'error', 'response_too_large'}
            end
    end.

-spec fail_stream(term(), #state{}) -> no_return().
fail_stream(Reason, #state{owner=Owner, ref=StreamRef,
                           client_ref=ClientRef}=State) ->
    CleanupResult = couchbeam_httpc:cancel_request(ClientRef),
    ets:delete(couchbeam_view_streams, StreamRef),
    couchbeam_receipt:complete(State#state.budget),
    case CleanupResult of
        'ok' ->
            notify_owner_cleanup_proven(State),
            report_error(Reason, StreamRef, Owner);
        {'error', 'transport_cleanup_timeout'} ->
            report_error('transport_cleanup_timeout', StreamRef, Owner)
    end,
    exit('normal').

-spec fail_or_report_stream(term(), #state{}) -> no_return().
fail_or_report_stream(Error, #state{owner=Owner, ref=StreamRef,
                                    budget='undefined'}) ->
    ets:delete(couchbeam_view_streams, StreamRef),
    report_error(Error, StreamRef, Owner),
    exit(Error);
fail_or_report_stream(Error, State) ->
    fail_stream(Error, State).

-spec maybe_cancel_request(reference(),
                           'undefined' | couchbeam_httpc:request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
maybe_cancel_request(_Ref, 'undefined') ->
    'ok';
maybe_cancel_request(Ref, _Budget) ->
    couchbeam_httpc:cancel_request(Ref).

%% The status-phase twin of `complete_stream/1' and `fail_stream/2': every
%% failure `do_init_stream/2' reports after the request went out closes the
%% transport here, and a proven cleanup is announced to the owner before the
%% result travels back through `report_error/3'. Without the announcement a
%% guardian killed while the stream waits for the status line — its lease
%% gone, the transport torn down under the stream by `hackney_manager', the
%% proof established by `couchbeam_httpc:close_request/1' against the table —
%% would leave the collector, which knows the guardian's pid, waiting a whole
%% cleanup budget for a verdict nobody can send.
-spec cancel_result(reference(), term(), #state{}) -> term().
cancel_result(Ref, Result, #state{budget=Budget}=State) ->
    case maybe_cancel_request(Ref, Budget) of
        'ok' ->
            notify_owner_cleanup_proven(State#state{client_ref=Ref}),
            Result;
        {'error', 'transport_cleanup_timeout'}=Error -> Error
    end.

-spec done_message(#state{}) ->
          {reference(), 'done' | {'done', non_neg_integer()}}.
done_message(#state{ref=Ref, budget='undefined'}) ->
    {Ref, 'done'};
done_message(#state{ref=Ref, response_bytes=Bytes}) ->
    {Ref, {'done', Bytes}}.

maybe_continue(#state{parent=Parent,
                      owner=Owner,
                      ref=Ref,
                      mref=MRef,
                      async=once}=State) ->

    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            maybe_close(State),
            exit(normal);
        {Ref, stream_next} ->
            loop(State);
        {Ref, cancel} ->
            maybe_close(State),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% tell the parent we exited
            Owner ! {Ref, ok};
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [],
                                  {loop, State});
        Else ->
            error_logger:error_msg("Unexpected message: ~w~n", [Else]),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% report the error
            report_error(Else, Ref, Owner),
            exit(Else)
    after 0 ->
            loop(State)
    end;
maybe_continue(#state{parent=Parent,
                      owner=Owner,
                      ref=Ref,
                      mref=MRef}=State) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            maybe_close(State),
            exit(normal);
        {Ref, cancel} ->
            maybe_close(State),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% tell the parent we exited
            Owner ! {Ref, ok};
        {Ref, pause} ->
            erlang:hibernate(?MODULE, maybe_continue, [State]);
        {Ref, resume} ->
            loop(State);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [],
                                  {loop, State});
        Else ->
            error_logger:error_msg("Unexpected message: ~w~n", [Else]),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% report the error
            report_error(Else, Ref, Owner),
            exit(Else)
    after 0 ->
            loop(State)
    end.

maybe_close(#state{client_ref=nil}) ->
    ok;
maybe_close(#state{client_ref=Ref, budget='undefined'}) ->
    hackney:close(Ref);
maybe_close(#state{client_ref=Ref}) ->
    couchbeam_httpc:cancel_request(Ref).

%% The owner is gone, so there is nobody left to receive a verdict; the
%% cleanup result is logged rather than returned, and the spec no longer
%% claims `'ok'' for a call that can report a cleanup timeout.
-spec maybe_close_after_owner_down(#state{}) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
maybe_close_after_owner_down(#state{budget='undefined'}) ->
    'ok';
maybe_close_after_owner_down(#state{ref=StreamRef,
                                    req={_Db, Url, Args}}=State) ->
    case maybe_close(State) of
        'ok' -> 'ok';
        {'error', 'transport_cleanup_timeout'}=Error ->
            #{'method' := Method, 'path' := Path} =
                couchbeam_httpc:request_identity(
                  Args#view_query_args.method, Url),
            logger:warning(
              #{'event' => 'couchbeam_bounded_owner_down_cleanup_unproven',
                'stream_ref' => StreamRef,
                'stream' => self(),
                'method' => Method,
                'path' => Path},
              #{'domain' => ['couchbeam', 'bounded_transport']}),
            Error
    end.


system_continue(_, _, {maybe_continue, State}) ->
    maybe_continue(State);
system_continue(_, _, {loop, State}) ->
    loop(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, #state{ref=StreamRef,
                                      client_ref=ClientRef}) ->
    hackney:close(ClientRef),
    %% unregister the stream
    catch ets:delete(couchbeam_view_streams, StreamRef),
    exit(Reason).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.


%%% json decoder %%%

init([Parent, Owner, StreamRef, MRef, ClientRef, Async]) ->
    InitialState = #viewst{parent=Parent,
                           owner=Owner,
                           ref=StreamRef,
                           mref=MRef,
                           client_ref=ClientRef,
                           async=Async},
    {wait_rows, 0, [[]], InitialState}.

handle_event(end_json, _) ->
    done;
handle_event(Event, {Fun, _, _, _}=St) ->
    ?MODULE:Fun(Event, St).



wait_rows(start_object, St) ->
    St;
wait_rows(end_object, St) ->
    St;
wait_rows({key, <<"rows">>}, {_, _, _, ViewSt}) ->
    {wait_rows1, 0, [[]], ViewSt};
wait_rows({key, <<"total_rows">>},  {_, _, _, ViewSt}) ->
    {wait_val, 0, [[]], ViewSt};
wait_rows({key, <<"offset">>},  {_, _, _, ViewSt}) ->
    {wait_val, 0, [[]], ViewSt}.

wait_val({_, _}, {_, _, _, ViewSt}) ->
    {wait_rows, 0, [[]], ViewSt}.

wait_rows1(start_array, {_, _, _, ViewSt}) ->
    {wait_rows1, 0, [[]], ViewSt};
wait_rows1(start_object, {_, _, Terms, ViewSt}) ->
    {collect_object, 0, [[]|Terms], ViewSt};
wait_rows1(end_array, {_, _, _, ViewSt}) ->
    {wait_rows, 0, [[]], ViewSt}.


collect_object(start_object, {_, NestCount, Terms, ViewSt}) ->
    {collect_object, NestCount + 1, [[]|Terms], ViewSt};

collect_object(end_object, {_, NestCount, [[], {key, Key}, Last|Terms],
                           ViewSt}) ->
    {collect_object, NestCount - 1, [[{Key, {[{}]}}] ++ Last] ++ Terms,
     ViewSt};

collect_object(end_object, {_, NestCount, [Object, {key, Key},
                                           Last|Terms], ViewSt}) ->
    {collect_object, NestCount - 1,
     [[{Key, {lists:reverse(Object)}}] ++ Last] ++ Terms, ViewSt};

collect_object(end_object, {_, 0, [[], Last|Terms], ViewSt}) ->
    [[Row]] = [[{[{}]}] ++ Last] ++ Terms,
    send_row(Row, ViewSt);

collect_object(end_object, {_, NestCount, [[], Last|Terms], ViewSt}) ->
    {collect_object, NestCount - 1, [[{[{}]}] ++ Last] ++ Terms, ViewSt};

collect_object(end_object, {_, 0, [Object, Last|Terms], ViewSt}) ->
    [[Row]] = [[{lists:reverse(Object)}] ++ Last] ++ Terms,
    send_row(Row, ViewSt);


collect_object(end_object, {_, NestCount, [Object, Last|Terms], ViewSt}) ->
    Acc = [[{lists:reverse(Object)}] ++ Last] ++ Terms,
    {collect_object, NestCount - 1, Acc, ViewSt};


collect_object(start_array, {_, NestCount, Terms, ViewSt}) ->
    {collect_object, NestCount, [[]|Terms], ViewSt};
collect_object(end_array, {_, NestCount, [List, {key, Key}, Last|Terms],
                          ViewSt}) ->
    {collect_object, NestCount,
     [[{Key, lists:reverse(List)}] ++ Last] ++ Terms, ViewSt};
collect_object(end_array, {_, NestCount, [List, Last|Terms], ViewSt}) ->
    {collect_object, NestCount, [[lists:reverse(List)] ++ Last] ++ Terms,
     ViewSt};

collect_object({key, Key}, {_, NestCount, Terms, ViewSt}) ->
    {collect_object, NestCount, [{key, Key}] ++ Terms,
     ViewSt};

collect_object({_, Event}, {_, NestCount, [{key, Key}, Last|Terms], ViewSt}) ->
    {collect_object, NestCount, [[{Key, Event}] ++ Last] ++ Terms, ViewSt};
collect_object({_, Event}, {_, NestCount, [Last|Terms], ViewSt}) ->
    {collect_object, NestCount, [[Event] ++ Last] ++ Terms, ViewSt}.

send_row(Row, #viewst{owner=Owner, ref=Ref}=ViewSt) ->
    Owner ! {Ref, {row, couchbeam_ejson:post_decode(Row)}},
    maybe_continue_decoding(ViewSt).

%% eventually wait for the next call from the parent
maybe_continue_decoding(#viewst{parent=Parent,
                                owner=Owner,
                                ref=Ref,
                                mref=MRef,
                                client_ref=ClientRef,
                                async=once}=ViewSt) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            exit(normal);
        {Ref, stream_next} ->
            {wait_rows1, 0, [[]], ViewSt};
        {Ref, cancel} ->
            hackney:close(ClientRef),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% tell the parent we exited
            Owner ! {Ref, ok},
            %% and exit
            exit(normal);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [],
                                  {maybe_continue_decoding, ViewSt});
        Else ->
            error_logger:error_msg("Unexpected message: ~w~n", [Else]),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% report the error
            report_error(Else, Ref, Owner),
            exit(Else)
    after 5000 ->
            erlang:hibernate(?MODULE, maybe_continue_decoding, [ViewSt])
    end;

maybe_continue_decoding(#viewst{parent=Parent,
                                owner=Owner,
                                ref=Ref,
                                mref=MRef,
                                client_ref=ClientRef}=ViewSt) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            exit(normal);
        {Ref, cancel} ->
            hackney:close(ClientRef),
            Owner ! {Ref, ok},
            exit(normal);
        {Ref, pause} ->
            erlang:hibernate(?MODULE, maybe_continue_decoding, [ViewSt]);
        {Ref, resume} ->
            {wait_rows1, 0, [[]], ViewSt};
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [],
                                  {maybe_continue_decoding, ViewSt});
        Else ->
            error_logger:error_msg("Unexpected message: ~w~n", [Else]),
            report_error(Else, Ref, Owner),
            exit(Else)
    after 0 ->
        {wait_rows1, 0, [[]], ViewSt}
    end.

report_error({error, _What}=Error, Ref, Pid) ->
    Pid ! {Ref, Error};
report_error(What, Ref, Pid) ->
    Pid ! {Ref, {error, What}}.
