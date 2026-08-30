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
                 async='false',
                 budget='undefined'}).


-define(TIMEOUT, 10000).
-define(DEFAULT_CHANGES_POLL, 5000). % we check every 5secs



start_link(Owner, StreamRef, {Db, Url, Args}, StreamOptions) ->
    proc_lib:start_link(?MODULE, init_stream, [self(), Owner, StreamRef,
                                               {Db, Url, Args},
                                               StreamOptions]).

init_stream(Parent, Owner, StreamRef, {_Db, _Url, _Args}=Req,
            StreamOptions) ->


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
    case do_init_stream(Req, InitState) of
        {ok, State} ->
            %% register the stream
            ets:insert(couchbeam_view_streams, [{StreamRef, self()}]),
            %% start the loop
            loop(State);
        Error ->
            report_error(Error, StreamRef, Owner)
    end,
    %% stop to monitor the parent
    erlang:demonitor(MRef),
    ok.

do_init_stream({#db{options=Opts}, Url, Args},
               #state{owner=LifecycleOwner, ref=StreamRef,
                      mref=MRef, budget=Budget}=State) ->
    %% we are doing the request asynchronously
    FinalOpts = request_options([{'async', 'once'} | Opts], Budget),
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
            Timeout = receive_timeout(State),
            receive
                {'DOWN', MRef, _, _, _} ->
                    %% parent exited there is no need to continue
                    exit(normal);
                {StreamRef, 'budget_timeout'} ->
                    cancel_result(Ref, Budget, {'error', 'timeout'});
                {hackney_response, Ref, {status, 200, _}} ->
                    #state{parent=Parent,
                           ref=StreamRef,
                           async=Async} = State,

                    DecoderOwner = decoder_owner(State),
                    DecoderFun = jsx:decoder(?MODULE, [Parent, DecoderOwner,
                                                       StreamRef, MRef, Ref,
                                                       Async, Budget], [stream]),
                    {ok, State#state{client_ref=Ref,
                                     decoder=DecoderFun}};

                {hackney_response, Ref, {status, 404, _}} ->
                    cancel_result(Ref, Budget, {error, not_found});
                {hackney_response, Ref, {status, Status, Reason}} ->
                    cancel_result(
                      Ref, Budget, {error, {http_error, Status, Reason}});
                {hackney_response, Ref, {error, Reason}} ->
                    cancel_result(Ref, Budget, {error, Reason})
            after Timeout ->
                    cancel_result(Ref, Budget, {'error', 'timeout'})
            end;
        {'error', 'transport_cleanup_timeout'}=Error
          when Budget =/= 'undefined' ->
            Error;
        {'error', _}=Error when Budget =/= 'undefined' ->
            LifecycleOwner ! {'bounded_transport_cleanup', self(),
                              'undefined'},
            Error;
        Error ->
            {error, Error}
    end.



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
loop_receive(#state{budget='undefined'}=State, _Owner, StreamRef, MRef,
             ClientRef) ->
    hackney:stream_next(ClientRef),
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
    end;
loop_receive(State, _Owner, StreamRef, MRef, ClientRef) ->
    hackney:stream_next(ClientRef),
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
    case CleanupResult of
        'ok' -> Owner ! done_message(State);
        {'error', 'transport_cleanup_timeout'} ->
            report_error('transport_cleanup_timeout', StreamRef, Owner)
    end,
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
    catch 'error':'badarg' -> exit('badarg')
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
                     #state{ref=StreamRef}=State) ->
    Timeout = receive_timeout(State),
    receive
        {Token, Result} ->
            erlang:demonitor(MonitorRef, ['flush']),
            Rows = take_decoder_rows(StreamRef, []),
            finish_bounded_decode(Result, Rows, State);
        {'DOWN', MonitorRef, 'process', DecoderPid, Reason} ->
            flush_bounded_decode_result(Token),
            discard_decoder_rows(StreamRef),
            fail_stream({'malformed_view', {'decoder_down', Reason}}, State)
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
            maybe_continue(State#state{decoder=DecodeFun})
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

-spec request_options(list(),
                      'undefined' | couchbeam_httpc:request_budget()) -> list().
request_options(Options, 'undefined') ->
    Options;
request_options(Options, Budget) ->
    [{'request_budget', Budget} | Options].

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
                           client_ref=ClientRef}) ->
    CleanupResult = couchbeam_httpc:cancel_request(ClientRef),
    ets:delete(couchbeam_view_streams, StreamRef),
    case CleanupResult of
        'ok' -> report_error(Reason, StreamRef, Owner);
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

-spec cancel_result(reference(), couchbeam_httpc:request_budget(), term()) ->
          term().
cancel_result(Ref, Budget, Result) ->
    case maybe_cancel_request(Ref, Budget) of
        'ok' -> Result;
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
                      async=once,
                      client_ref=ClientRef}=State) ->

    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            maybe_close(State),
            exit(normal);
        {hackney_response, ClientRef, done} ->
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% report the error
            report_error({error, closed}, Ref, Owner),
            exit({error, closed});
        {hackney_response, ClientRef, {error, _}=Error} ->
            fail_or_report_stream(Error, State);
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
                      mref=MRef,
                      client_ref=ClientRef}=State) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            maybe_close(State),
            exit(normal);
        {hackney_response, ClientRef, done} ->
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% report the error
            report_error({error, closed}, Ref, Owner),
            exit({error, closed});
        {hackney_response, ClientRef, {error, _}=Error} ->
            fail_or_report_stream(Error, State);
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

-spec maybe_close_after_owner_down(#state{}) -> 'ok'.
maybe_close_after_owner_down(#state{budget='undefined'}) ->
    'ok';
maybe_close_after_owner_down(State) ->
    maybe_close(State).


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
    init([Parent, Owner, StreamRef, MRef, ClientRef, Async, 'undefined']);
init([Parent, Owner, StreamRef, MRef, ClientRef, Async, Budget]) ->
    InitialState = #viewst{parent=Parent,
                           owner=Owner,
                           ref=StreamRef,
                           mref=MRef,
                           client_ref=ClientRef,
                           async=Async,
                           budget=Budget},
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
                                budget=Budget,
                                async=once}=ViewSt) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            close_view_after_owner_down(ClientRef, Budget),
            exit(normal);
        {Ref, stream_next} ->
            {wait_rows1, 0, [[]], ViewSt};
        {Ref, cancel} ->
            close_view_request(ClientRef, Budget),
            %% unregister the stream
            ets:delete(couchbeam_view_streams, Ref),
            %% tell the parent we exited
            Owner ! {Ref, ok},
            %% and exit
            exit(normal);
        {Ref, 'budget_timeout'} ->
            fail_view_decoder_timeout(Ref, ClientRef, Budget, Owner);
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
                                client_ref=ClientRef,
                                budget=Budget}=ViewSt) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            %% parent exited there is no need to continue
            close_view_after_owner_down(ClientRef, Budget),
            exit(normal);
        {Ref, cancel} ->
            close_view_request(ClientRef, Budget),
            Owner ! {Ref, ok},
            exit(normal);
        {Ref, 'budget_timeout'} ->
            fail_view_decoder_timeout(Ref, ClientRef, Budget, Owner);
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

-spec close_view_request(reference(),
                         'undefined' | couchbeam_httpc:request_budget()) ->
          'ok' | {'error', 'transport_cleanup_timeout'}.
close_view_request(ClientRef, 'undefined') ->
    hackney:close(ClientRef);
close_view_request(ClientRef, _Budget) ->
    couchbeam_httpc:cancel_request(ClientRef).

-spec close_view_after_owner_down(
        reference(), 'undefined' | couchbeam_httpc:request_budget()) -> 'ok'.
close_view_after_owner_down(_ClientRef, 'undefined') ->
    'ok';
close_view_after_owner_down(ClientRef, Budget) ->
    close_view_request(ClientRef, Budget).

-spec fail_view_decoder_timeout(reference(), reference(),
                                couchbeam_httpc:request_budget(), pid()) ->
          no_return().
fail_view_decoder_timeout(Ref, ClientRef, Budget, Owner) ->
    CleanupResult = close_view_request(ClientRef, Budget),
    ets:delete(couchbeam_view_streams, Ref),
    case CleanupResult of
        'ok' -> report_error('timeout', Ref, Owner);
        {'error', 'transport_cleanup_timeout'} ->
            report_error('transport_cleanup_timeout', Ref, Owner)
    end,
    exit('normal').

report_error({error, _What}=Error, Ref, Pid) ->
    Pid ! {Ref, Error};
report_error(What, Ref, Pid) ->
    Pid ! {Ref, {error, What}}.
