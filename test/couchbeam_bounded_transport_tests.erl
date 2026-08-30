-module(couchbeam_bounded_transport_tests).

-include_lib("eunit/include/eunit.hrl").

bounded_open_doc_decodes_within_budget_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"_id\":\"doc\",\"value\":1}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'ok', {[{<<"_id">>, <<"doc">>}, {<<"value">>, 1}]},
                  byte_size(Body)},
                 couchbeam:open_doc_bounded(
                   Db, <<"doc">>, [], {1000, 1024}))
      end).

bounded_db_info_closes_oversized_response_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"oversized\":true}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_headers(Socket, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'response_too_large'},
                 couchbeam:db_info_bounded(Db, {1000, 8})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_save_doc_closes_timed_out_response_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    with_http_server(
      fun(Socket, Parent) ->
              send_json_headers(Socket, 32),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'timeout'},
                 couchbeam:save_doc_bounded(
                   Db, {[{<<"_id">>, <<"doc">>}]}, [], {50, 1024})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_closes_oversized_raw_stream_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_headers(Socket, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'response_too_large'},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, 16})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_returns_raw_bytes_for_cumulative_budget_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ExpectedRow = {[{<<"id">>, <<"doc">>},
                              {<<"key">>, <<"doc">>},
                              {<<"value">>, {[]}}]},
              ?assertEqual(
                 {'ok', [ExpectedRow], byte_size(Body)},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, 1024}))
      end).

bounded_view_closes_timed_out_raw_stream_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    with_http_server(
      fun(Socket, Parent) ->
              send_json_headers(Socket, 32),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'timeout'},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {50, 1024})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end,
              assert_no_late_view_message()
      end).

bounded_view_discards_rows_emitted_before_oversize_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    FirstChunk = <<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"first\",\"key\":\"first\",\"value\":{}},">>,
    SecondChunk = <<"{\"id\":\"second\",\"key\":\"second\",\"value\":{}}]}">>,
    MaxBytes = byte_size(FirstChunk) + 1,
    with_http_server(
      fun(Socket, Parent) ->
              send_chunked_headers(Socket),
              send_http_chunk(Socket, FirstChunk),
              timer:sleep(10),
              send_http_chunk(Socket, SecondChunk),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'response_too_large'},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, MaxBytes})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_db_info_closes_oversized_error_body_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"error\":\"too large\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_headers(Socket, 500, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'response_too_large'},
                 couchbeam:db_info_bounded(Db, {1000, 8})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_counts_trailing_raw_chunks_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Json = <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
    Trailing = <<"                ">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_chunked_headers(Socket),
              send_http_chunk(Socket, Json),
              timer:sleep(10),
              send_http_chunk(Socket, Trailing),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'response_too_large'},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, byte_size(Json)})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_closes_http_error_response_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    with_http_server(
      fun(Socket, Parent) ->
              send_json_headers(Socket, 404, 32),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'not_found'},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, 1024})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_reports_invalid_json_without_waiting_past_budget_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"not-json">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', {'invalid_json', 'badarg'}},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {100, 1024}))
      end).

bounded_view_rejects_incomplete_json_at_http_end_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":0,\"offset\":0,\"rows\":[">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', {'invalid_json', 'incomplete'}},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, 1024}))
      end).

bounded_fetch_owns_stream_delivery_options_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    SinkPid = spawn(fun discard_messages/0),
    try
        with_http_server(
          fun(Socket, Parent) ->
                  send_json_response(Socket, Body),
                  Parent ! {'server_done', self()}
          end,
          fun(BaseUrl) ->
                  Server = couchbeam:server_connection(
                             BaseUrl, [{'no_proxy_env', 'true'}]),
                  {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
                  ?assertMatch(
                     {'ok', [_Row], _Bytes},
                     couchbeam_view:fetch_bounded(
                       Db, 'all_docs',
                       [{'stream_to', SinkPid}, {'async', 'once'}],
                       {100, 1024}))
          end)
    after
        exit(SinkPid, 'kill')
    end.

legacy_open_doc_result_shape_unchanged_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"_id\":\"doc\",\"value\":1}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'ok', {[{<<"_id">>, <<"doc">>}, {<<"value">>, 1}]}},
                 couchbeam:open_doc(Db, <<"doc">>, []))
      end).

legacy_view_fetch_result_shape_unchanged_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ExpectedRow = {[{<<"id">>, <<"doc">>},
                              {<<"key">>, <<"doc">>},
                              {<<"value">>, {[]}}]},
              ?assertEqual(
                 {'ok', [ExpectedRow]},
                 couchbeam_view:fetch(Db, 'all_docs', []))
      end).

bounded_open_doc_cancels_slow_headers_at_absolute_deadline_test() ->
    assert_slow_header_deadline(
      fun(Db) ->
              couchbeam:open_doc_bounded(Db, <<"doc">>, [], {50, 1024})
      end).

bounded_db_info_cancels_slow_headers_at_absolute_deadline_test() ->
    assert_slow_header_deadline(
      fun(Db) -> couchbeam:db_info_bounded(Db, {50, 1024}) end).

bounded_save_doc_cancels_slow_headers_at_absolute_deadline_test() ->
    assert_slow_header_deadline(
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [], {50, 1024})
      end).

bounded_json_decode_obeys_near_deadline_test() ->
    Body = iolist_to_binary(
             [<<"[">>, lists:duplicate(200000, <<"0,">>), <<"0]">>]),
    Budget = #{'deadline_ms' =>
                   erlang:monotonic_time('millisecond') + 1,
               'max_response_bytes' => byte_size(Body)},
    ?assertEqual(
       {'error', 'timeout'},
       couchbeam_httpc:decode_bounded_json(Body, byte_size(Body), Budget)).

bounded_view_rejects_trailing_non_whitespace_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Json = <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
    Trailing = <<"garbage">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_chunked_headers(Socket),
              send_http_chunk(Socket, Json),
              timer:sleep(10),
              send_http_chunk(Socket, Trailing),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', {'invalid_json', 'trailing_data'}},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, 1024})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_normalizes_structurally_invalid_row_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[1]}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_headers(Socket, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch(
                 {'error', {'malformed_view', _Reason}},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, 1024})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

assert_slow_header_deadline(CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    with_http_server(
      fun(Socket, Parent) ->
              send_slow_headers(Socket, 10, 20),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              ?assertEqual({'error', 'timeout'}, CallFun(Db)),
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              ?assert(Elapsed < 150)
      end).

send_slow_headers(Socket, Count, DelayMs) ->
    case gen_tcp:send(Socket, <<"HTTP/1.1 200 OK\r\nX-Slow: ">>) of
        'ok' -> send_slow_header_bytes(Socket, Count, DelayMs);
        {'error', _}=Error -> Error
    end.

send_slow_header_bytes(Socket, 0, _DelayMs) ->
    gen_tcp:send(Socket, <<"\r\nContent-Length: 2\r\n\r\n{}">>);
send_slow_header_bytes(Socket, Count, DelayMs) ->
    timer:sleep(DelayMs),
    case gen_tcp:send(Socket, <<"a">>) of
        'ok' -> send_slow_header_bytes(Socket, Count - 1, DelayMs);
        {'error', _}=Error -> Error
    end.

with_http_server(ServerFun, ClientFun) ->
    {'ok', ListenSocket} = gen_tcp:listen(
                             0,
                             ['binary', {'active', 'false'},
                              {'reuseaddr', 'true'}]),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    Parent = self(),
    ServerPid = spawn(
                  fun() ->
                          case gen_tcp:accept(ListenSocket) of
                              {'ok', Socket} ->
                                  case recv_request(Socket, <<>>) of
                                      {'ok', _Request} -> ServerFun(Socket, Parent);
                                      _ -> 'ok'
                                  end,
                                  gen_tcp:close(Socket);
                              _ ->
                                  'ok'
                          end
                  end),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try ClientFun(BaseUrl)
    after
        gen_tcp:close(ListenSocket),
        receive
            {'server_done', ServerPid} -> 'ok'
        after 1000 ->
            exit(ServerPid, 'kill')
        end
    end.

recv_request(Socket, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        {'nomatch'} ->
            case gen_tcp:recv(Socket, 0, 1000) of
                {'ok', Chunk} -> recv_request(Socket, <<Acc/binary, Chunk/binary>>);
                Error -> Error
            end;
        _ ->
            {'ok', Acc}
    end.

send_json_response(Socket, Body) ->
    'ok' = send_json_headers(Socket, byte_size(Body)),
    gen_tcp:send(Socket, Body).

send_json_headers(Socket, ContentLength) ->
    send_json_headers(Socket, 200, ContentLength).

send_json_headers(Socket, Status, ContentLength) ->
    gen_tcp:send(
      Socket,
      [<<"HTTP/1.1 ">>, integer_to_binary(Status), <<" Result\r\nContent-Type: application/json\r\nContent-Length: ">>,
       integer_to_binary(ContentLength),
       <<"\r\nConnection: keep-alive\r\n\r\n">>]).

send_chunked_headers(Socket) ->
    gen_tcp:send(
      Socket,
      <<"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n">>).

send_http_chunk(Socket, Chunk) ->
    Size = integer_to_binary(byte_size(Chunk), 16),
    gen_tcp:send(Socket, [Size, <<"\r\n">>, Chunk, <<"\r\n">>]).

recv_until_closed(Socket) ->
    case gen_tcp:recv(Socket, 0, 1000) of
        {'ok', _Data} -> recv_until_closed(Socket);
        Error -> Error
    end.

ensure_couchbeam_supervisor() ->
    case whereis(couchbeam_sup) of
        'undefined' ->
            {'ok', _Pid} = couchbeam_sup:start_link(),
            'ok';
        _Pid ->
            'ok'
    end.

assert_no_late_view_message() ->
    receive
        {Ref, {'error', Error}} when is_reference(Ref) ->
            ?assertEqual('no_late_view_message', Error)
    after 20 ->
            'ok'
    end.

discard_messages() ->
    receive
        _ -> discard_messages()
    end.
