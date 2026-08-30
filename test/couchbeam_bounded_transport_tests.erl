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

bounded_save_doc_cancels_stalled_upload_test() ->
    Capacity = measured_send_capacity(),
    Payload = binary:copy(<<"x">>, Capacity * 256),
    assert_stalled_upload_deadline(
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db,
                {[{<<"_id">>, <<"doc">>}, {<<"payload">>, Payload}]},
                [], {500, 1024})
      end).

bounded_view_post_cancels_stalled_upload_test() ->
    Capacity = measured_send_capacity(),
    Key = binary:copy(<<"k">>, Capacity * 256),
    assert_stalled_upload_deadline(
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'keys', [Key]}], {500, 1024})
      end).

bounded_view_owner_death_cancels_post_transport_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Capacity = measured_send_capacity(),
    Key = binary:copy(<<"k">>, Capacity * 256),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'owner_death_upload_ready', self()},
              receive 'observe_owner_close' -> 'ok' end,
              ServerParent ! {'owner_death_peer_close',
                              recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'socket_options', [{'sndbuf', 4096}]},
                          {'bounded_upload_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'owner_death_result', self(),
                                           couchbeam_view:fetch_bounded(
                                             Db, 'all_docs',
                                             [{'keys', [Key]}],
                                             {2000, 1024})}
                         end),
              WorkerPid = receive
                              {'bounded_upload_worker', Worker} -> Worker
                          after 1000 ->
                                  ?assert('false')
                          end,
              ServerPid = receive
                              {'owner_death_upload_ready', Pid} -> Pid
                          after 1000 ->
                                  ?assert('false')
                          end,
              Ref = owned_request_ref(WorkerPid),
              Started = erlang:monotonic_time('millisecond'),
              exit(Caller, 'kill'),
              ServerPid ! 'observe_owner_close',
              ?assertEqual('ok', await_ref_absent(Ref, 200)),
              receive
                  {'owner_death_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 200 ->
                      ?assert('false')
              end,
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              ?assert(Elapsed < 200),
              receive
                  {'owner_death_result', Caller, _Result} -> ?assert('false')
              after 0 ->
                      'ok'
              end
      end).

bounded_save_doc_encode_obeys_deadline_before_transport_test() ->
    assert_encode_deadline_before_transport(
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}, {<<"value">>, 1}]},
                [], {50, 1024})
      end).

bounded_view_post_encode_obeys_deadline_before_transport_test() ->
    ensure_couchbeam_supervisor(),
    assert_encode_deadline_before_transport(
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'keys', [<<"key">>]}], {50, 1024})
      end).

bounded_late_handoff_is_compensated_before_return_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"db_name\":\"db\"}">>,
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              send_json_response(Socket, Body),
              ServerParent ! {'peer_close_result', recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_handoff_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {100, 1024}),
                                 Ref = receive
                                           {'test_ref', TestRef} -> TestRef
                                       end,
                                 Late = receive
                                            {'hackney_response', Ref, _} -> 'true'
                                        after 0 ->
                                                'false'
                                        end,
                                 Parent ! {'bounded_result', self(),
                                           Result, Late}
                         end),
              receive
                  {'bounded_handoff_ready', Ref, Caller} ->
                      'ok' = sys:suspend('hackney_manager'),
                      try
                          Caller ! {'test_ref', Ref},
                          Caller ! {'bounded_handoff_continue', Ref},
                          timer:sleep(120)
                      after
                          'ok' = sys:resume('hackney_manager')
                      end,
                      receive
                          {'bounded_result', Caller, Result, Late} ->
                              ?assertEqual({'error', 'timeout'}, Result),
                              ?assertEqual('false', Late),
                              ?assertEqual(
                                 [], ets:lookup('hackney_manager_refs', Ref))
                      after 1000 ->
                              ?assert('false')
                      end
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1000 ->
                      ?assert('false')
              end
      end).

bounded_view_decode_watchdog_cancels_transport_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              send_json_headers(Socket, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              ServerParent ! {'peer_close_result', recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_handoff_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              {Caller, Ref} = start_captured_view_call(
                                fun() ->
                                        couchbeam_view:fetch_bounded(
                                          Db, 'all_docs',
                                          [{'bounded_decode_test_delay_ms',
                                            200}], {50, 1024})
                                end),
              receive
                  {'captured_view_result', Caller, Ref, Result, RefAbsent} ->
                      ?assertEqual({'error', 'timeout'}, Result),
                      ?assertEqual('true', RefAbsent)
              after 1000 ->
                      ?assert('false')
              end,
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              ?assert(Elapsed < 150),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_status_transport_error_closes_peer_test() ->
    assert_view_transport_error_closed('status').

bounded_view_body_transport_error_closes_peer_test() ->
    assert_view_transport_error_closed('body').

assert_stalled_upload_deadline(CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'upload_backpressured', self()},
              timer:sleep(550),
              ServerParent ! {'peer_close_result', recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'socket_options', [{'sndbuf', 4096}]},
                          {'bounded_upload_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'upload_result', self(),
                                           CallFun(Db)}
                         end),
              WorkerPid = receive
                              {'bounded_upload_worker', Worker} -> Worker
                          after 1000 ->
                                  ?assert('false')
                          end,
              receive
                  {'upload_backpressured', _ServerPid} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              Ref = owned_request_ref(WorkerPid),
              receive
                  {'upload_result', Caller, Result} ->
                      ?assertEqual({'error', 'timeout'}, Result),
                      ?assertEqual(
                         [], ets:lookup('hackney_manager_refs', Ref))
              after 1000 ->
                      ?assert('false')
              end,
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              ?assert(Elapsed < 800),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

assert_encode_deadline_before_transport(CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    {'ok', ListenSocket} = gen_tcp:listen(
                             0, ['binary', {'active', 'false'},
                                 {'reuseaddr', 'true'}]),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try
        Server = couchbeam:server_connection(
                   BaseUrl,
                   [{'no_proxy_env', 'true'},
                    {'bounded_encode_test_delay_ms', 200}]),
        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
        Started = erlang:monotonic_time('millisecond'),
        ?assertEqual({'error', 'timeout'}, CallFun(Db)),
        Elapsed = erlang:monotonic_time('millisecond') - Started,
        ?assert(Elapsed < 150),
        ?assertEqual({'error', 'timeout'}, gen_tcp:accept(ListenSocket, 20))
    after
        gen_tcp:close(ListenSocket)
    end.

measured_send_capacity() ->
    {'ok', ListenSocket} = gen_tcp:listen(
                             0, ['binary', {'active', 'false'},
                                 {'reuseaddr', 'true'}]),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    {'ok', ClientSocket} = gen_tcp:connect(
                             {127, 0, 0, 1}, Port,
                             ['binary', {'active', 'false'},
                              {'sndbuf', 4096}]),
    {'ok', ServerSocket} = gen_tcp:accept(ListenSocket),
    {'ok', [{'sndbuf', Capacity}]} = inet:getopts(ClientSocket, ['sndbuf']),
    gen_tcp:close(ClientSocket),
    gen_tcp:close(ServerSocket),
    gen_tcp:close(ListenSocket),
    Capacity.

owned_request_ref(WorkerPid) ->
    owned_request_ref(
      WorkerPid, erlang:monotonic_time('millisecond') + 1000).

owned_request_ref(WorkerPid, DeadlineMs) ->
    case ets:match_object(
           'hackney_manager_refs', {'_', {WorkerPid, '_', '_'}}) of
        [{Ref, _}] -> Ref;
        [] ->
            case erlang:monotonic_time('millisecond') < DeadlineMs of
                'true' ->
                    erlang:yield(),
                    owned_request_ref(WorkerPid, DeadlineMs);
                'false' ->
                    ?assert('false')
            end
    end.

await_ref_absent(Ref, TimeoutMs) ->
    await_ref_absent_until(
      Ref, erlang:monotonic_time('millisecond') + TimeoutMs).

await_ref_absent_until(Ref, DeadlineMs) ->
    case ets:lookup('hackney_manager_refs', Ref) of
        [] -> 'ok';
        [_] ->
            case erlang:monotonic_time('millisecond') < DeadlineMs of
                'true' ->
                    erlang:yield(),
                    await_ref_absent_until(Ref, DeadlineMs);
                'false' ->
                    {'error', 'timeout'}
            end
    end.

assert_view_transport_error_closed(Phase) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              maybe_send_transport_error_headers(Socket, Phase),
              'ok' = gen_tcp:shutdown(Socket, 'write'),
              ServerParent ! {'peer_close_result', recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_handoff_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              {Caller, Ref} = start_captured_view_call(
                                fun() ->
                                        couchbeam_view:fetch_bounded(
                                          Db, 'all_docs', [], {1000, 1024})
                                end),
              receive
                  {'captured_view_result', Caller, Ref, Result, RefAbsent} ->
                      ?assertMatch({'error', _}, Result),
                      ?assertEqual('true', RefAbsent)
              after 1500 ->
                      ?assert('false')
              end,
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

start_captured_view_call(CallFun) ->
    Parent = self(),
    Caller = spawn(
               fun() ->
                       Result = CallFun(),
                       Ref = receive {'test_ref', TestRef} -> TestRef end,
                       RefAbsent = ets:lookup('hackney_manager_refs', Ref) =:= [],
                       Parent ! {'captured_view_result', self(), Ref,
                                 Result, RefAbsent}
               end),
    receive
        {'bounded_handoff_ready', Ref, StreamPid} ->
            Caller ! {'test_ref', Ref},
            StreamPid ! {'bounded_handoff_continue', Ref},
            {Caller, Ref}
    after 1000 ->
            exit(Caller, 'kill'),
            ?assert('false')
    end.

maybe_send_transport_error_headers(_Socket, 'status') ->
    'ok';
maybe_send_transport_error_headers(Socket, 'body') ->
    send_json_headers(Socket, 32).

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
