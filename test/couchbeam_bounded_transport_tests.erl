%%% Regression suite of the bounded transport and of the official lifecycle
%%% it is grafted onto. Run in the dependency's own checkout:
%%%
%%%     rebar3 as test eunit --module=couchbeam_bounded_transport_tests
%%%
%%% Wall time is about 42 s on a loopback: the long scenarios live in
%%% `{timeout, 30, ...}' generators (pauses of 10.2 s, 7 s and 5 s pin the
%%% legacy inter-chunk wait and the deadline verdicts), the rest finish in
%%% milliseconds. Which scenario has to be a generator is not left to
%%% judgement: eunit runs a plain `*_test/0' under a 5 s default and enforces
%%% it with an untrappable kill, and
%%% `test_wait_budgets_stay_under_eunit_default_test' computes every test's
%%% worst-case wait budget from this module's own abstract code and fails on
%%% any plain test that could outlast that default. Every scenario binds its
%%% own loopback listener on an ephemeral port (`listen_loopback/0', used by
%%% `with_http_server/2' and `with_http_request_server/2') and answers over
%%% plain TCP; no CouchDB is needed, and the ten official inline tests of
%%% `couchbeam'/`couchbeam_view' that do need one fail with `nxdomain' in a
%%% full `rebar3 as test eunit' run, on the official base as well. The whole
%%% module runs in one eunit process, so shared state is the hazard: the
%%% scenarios that need a clean mailbox empty it themselves
%%% (`drain_transport_messages/0'; there is no fixture doing it for the rest),
%%% and the ones that park the shared `hackney_manager' go
%%% through `suspend_manager/0', whose watcher resumes it even when the
%%% scenario is killed without running its `after'.
%%% Snapshot 2026-09-05: test inventory is recorded in the chain output.
-module(couchbeam_bounded_transport_tests).

-include_lib("eunit/include/eunit.hrl").
-include("couchbeam.hrl").

%% logger handler callback used by the cleanup-warning capture
-export([log/2]).

bounded_db_info_preserves_kazoo_headers_test() ->
    assert_bounded_kazoo_headers(
      <<"GET /db HTTP/1.1">>,
      <<"{\"db_name\":\"db\"}">>,
      fun(Db) -> couchbeam:db_info_bounded(Db, {1000, 1024}) end).

bounded_open_doc_preserves_kazoo_headers_test() ->
    assert_bounded_kazoo_headers(
      <<"GET /db/doc HTTP/1.1">>,
      <<"{\"_id\":\"doc\"}">>,
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [], {1000, 1024})
      end).

bounded_save_doc_preserves_kazoo_headers_test() ->
    assert_bounded_kazoo_headers(
      <<"PUT /db/doc HTTP/1.1">>,
      <<"{\"ok\":true,\"id\":\"doc\",\"rev\":\"1-a\"}">>,
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024})
      end).

bounded_view_get_preserves_kazoo_headers_test() ->
    ensure_couchbeam_supervisor(),
    assert_bounded_kazoo_headers(
      <<"GET /db/_all_docs HTTP/1.1">>,
      <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [], {1000, 1024})
      end).

bounded_view_post_preserves_kazoo_headers_test() ->
    ensure_couchbeam_supervisor(),
    assert_bounded_kazoo_headers(
      <<"POST /db/_all_docs HTTP/1.1">>,
      <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'keys', [<<"doc">>]}], {1000, 1024})
      end).

official_and_bounded_api_exports_coexist_test() ->
    assert_exports(
      couchbeam,
      [{'find', 3}, {'db_info_bounded', 2}, {'open_doc_bounded', 4},
       {'save_doc_bounded', 4}]),
    assert_exports(
      couchbeam_view,
      [{'show', 2}, {'show', 3}, {'show', 4},
       {'fetch', 1}, {'fetch', 2}, {'fetch', 3}, {'fetch_bounded', 4}]),
    assert_exports(
      couchbeam_httpc,
      [{'request_bounded', 6}, {'request_bounded', 7},
       {'bounded_json_body', 2}, {'bounded_encode_json', 2},
       {'bounded_encode_json', 3}, {'new_request_budget', 1},
       {'cancel_request', 1}]).

%% --- official lifecycle locks -----------------------------------------------
%% Down to `gen_changes_callback_crash_is_controlled_test' the tests pin
%% behaviour of the official base this branch does not change (`stream_next'
%% pruning, changes streams, `gen_changes' crash handling, owner death on a
%% legacy stream): the regression matrix the spec requires alongside the
%% bounded suite, so a semantic port cannot lose it unnoticed.

legacy_malformed_view_closes_transport_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[invalid]}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'legacy_malformed_peer_close',
                        recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              {'ok', Ref} = couchbeam_view:stream(Db, 'all_docs', []),
              receive
                  {'legacy_malformed_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      couchbeam_view:cancel_stream(Ref),
                      ?assert('false')
              end
      end).

legacy_view_owner_death_closes_transport_test_() ->
    {'timeout', 30, fun legacy_view_owner_death_closes_transport/0}.

legacy_view_owner_death_closes_transport() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = send_json_headers(Socket, 32),
              ServerParent ! {'legacy_owner_stream_ready', self()},
              ServerParent ! {'legacy_owner_peer_close',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Owner = spawn(
                        fun() ->
                                {'ok', Ref} = couchbeam_view:stream(
                                                Db, 'all_docs', []),
                                Parent ! {'legacy_owner_ref', self(), Ref},
                                receive 'stop' -> 'ok' end
                        end),
              receive
                  {'legacy_owner_stream_ready', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Owner, 'kill'),
                      ?assert('false')
              end,
              Ref = receive
                        {'legacy_owner_ref', Owner, StreamRef} -> StreamRef
                    after 1000 ->
                            exit(Owner, 'kill'),
                            ?assert('false')
                    end,
              [_] = await_stream_entry('couchbeam_view_streams', Ref),
              exit(Owner, 'kill'),
              receive
                  {'legacy_owner_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

official_dead_stream_entries_are_removed_test() ->
    ensure_couchbeam_supervisor(),
    DeadPid = dead_pid(),
    ViewRef = make_ref(),
    'true' = ets:insert('couchbeam_view_streams', {ViewRef, DeadPid}),
    ?assertEqual({'error', 'stream_undefined'},
                 couchbeam_view:stream_next(ViewRef)),
    ?assertEqual([], ets:lookup('couchbeam_view_streams', ViewRef)),
    ChangesRef = make_ref(),
    'true' = ets:insert('couchbeam_changes_streams', {ChangesRef, DeadPid}),
    ?assertEqual({'error', 'stream_undefined'},
                 couchbeam_changes:stream_next(ChangesRef)),
    ?assertEqual([], ets:lookup('couchbeam_changes_streams', ChangesRef)).

changes_stream_registers_before_follow_returns_test_() ->
    {'timeout', 30, fun changes_stream_registers_before_follow_returns/0}.

changes_stream_registers_before_follow_returns() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 32),
              Parent ! {'changes_registration_ready', self()},
              _ = recv_until_closed(Socket, 3000),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              {'ok', Ref} = couchbeam_changes:follow(
                              Db, [{'feed', 'continuous'}]),
              receive
                  {'changes_registration_ready', _ServerPid} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              [{Ref, Pid}] = ets:lookup('couchbeam_changes_streams', Ref),
              ?assert(is_process_alive(Pid)),
              exit(Pid, 'shutdown')
      end).

changes_stream_malformed_json_closes_transport_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{not-json}\n">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'changes_malformed_peer_close',
                        recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              {'ok', _Ref} = couchbeam_changes:follow(
                               Db, [{'feed', 'continuous'}]),
              receive
                  {'changes_malformed_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

gen_changes_callback_crash_is_controlled_test() ->
    Ref = make_ref(),
    State = #gen_changes_state{stream_ref=Ref,
                               mod=couchbeam_gen_changes_crash_callback,
                               modstate=self(),
                               last_seq=0},
    Change = {[{<<"seq">>, 1}]},
    ?assertMatch(
       {'stop',
        {'handle_change_crashed',
         {'error', 'intentional_callback_crash'}},
        #gen_changes_state{last_seq=1}},
       gen_changes:handle_info({Ref, {'change', Change}}, State)).

%% --- bounded transport -----------------------------------------------------

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
              %% 300 ms, not 50: with a tighter budget the deadline raced
              %% the socket teardown of the previous fixture and the verdict
              %% occasionally arrived as {'closed', 'timeout'}
              ?assertEqual(
                 {'error', 'timeout'},
                 couchbeam:save_doc_bounded(
                   Db, {[{<<"_id">>, <<"doc">>}]}, [], {300, 1024})),
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
    FirstChunk = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[">>,
    SecondChunk = <<"{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    BodySize = byte_size(FirstChunk) + byte_size(SecondChunk),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, FirstChunk),
              timer:sleep(10),
              'ok' = send_http_chunk(Socket, SecondChunk),
              'ok' = send_http_chunk(Socket, <<>>),
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
                 {'ok', [ExpectedRow], BodySize},
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
              assert_no_late_view_message(),
              assert_no_lifecycle_messages()
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

legacy_view_fetch_sync_result_shape_unchanged_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
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
                 couchbeam_view:fetch(Db, 'all_docs', ['sync_query']))
      end).

legacy_view_stream_allows_long_interchunk_pause_test_() ->
    {timeout, 15,
     fun() ->
             {'ok', _} = application:ensure_all_started('hackney'),
             ensure_couchbeam_supervisor(),
             Prefix = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[">>,
             Suffix = <<"{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
             BodySize = byte_size(Prefix) + byte_size(Suffix),
             with_http_server(
               fun(Socket, Parent) ->
                       'ok' = send_json_headers(Socket, BodySize),
                       'ok' = gen_tcp:send(Socket, Prefix),
                       timer:sleep(10200),
                       'ok' = gen_tcp:send(Socket, Suffix),
                       Parent ! {'server_done', self()}
               end,
               fun(BaseUrl) ->
                       Server = couchbeam:server_connection(
                                  BaseUrl, [{'no_proxy_env', 'true'},
                                            {'recv_timeout', 15000}]),
                       {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
                       ExpectedRow = {[{<<"id">>, <<"doc">>},
                                       {<<"key">>, <<"doc">>},
                                       {<<"value">>, {[]}}]},
                       {'ok', Ref} = couchbeam_view:stream(
                                       Db, 'all_docs', []),
                       ?assertEqual(
                          {'ok', [ExpectedRow]},
                          collect_legacy_stream(Ref, []))
               end)
     end}.

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
    {'ok', Budget} = couchbeam_httpc:new_request_budget({1, byte_size(Body)}),
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

bounded_view_rejects_structurally_invalid_row_test() ->
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
    drain_transport_messages(),
    with_http_server(
      fun(Socket, Parent) ->
              %% 10 header bytes, 100 ms apart: a client that waited for the
              %% server would need about a second, the 50 ms budget must not.
              send_slow_headers(Socket, 10, 100),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              ?assertEqual({'error', 'timeout'}, CallFun(Db)),
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              ?assert(Elapsed < 500),
              assert_no_stray_transport_messages()
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

bounded_save_doc_cancels_unanswered_request_test_() ->
    {'timeout', 30, fun bounded_save_doc_cancels_unanswered_request/0}.

bounded_save_doc_cancels_unanswered_request() ->
    Capacity = measured_send_capacity(),
    Payload = binary:copy(<<"x">>, Capacity * 256),
    assert_unanswered_request_deadline(
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db,
                {[{<<"_id">>, <<"doc">>}, {<<"payload">>, Payload}]},
                [], {500, 1024})
      end).

bounded_view_post_cancels_unanswered_request_test_() ->
    {'timeout', 30, fun bounded_view_post_cancels_unanswered_request/0}.

bounded_view_post_cancels_unanswered_request() ->
    ensure_couchbeam_supervisor(),
    Capacity = measured_send_capacity(),
    Key = binary:copy(<<"k">>, Capacity * 256),
    assert_unanswered_request_deadline(
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'keys', [Key]}], {500, 1024})
      end).

bounded_view_stream_death_cancels_post_transport_test_() ->
    {'timeout', 30, fun bounded_view_stream_death_cancels_post_transport/0}.

bounded_view_stream_death_cancels_post_transport() ->
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
                          {'bounded_upload_test_hook', Parent},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs',
                                            [{'keys', [Key]}],
                                            {2000, 1024}),
                                 {Ref, Worker, Lease} = receive
                                     {'owner_death_context', Context} -> Context
                                 end,
                                 Parent ! {
                                   'owner_death_api_returned', self(), Result,
                                   ets:lookup('hackney_manager_refs', Ref) =:= [],
                                   is_process_alive(Worker),
                                   is_process_alive(Lease)},
                                 receive
                                     {'peer_observed_before_result', Ref} ->
                                         Parent ! {'owner_death_result', self(),
                                                   Result}
                                 end
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
              {WorkerPid, LeasePid, GuardianPid, ViewPid} = receive
                  {'bounded_upload_context', WorkerPid, Lease, Guardian,
                   StreamPid} ->
                      {WorkerPid, Lease, Guardian, StreamPid}
              after 1000 ->
                      ?assert('false')
              end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              Caller ! {'owner_death_context', {Ref, WorkerPid, LeasePid}},
              Started = erlang:monotonic_time('millisecond'),
              exit(ViewPid, 'kill'),
              ServerPid ! 'observe_owner_close',
              receive
                  {'owner_death_api_returned', Caller, Result,
                   RefAbsent, WorkerAlive, LeaseAlive} ->
                      ?assertMatch({'error', {'stream_down', _}}, Result),
                      ?assertEqual('true', RefAbsent),
                      ?assertEqual('false', WorkerAlive),
                      ?assertEqual('false', LeaseAlive)
              after 1000 ->
                      ?assert('false')
              end,
              receive
                  {'owner_death_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1000 ->
                      ?assert('false')
              end,
              Caller ! {'peer_observed_before_result', Ref},
              receive
                  {'owner_death_result', Caller, FinalResult} ->
                      ?assertMatch({'error', {'stream_down', _}}, FinalResult)
              after 1000 ->
                      ?assert('false')
              end,
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              %% cleanup after owner death must not wait for the 2000 ms budget
              ?assert(Elapsed < 1000),
              assert_process_gone(GuardianPid)
      end).

bounded_direct_caller_death_before_guardian_resources_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    BeforeRefs = lists:sort(ets:tab2list('hackney_manager_refs')),
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try
        Server = couchbeam:server_connection(
                   BaseUrl,
                   [{'no_proxy_env', 'true'},
                    {'bounded_guardian_ready_test_hook', Parent}]),
        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
        Caller = spawn(
                   fun() ->
                           Parent ! {'pre_resource_result', self(),
                                     couchbeam:db_info_bounded(
                                       Db, {1000, 1024})}
                   end),
        receive
            {'bounded_guardian_ready', GuardianPid, Caller} ->
                GuardianRef = erlang:monitor('process', GuardianPid),
                exit(Caller, 'kill'),
                receive
                    {'DOWN', GuardianRef, 'process', GuardianPid, _Reason} ->
                        'ok'
                after 200 ->
                        ?assert('false')
                end,
                ?assertEqual(
                   BeforeRefs,
                   lists:sort(ets:tab2list('hackney_manager_refs'))),
                ?assertEqual({'error', 'timeout'},
                             gen_tcp:accept(ListenSocket, 20)),
                receive
                    {'pre_resource_result', Caller, _Result} -> ?assert('false')
                after 0 ->
                        'ok'
                end
        after 1000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end
    after
        gen_tcp:close(ListenSocket)
    end.

bounded_guardian_post_start_cancel_waits_for_manager_cleanup_test_() ->
    {'timeout', 30, fun bounded_guardian_post_start_cancel_waits_for_manager_cleanup/0}.

bounded_guardian_post_start_cancel_waits_for_manager_cleanup() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'post_start_request_received', self()},
              ServerParent ! {'post_start_peer_close',
                              recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {100, 1024}),
                                 receive 'publish_post_start_result' -> 'ok' end,
                                 Parent ! {'post_start_result', self(), Result}
                         end),
              {GuardianPid, WorkerPid, LeasePid} = receive
                  {'bounded_guardian_started_ready', Guardian, Worker, Lease,
                   Caller} ->
                      {Guardian, Worker, Lease}
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              receive
                  {'post_start_request_received', _ServerPid} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              Ref = owned_request_ref(WorkerPid),
              LeaseMonitor = erlang:monitor('process', LeasePid),
              exit(LeasePid, 'kill'),
              receive
                  {'DOWN', LeaseMonitor, 'process', LeasePid, _} -> 'ok'
              after 200 ->
                      ?assert('false')
              end,
              ?assertEqual('true', is_process_alive(GuardianPid)),
              ManagerWatcher = suspend_manager(),
              try
                  receive
                      {'bounded_guardian_cleanup', 'started', GuardianPid,
                       'undefined'} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  WorkerMonitor = erlang:monitor('process', WorkerPid),
                  receive
                      {'DOWN', WorkerMonitor, 'process', WorkerPid, _} ->
                          'ok'
                  after 200 ->
                          ?assert('false')
                  end,
                  ?assertEqual('true', is_process_alive(GuardianPid)),
                  ?assertMatch([_], ets:lookup('hackney_manager_refs', Ref)),
                  receive
                      {'post_start_result', Caller, _Result} -> ?assert('false');
                      {'bounded_guardian_cleanup', 'complete', GuardianPid,
                       _CleanupRef} -> ?assert('false')
                  after 0 ->
                          'ok'
                  end
              after
                  'ok' = resume_manager(ManagerWatcher)
              end,
              receive
                  {'bounded_guardian_cleanup', 'complete', GuardianPid,
                   'undefined'} -> 'ok'
              after 500 ->
                      ?assert('false')
              end,
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'post_start_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 500 ->
                      ?assert('false')
              end,
              Caller ! 'publish_post_start_result',
              receive
                  {'post_start_result', Caller, Result} ->
                      ?assertEqual({'error', 'timeout'}, Result)
              after 200 ->
                      ?assert('false')
              end,
              assert_process_gone(GuardianPid),
              ?assertEqual('false', is_process_alive(WorkerPid)),
              ?assertEqual('false', is_process_alive(LeasePid))
      end).

bounded_guardian_reports_cleanup_failure_and_retries_on_owner_down_test_() ->
    {'timeout', 30, fun bounded_guardian_reports_cleanup_failure_and_retries_on_owner_down/0}.

bounded_guardian_reports_cleanup_failure_and_retries_on_owner_down() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    with_captured_warnings(
      fun() -> cleanup_failure_and_retry_scenario(Parent) end,
      fun(Events) ->
              %% exactly one warning for the whole retry sequence, and one
              %% recovery notice once a retry finally proved the cleanup;
              %% both name what an operator can act on — the method and the
              %% path (never the URL, which carries Kazoo's credentials)
              ?assertMatch([{'report', #{'method' := 'get',
                                         'path' := <<"/db">>}}],
                           cleanup_unproven_warnings(Events)),
              ?assertMatch([{'report', #{'method' := 'get',
                                         'path' := <<"/db">>}}],
                           cleanup_recovered_notices(Events))
      end).

cleanup_failure_and_retry_scenario(Parent) ->
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'cleanup_failure_request_received', self()},
              ServerParent ! {'cleanup_failure_peer_close',
                              recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {100, 1024}),
                                 Parent ! {'cleanup_failure_result', self(),
                                           Result},
                                 receive 'stop_cleanup_failure_caller' -> 'ok'
                                 end
                         end),
              {GuardianPid, WorkerPid} = receive
                  {'bounded_guardian_started_ready', Guardian, Worker, _Lease,
                   Caller} ->
                      {Guardian, Worker}
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              receive
                  {'cleanup_failure_request_received', _ServerPid} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              Ref = owned_request_ref(WorkerPid),
              ManagerWatcher = suspend_manager(),
              try
                  receive
                      {'bounded_guardian_cleanup', 'started', GuardianPid,
                       'undefined'} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  {'reductions', ReductionsBefore} =
                      process_info(GuardianPid, reductions),
                  receive
                      {'bounded_guardian_cleanup', 'failed', GuardianPid,
                       'undefined'} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  {'reductions', ReductionsAfter} =
                      process_info(GuardianPid, reductions),
                  ?assert(ReductionsAfter - ReductionsBefore < 100000),
                  receive
                      {'cleanup_failure_result', Caller, Result} ->
                          ?assertEqual(
                             {'error', 'transport_cleanup_timeout'}, Result)
                  after 200 ->
                          ?assert('false')
                  end,
                  ?assertEqual('true', is_process_alive(GuardianPid)),
                  ?assertMatch([_], ets:lookup('hackney_manager_refs', Ref))
              after
                  'ok' = resume_manager(ManagerWatcher)
              end,
              Caller ! 'stop_cleanup_failure_caller',
              receive
                  {'bounded_guardian_cleanup', 'started', GuardianPid,
                   'undefined'} -> 'ok'
              after 500 ->
                      ?assert('false')
              end,
              receive
                  {'bounded_guardian_cleanup', 'complete', GuardianPid,
                   'undefined'} -> 'ok'
              after 500 ->
                      ?assert('false')
              end,
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'cleanup_failure_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 500 ->
                      ?assert('false')
              end,
              assert_process_gone(GuardianPid)
      end).

bounded_view_cleanup_uses_guardian_deadline_when_manager_suspended_test_() ->
    {'timeout', 30, fun bounded_view_cleanup_uses_guardian_deadline_when_manager_suspended/0}.

bounded_view_cleanup_uses_guardian_deadline_when_manager_suspended() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'view_cleanup_request_received', self()},
              ServerParent ! {'view_cleanup_peer_close',
                              recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {100, 1024}),
                                 Parent ! {'view_cleanup_result', self(),
                                           Result},
                                 receive
                                     'inspect_view_cleanup_mailbox' ->
                                         {'messages', Messages} =
                                             process_info(self(), messages),
                                         Parent ! {
                                           'view_cleanup_mailbox', self(),
                                           Messages}
                                 end,
                                 receive 'stop_view_cleanup_caller' -> 'ok' end
                         end),
              {GuardianPid, WorkerPid, ViewPid} = receive
                  {'bounded_guardian_started_ready', Guardian, Worker, _Lease,
                   StreamPid} ->
                      {Guardian, Worker, StreamPid}
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(ViewPid =/= Caller),
              GuardianMonitor = erlang:monitor('process', GuardianPid),
              receive
                  {'view_cleanup_request_received', _ServerPid} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              Ref = owned_request_ref(WorkerPid),
              ManagerWatcher = suspend_manager(),
              try
                  receive
                      {'bounded_guardian_cleanup', 'started', GuardianPid,
                       'undefined'} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  receive
                      {'bounded_guardian_cleanup', 'failed', GuardianPid,
                       'undefined'} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  receive
                      {'view_cleanup_result', Caller, Result} ->
                          ?assertEqual(
                             {'error', 'transport_cleanup_timeout'}, Result)
                  after 500 ->
                          ?assert('false')
                  end,
                  ?assertEqual('true', is_process_alive(GuardianPid)),
                  ?assertMatch([_], ets:lookup('hackney_manager_refs', Ref))
              after
                  'ok' = resume_manager(ManagerWatcher)
              end,
              receive
                  {'bounded_guardian_cleanup', 'started', GuardianPid,
                   'undefined'} -> 'ok'
              after 500 ->
                      ?assert('false')
              end,
              receive
                  {'bounded_guardian_cleanup', 'complete', GuardianPid,
                   'undefined'} -> 'ok'
              after 500 ->
                      ?assert('false')
              end,
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'view_cleanup_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 500 ->
                      ?assert('false')
              end,
              Caller ! 'inspect_view_cleanup_mailbox',
              receive
                  {'view_cleanup_mailbox', Caller, []} -> 'ok';
                  {'view_cleanup_mailbox', Caller, Messages} ->
                      ?assertEqual([], Messages)
              after 200 ->
                      ?assert('false')
              end,
              Caller ! 'stop_view_cleanup_caller',
              receive
                  {'DOWN', GuardianMonitor, 'process', GuardianPid, _} -> 'ok'
              after 200 ->
                      ?assert('false')
              end
      end).

bounded_body_close_propagates_cleanup_timeout_for_concrete_ref_test_() ->
    {'timeout', 30, fun bounded_body_close_propagates_cleanup_timeout_for_concrete_ref/0}.

bounded_body_close_propagates_cleanup_timeout_for_concrete_ref() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'body_cleanup_request_ready', self()},
              receive 'send_body_cleanup_response' -> 'ok' end,
              'ok' = send_json_response(Socket, Body),
              _ = recv_until_closed(Socket),
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent},
                          {'bounded_handoff_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {100, 1024}),
                                 Parent ! {'body_cleanup_result', self(),
                                           Result},
                                 receive
                                     'inspect_body_cleanup_mailbox' ->
                                         {'messages', Messages} =
                                             process_info(self(), messages),
                                         Parent ! {
                                           'body_cleanup_mailbox', self(),
                                           Messages}
                                 end,
                                 receive 'stop_body_cleanup_caller' -> 'ok' end
                         end),
              {GuardianPid, LeasePid} = receive
                  {'bounded_guardian_started_ready', Guardian, _Worker, Lease,
                   Caller} ->
                      Guardian ! {'bounded_guardian_started_continue',
                                  Guardian},
                      {Guardian, Lease}
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              Ref = receive
                        {'bounded_handoff_ready', CapturedRef, Caller} ->
                            Caller ! {'bounded_handoff_continue', CapturedRef},
                            CapturedRef
                    after 1000 ->
                            ?assert('false')
                    end,
              await_ref_owner(Ref, LeasePid),
              ServerPid = receive
                              {'body_cleanup_request_ready', Pid} -> Pid
                          after 1000 ->
                                  ?assert('false')
                          end,
              ManagerWatcher = suspend_manager(),
              try
                  ServerPid ! 'send_body_cleanup_response',
                  receive
                      {'bounded_guardian_cleanup', 'started', GuardianPid,
                       Ref} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  receive
                      {'bounded_guardian_cleanup', 'failed', GuardianPid,
                       Ref} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  receive
                      {'body_cleanup_result', Caller, Result} ->
                          ?assertEqual(
                             {'error', 'transport_cleanup_timeout'}, Result)
                  after 300 ->
                          ?assert('false')
                  end,
                  ?assertMatch([_], ets:lookup('hackney_manager_refs', Ref))
              after
                  'ok' = resume_manager(ManagerWatcher)
              end,
              receive
                  {'bounded_guardian_cleanup', 'complete', GuardianPid, Ref} ->
                      'ok'
              after 500 ->
                      ?assert('false')
              end,
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              Caller ! 'inspect_body_cleanup_mailbox',
              receive
                  {'body_cleanup_mailbox', Caller, Messages} ->
                      ?assertEqual('false', has_cleanup_ack(Messages))
              after 200 ->
                      ?assert('false')
              end,
              Caller ! 'stop_body_cleanup_caller'
      end).

bounded_known_status_propagates_cleanup_timeout_for_concrete_ref_test_() ->
    {'timeout', 30, fun bounded_known_status_propagates_cleanup_timeout_for_concrete_ref/0}.

bounded_known_status_propagates_cleanup_timeout_for_concrete_ref() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'status_cleanup_request_ready', self()},
              receive 'send_status_cleanup_response' -> 'ok' end,
              'ok' = send_json_headers(Socket, 404, 0),
              ServerParent ! {'status_cleanup_peer_close',
                              recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent},
                          {'bounded_handoff_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'status_cleanup_result', self(),
                                           couchbeam:db_info_bounded(
                                             Db, {100, 1024})}
                         end),
              {GuardianPid, LeasePid} = receive
                  {'bounded_guardian_started_ready', Guardian, _Worker, Lease,
                   Caller} ->
                      Guardian ! {'bounded_guardian_started_continue',
                                  Guardian},
                      {Guardian, Lease}
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              Ref = receive
                        {'bounded_handoff_ready', CapturedRef, Caller} ->
                            Caller ! {'bounded_handoff_continue', CapturedRef},
                            CapturedRef
                    after 1000 ->
                            ?assert('false')
                    end,
              await_ref_owner(Ref, LeasePid),
              ServerPid = receive
                              {'status_cleanup_request_ready', Pid} -> Pid
                          after 1000 ->
                                  ?assert('false')
                          end,
              ManagerWatcher = suspend_manager(),
              try
                  ServerPid ! 'send_status_cleanup_response',
                  receive
                      {'bounded_guardian_cleanup', 'started', GuardianPid,
                       Ref} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  receive
                      {'bounded_guardian_cleanup', 'failed', GuardianPid,
                       Ref} -> 'ok'
                  after 500 ->
                          ?assert('false')
                  end,
                  receive
                      {'status_cleanup_result', Caller, Result} ->
                          ?assertEqual(
                             {'error', 'transport_cleanup_timeout'}, Result)
                  after 300 ->
                          ?assert('false')
                  end,
                  ?assertMatch([_], ets:lookup('hackney_manager_refs', Ref))
              after
                  'ok' = resume_manager(ManagerWatcher)
              end,
              receive
                  {'bounded_guardian_cleanup', 'complete', GuardianPid, Ref} ->
                      'ok'
              after 500 ->
                      ?assert('false')
              end,
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'status_cleanup_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 500 ->
                      ?assert('false')
              end
      end).

bounded_ref_registration_ack_timeout_compensates_without_exit_test_() ->
    {'timeout', 30, fun bounded_ref_registration_ack_timeout_compensates_without_exit/0}.

bounded_ref_registration_ack_timeout_compensates_without_exit() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'ref_ack_request_ready', self()},
              ServerParent ! {'ref_ack_peer_close', recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_ref_ack_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {100, 1024}),
                                 Parent ! {'ref_ack_result', self(), Result}
                         end),
              {GuardianPid, Ref} = receive
                        {'bounded_guardian_ref_ack_ready', Guardian,
                         CapturedRef, Caller} -> {Guardian, CapturedRef}
                    after 1000 ->
                            exit(Caller, 'kill'),
                            ?assert('false')
                    end,
              receive
                  {'ref_ack_request_ready', _ServerPid} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              receive
                  {'bounded_guardian_ref_ack_timeout', GuardianPid, Ref,
                   Caller} ->
                      GuardianPid ! {'bounded_guardian_ref_ack_continue',
                                     GuardianPid, Ref}
              after 500 ->
                      ?assert('false')
              end,
              receive
                  {'bounded_guardian_ref_ack_sent', GuardianPid, Ref} ->
                      Caller ! {
                        'bounded_guardian_ref_ack_timeout_continue',
                        GuardianPid, Ref}
              after 200 ->
                      ?assert('false')
              end,
              receive
                  {'ref_ack_result', Caller, Result} ->
                      %% the guardian proved the cleanup on request (the row
                      %% is gone, below), so the verdict is the deadline —
                      %% `transport_cleanup_timeout' would claim a proof that
                      %% was in fact obtained
                      ?assertEqual({'error', 'timeout'}, Result)
              after 500 ->
                      ?assert('false')
              end,
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'ref_ack_peer_close', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 500 ->
                      ?assert('false')
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

bounded_late_handoff_is_compensated_before_return_test_() ->
    {'timeout', 30, fun bounded_late_handoff_is_compensated_before_return/0}.

bounded_late_handoff_is_compensated_before_return() ->
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
                      ManagerWatcher = suspend_manager(),
                      try
                          Caller ! {'test_ref', Ref},
                          Caller ! {'bounded_handoff_continue', Ref},
                          timer:sleep(120)
                      after
                          'ok' = resume_manager(ManagerWatcher)
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

bounded_view_decode_watchdog_cancels_transport_test_() ->
    {'timeout', 30, fun bounded_view_decode_watchdog_cancels_transport/0}.

bounded_view_decode_watchdog_cancels_transport() ->
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
                                            1000}], {50, 1024})
                                end),
              receive
                  {'captured_view_result', Caller, Ref, Result, RefAbsent} ->
                      ?assertEqual({'error', 'timeout'}, Result),
                      ?assertEqual('true', RefAbsent)
              after 1000 ->
                      ?assert('false')
              end,
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              ?assert(Elapsed < 500),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                  ?assert('false')
              end
      end).

bounded_view_status_transport_error_closes_peer_test_() ->
    {'timeout', 30, fun bounded_view_status_transport_error_closes_peer/0}.

bounded_view_status_transport_error_closes_peer() ->
    assert_view_transport_error_closed('status').

bounded_view_body_transport_error_closes_peer_test_() ->
    {'timeout', 30, fun bounded_view_body_transport_error_closes_peer/0}.

bounded_view_body_transport_error_closes_peer() ->
    assert_view_transport_error_closed('body').

%% --- round 2 regressions ---------------------------------------------------

legacy_call_ignores_well_formed_budget_in_db_options_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
              %% open_db/3 merges the server options into #db.options, so a
              %% budget parked there reaches every legacy call site
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'},
                                   {'request_budget', Budget}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% legacy shape, not the bounded triple, and no lease-owned Ref
              ?assertEqual({'ok', {[{<<"db_name">>, <<"db">>}]}},
                           couchbeam:db_info(Db)),
              assert_no_stray_transport_messages()
      end).

legacy_view_stream_ignores_budget_in_db_options_test() ->
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'},
                                   {'request_budget', Budget}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ExpectedRow = {[{<<"id">>, <<"doc">>},
                              {<<"key">>, <<"doc">>},
                              {<<"value">>, {[]}}]},
              ?assertEqual({'ok', [ExpectedRow]},
                           couchbeam_view:fetch(Db, 'all_docs', [])),
              assert_no_lifecycle_messages()
      end).

legacy_call_ignores_expired_budget_in_db_options_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Expired = #{'deadline_ms' =>
                              erlang:monotonic_time('millisecond') - 1000,
                          'timeout_ms' => 1000,
                          'max_response_bytes' => 1024},
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'},
                                   {'request_budget', Expired}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% an expired budget must not fail an unbounded call
              ?assertEqual({'ok', {[{<<"db_name">>, <<"db">>}]}},
                           couchbeam:db_info(Db))
      end).

bounded_budget_overrides_hackney_default_recv_timeout_test_() ->
    {'timeout', 30, fun bounded_budget_overrides_hackney_default_recv_timeout/0}.

bounded_budget_overrides_hackney_default_recv_timeout() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              %% longer than hackney's own 5000 ms default recv_timeout and
              %% well inside the 15 s budget the caller asked for
              timer:sleep(7000),
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% the budget, not the library default, decides how long a
              %% bounded call is willing to wait
              ?assertEqual(
                 {'ok', {[{<<"db_name">>, <<"db">>}]}, byte_size(Body)},
                 couchbeam:db_info_bounded(Db, {15000, 1024}))
      end).

legacy_view_stream_delivers_to_stream_to_pid_test_() ->
    {'timeout', 30, fun legacy_view_stream_delivers_to_stream_to_pid/0}.

legacy_view_stream_delivers_to_stream_to_pid() ->
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              send_json_response(Socket, Body),
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Consumer = spawn(
                           fun() ->
                                   Ref = receive {'stream_ref', R} -> R end,
                                   Parent ! {'consumer_result', self(),
                                             collect_legacy_stream(Ref, [])}
                           end),
              %% the documented delivery mode of the legacy stream: rows go to
              %% the named process, not to the caller
              {'ok', Ref} = couchbeam_view:stream(
                              Db, 'all_docs', [{'stream_to', Consumer}]),
              Consumer ! {'stream_ref', Ref},
              ExpectedRow = {[{<<"id">>, <<"doc">>},
                              {<<"key">>, <<"doc">>},
                              {<<"value">>, {[]}}]},
              receive
                  {'consumer_result', Consumer, Result} ->
                      ?assertEqual({'ok', [ExpectedRow]}, Result)
              after 5000 ->
                      ?assert('false')
              end,
              receive
                  {Ref, _Unexpected} -> ?assert('false')
              after 0 ->
                      'ok'
              end
      end).

bounded_save_doc_rejects_response_without_id_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"ok\":true,\"rev\":\"1-a\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 201, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% without the guard the caller's own _id is overwritten with
              %% the atom undefined
              ?assertEqual(
                 {'error', {'invalid_response', 'missing_id'}},
                 couchbeam:save_doc_bounded(
                   Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024}))
      end).

bounded_save_doc_accepts_attachment_stubs_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Response = <<"{\"ok\":true,\"id\":\"doc\",\"rev\":\"2-b\"}">>,
    %% exactly the shape a plain read returns: a stub, no inline data
    Stubs = {[{<<"a.txt">>,
               {[{<<"content_type">>, <<"text/plain">>},
                 {<<"length">>, 3},
                 {<<"stub">>, 'true'}]}}]},
    Doc = {[{<<"_id">>, <<"doc">>}, {<<"_attachments">>, Stubs}]},
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Response),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% a document that has attachments must stay writable
              {'ok', Saved, _Bytes} = couchbeam:save_doc_bounded(
                                        Db, Doc, [], {1000, 1024}),
              ?assertEqual(<<"2-b">>, couchbeam_doc:get_rev(Saved))
      end).

request_ignores_expired_budget_option_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Expired = #{'deadline_ms' =>
                              erlang:monotonic_time('millisecond') - 1000,
                          'timeout_ms' => 1000,
                          'max_response_bytes' => 1024},
              %% the unbounded entry point takes no budget; an entry of that
              %% name in its options is inert caller data
              ?assertMatch(
                 {'ok', 200, _Headers, _Ref},
                 couchbeam_httpc:request(
                   'get', <<BaseUrl/binary, "/db">>, [], <<>>,
                   [{'no_proxy_env', 'true'},
                    {'request_budget', Expired}]))
      end).

bounded_body_of_exactly_max_bytes_succeeds_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% the cap is inclusive: exactly MaxBytes must not be refused
              ?assertEqual(
                 {'ok', {[{<<"db_name">>, <<"db">>}]}, byte_size(Body)},
                 couchbeam:db_info_bounded(Db, {1000, byte_size(Body)}))
      end).

bounded_view_of_exactly_max_bytes_succeeds_test() ->
    ensure_couchbeam_supervisor(),
    Body = <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
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
                 {'ok', [], byte_size(Body)},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {1000, byte_size(Body)}))
      end).

bounded_view_preserves_row_order_across_chunks_test() ->
    ensure_couchbeam_supervisor(),
    %% two rows in the first chunk pin the stream-side ordering, the third
    %% one in a later chunk pins the collector-side ordering
    FirstChunk = <<"{\"total_rows\":3,\"offset\":0,\"rows\":[{\"id\":\"a\",\"key\":1,\"value\":{}},{\"id\":\"b\",\"key\":2,\"value\":{}},">>,
    SecondChunk = <<"{\"id\":\"c\",\"key\":3,\"value\":{}}">>,
    ThirdChunk = <<"]}">>,
    BodySize = byte_size(FirstChunk) + byte_size(SecondChunk)
        + byte_size(ThirdChunk),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, FirstChunk),
              timer:sleep(10),
              'ok' = send_http_chunk(Socket, SecondChunk),
              timer:sleep(10),
              'ok' = send_http_chunk(Socket, ThirdChunk),
              'ok' = send_http_chunk(Socket, <<>>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Row = fun(Id, Key) ->
                            {[{<<"id">>, Id}, {<<"key">>, Key},
                              {<<"value">>, {[]}}]}
                    end,
              %% order is the order CouchDB emitted, across chunk boundaries
              ?assertEqual(
                 {'ok', [Row(<<"a">>, 1), Row(<<"b">>, 2), Row(<<"c">>, 3)],
                  BodySize},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {2000, 1024}))
      end).

bounded_view_accepts_whitespace_only_trailing_chunk_test() ->
    ensure_couchbeam_supervisor(),
    Json = <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
    Trailing = <<"\r\n  \t\n">>,
    BodySize = byte_size(Json) + byte_size(Trailing),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, Json),
              timer:sleep(10),
              'ok' = send_http_chunk(Socket, Trailing),
              'ok' = send_http_chunk(Socket, <<>>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% JSON may finish before the HTTP body; whitespace after it is
              %% counted but not treated as trailing garbage
              ?assertEqual(
                 {'ok', [], BodySize},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {2000, BodySize}))
      end).

bounded_view_post_sends_keys_body_test() ->
    ensure_couchbeam_supervisor(),
    Keys = [<<"first">>, <<"second">>],
    Expected = couchbeam_ejson:encode({[{<<"keys">>, Keys}]}),
    Parent = self(),
    with_http_request_server(
      fun(Socket, Request, ServerParent) ->
              %% CouchDB answers 415 to a `_all_docs'/`_view' POST without it
              assert_request_header(
                <<"Content-Type">>, <<"application/json">>, Request),
              {'ok', RequestBody} = recv_request_body(Socket, Request),
              Parent ! {'received_post_body', RequestBody},
              send_json_response(
                Socket, <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>),
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch({'ok', [], _},
                           couchbeam_view:fetch_bounded(
                             Db, 'all_docs', [{'keys', Keys}], {2000, 1024})),
              receive
                  {'received_post_body', RequestBody} ->
                      ?assertEqual(Expected, RequestBody)
              after 1000 ->
                      ?assert('false')
              end
      end).

bounded_view_decoder_dies_with_its_stream_test_() ->
    {'timeout', 30, fun bounded_view_decoder_dies_with_its_stream/0}.

bounded_view_decoder_dies_with_its_stream() ->
    ensure_couchbeam_supervisor(),
    Parent = self(),
    Json = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, Json),
              ServerParent ! {'view_decoder_chunk_sent', self()},
              _ = recv_until_closed(Socket, 2000),
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 %% the decode delay is a stream option, so
                                 %% it travels with the fetch options
                                 _ = couchbeam_view:fetch_bounded(
                                       Db, 'all_docs',
                                       [{'bounded_decode_test_delay_ms',
                                         20000}], {60000, 4096}),
                                 receive 'never' -> 'ok' end
                         end),
              %% this hook only reports, it does not hold the guardian
              StreamPid = receive
                              {'bounded_upload_context', _W, _L, _G, Stream} ->
                                  Stream
                          after 2000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              ?assert(StreamPid =/= Caller),
              Decoders = await_condition(
                           fun() ->
                                   case view_decoder_processes() of
                                       [] -> 'false';
                                       Pids -> {'true', Pids}
                                   end
                           end, 5000),
              ?assertMatch([_ | _], Decoders),
              exit(StreamPid, 'kill'),
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case lists:any(fun erlang:is_process_alive/1,
                                          Decoders) of
                               'true' -> 'false';
                               'false' -> {'true', 'true'}
                           end
                   end, 2000)),
              exit(Caller, 'kill')
      end).

bounded_json_encoder_dies_with_its_caller_test_() ->
    {'timeout', 30, fun bounded_json_encoder_dies_with_its_caller/0}.

bounded_json_encoder_dies_with_its_caller() ->
    Doc = {[{<<"_id">>, <<"doc">>},
            {<<"payload">>, binary:copy(<<"x">>, 2000000)}]},
    {'ok', Budget} = couchbeam_httpc:new_request_budget({120000, 8000000}),
    Caller = spawn(
               fun() ->
                       _ = couchbeam_httpc:bounded_encode_json(
                             Doc, Budget,
                             [{'bounded_encode_test_delay_ms', 20000}]),
                       receive 'never' -> 'ok' end
               end),
    Encoders = await_condition(
                 fun() ->
                         case json_encoder_processes() of
                             [] -> 'false';
                             Pids -> {'true', Pids}
                         end
                 end, 5000),
    ?assertMatch([_ | _], Encoders),
    exit(Caller, 'kill'),
    ?assertEqual('true',
                 await_condition(
                   fun() ->
                           case lists:any(fun erlang:is_process_alive/1,
                                          Encoders) of
                               'true' -> 'false';
                               'false' -> {'true', 'true'}
                           end
                   end, 2000)).

bounded_db_info_rejects_non_object_body_test() ->
    assert_bounded_non_object_refused(
      fun(Db) -> couchbeam:db_info_bounded(Db, {1000, 1024}) end).

bounded_open_doc_rejects_non_object_body_test() ->
    assert_bounded_non_object_refused(
      fun(Db) -> couchbeam:open_doc_bounded(Db, <<"doc">>, [], {1000, 1024}) end).

bounded_save_doc_rejects_response_without_rev_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"ok\":true,\"id\":\"doc\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 202, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              %% a 202 without rev would otherwise put the atom undefined
              %% into _rev and the next save would send it as a revision
              ?assertEqual(
                 {'error', {'invalid_response', 'missing_rev'}},
                 couchbeam:save_doc_bounded(
                   Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024}))
      end).

bounded_save_doc_refuses_batch_option_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'batch'}},
      fun(Db) ->
              %% a batch write answers 202 without a revision, so executing it
              %% and rejecting the reply would hide whether it was applied
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{"batch", "ok"}],
                {1000, 1024})
      end).

%% Legacy `save_doc/3' picks multipart only for the explicit `Atts' argument
%% of `save_doc/4'; a document that carries inline base64 attachment data goes
%% out as one JSON encoding. The bounded write must send the same bytes, or a
%% document read with `attachments=true' becomes unwritable through it.
bounded_save_doc_sends_inline_attachment_data_like_legacy_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Doc = {[{<<"_id">>, <<"doc">>},
            {<<"_attachments">>,
             {[{<<"a.txt">>, {[{<<"content_type">>, <<"text/plain">>},
                               {<<"data">>, <<"eA==">>}]}}]}}]},
    Response = <<"{\"ok\":true,\"id\":\"doc\",\"rev\":\"2-b\"}">>,
    BoundedBody = received_put_body(
                    Response,
                    fun(Db) ->
                            {'ok', Saved, _Bytes} = couchbeam:save_doc_bounded(
                                                      Db, Doc, [], {1000, 1024}),
                            ?assertEqual(<<"2-b">>, couchbeam_doc:get_rev(Saved))
                    end),
    LegacyBody = received_put_body(
                   Response,
                   fun(Db) ->
                           {'ok', Saved} = couchbeam:save_doc(Db, Doc, []),
                           ?assertEqual(<<"2-b">>, couchbeam_doc:get_rev(Saved))
                   end),
    ?assertEqual(couchbeam_ejson:encode(Doc), BoundedBody),
    ?assertEqual(LegacyBody, BoundedBody).

%% Run `CallFun' against a server that answers one PUT with `Response' and
%% return the request body that server received.
received_put_body(Response, CallFun) ->
    Parent = self(),
    with_http_request_server(
      fun(Socket, Request, ServerParent) ->
              {'ok', RequestBody} = recv_request_body(Socket, Request),
              Parent ! {'received_put_body', RequestBody},
              send_json_response(Socket, Response),
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              CallFun(Db)
      end),
    receive
        {'received_put_body', Body} -> Body
    after 1000 ->
            erlang:error('put_body_not_received')
    end.

bounded_open_doc_refuses_binary_accept_param_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'accept'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{<<"accept">>, <<"multipart/related">>}],
                {1000, 1024})
      end).

bounded_open_doc_refuses_accept_before_budget_check_test() ->
    %% precedence is pinned: the unsupported parameter wins over an invalid
    %% budget, so callers get the actionable error
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'accept'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'accept', <<"application/json">>}], {0, 0})
      end).

bounded_request_refuses_follow_redirect_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_option', 'follow_redirect'}},
      fun(Db) ->
              %% hackney would answer a 3xx with redirect/see_other control
              %% messages the bounded reader has no clause for
              couchbeam:db_info_bounded(Db, {1000, 1024})
      end,
      [{'follow_redirect', 'true'}]).

bounded_db_info_maps_401_test() ->
    assert_bounded_status_mapping(
      401, {'error', 'unauthenticated'},
      fun(Db) -> couchbeam:db_info_bounded(Db, {1000, 1024}) end).

bounded_db_info_maps_403_test() ->
    assert_bounded_status_mapping(
      403, {'error', 'forbidden'},
      fun(Db) -> couchbeam:db_info_bounded(Db, {1000, 1024}) end).

bounded_save_doc_maps_412_test() ->
    assert_bounded_status_mapping(
      412, {'error', 'precondition_failed'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024})
      end).

assert_bounded_non_object_refused(CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"[1,2]">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual({'error', {'invalid_response', [1, 2]}},
                           CallFun(Db))
      end).

%% The two workers below are held inside their deliberate test delay. The
%% delay helper itself tail-calls `timer:sleep/1' and so leaves no frame; the
%% enclosing anonymous fun does, and its generated name carries the name of
%% the function that created it.
view_decoder_processes() ->
    processes_in('couchbeam_view_stream', "decode_data_bounded").

json_encoder_processes() ->
    processes_in('couchbeam_httpc', "bounded_encode_json").

processes_in(Module, NamePart) ->
    [Pid || Pid <- erlang:processes(),
            Pid =/= self(),
            has_stack_frame(process_info(Pid, 'current_stacktrace'),
                            Module, NamePart)].

has_stack_frame({'current_stacktrace', Frames}, Module, NamePart) ->
    lists:any(fun({M, F, _A, _Loc}) ->
                      M =:= Module
                          andalso string:find(atom_to_list(F), NamePart)
                          =/= 'nomatch';
                 (_) -> 'false'
              end, Frames);
has_stack_frame(_Other, _Module, _NamePart) ->
    'false'.

bounded_view_stream_death_before_guardian_start_reports_stream_down_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Parent = self(),
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try
        Server = couchbeam:server_connection(
                   BaseUrl,
                   [{'no_proxy_env', 'true'},
                    {'bounded_guardian_ready_test_hook', Parent}]),
        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
        Started = erlang:monotonic_time('millisecond'),
        Caller = spawn(
                   fun() ->
                           Result = couchbeam_view:fetch_bounded(
                                      Db, 'all_docs', [], {1000, 1024}),
                           {'messages', Messages} =
                               process_info(self(), 'messages'),
                           Parent ! {'stream_death_result', self(), Result,
                                     Messages}
                   end),
        {GuardianPid, StreamPid} =
            receive
                {'bounded_guardian_ready', Guardian, Stream} ->
                    {Guardian, Stream}
            after 1000 ->
                    exit(Caller, 'kill'),
                    ?assert('false')
            end,
        ?assert(StreamPid =/= Caller),
        GuardianMonitor = erlang:monitor('process', GuardianPid),
        %% the stream process dies before it ever tells the guardian to start
        exit(StreamPid, 'kill'),
        receive
            {'stream_death_result', Caller, Result, Messages} ->
                %% `killed' or `noproc', depending on whether the kill
                %% landed before the collector's monitor was placed: the
                %% property is the immediate `stream_down' verdict
                ?assertMatch({'error', {'stream_down', _}}, Result),
                ?assertEqual([], lifecycle_messages(Messages))
        after 1000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end,
        Elapsed = erlang:monotonic_time('millisecond') - Started,
        %% the verdict must not wait for the 1000 ms cleanup budget
        ?assert(Elapsed < 500),
        receive
            {'DOWN', GuardianMonitor, 'process', GuardianPid, _} -> 'ok'
        after 500 ->
                ?assert('false')
        end,
        ?assertEqual({'error', 'timeout'}, gen_tcp:accept(ListenSocket, 20))
    after
        gen_tcp:close(ListenSocket)
    end.

db_request_bounded_rejects_budget_without_timeout_ms_test() ->
    Malformed = #{'deadline_ms' => erlang:monotonic_time('millisecond') + 1000,
                  'max_response_bytes' => 1024},
    ?assertEqual(
       {'error', 'invalid_request_budget'},
       couchbeam_httpc:db_request_bounded(
         'get', <<"http://127.0.0.1:1/db">>, [], <<>>, [], [200], Malformed)).

bounded_json_body_rejects_budget_without_timeout_ms_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Malformed = #{'deadline_ms' => erlang:monotonic_time('millisecond') + 1000,
                  'max_response_bytes' => 1024},
    ?assertEqual(
       {'error', 'invalid_request_budget'},
       couchbeam_httpc:bounded_json_body(make_ref(), Malformed)).

legacy_call_ignores_malformed_budget_in_db_options_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              Malformed = #{'deadline_ms' =>
                                erlang:monotonic_time('millisecond') + 1000,
                            'max_response_bytes' => 1024},
              {'ok', Db} = couchbeam:open_db(
                             Server, <<"db">>,
                             [{'request_budget', Malformed}]),
              %% caller data, not an instruction: a legacy call neither goes
              %% bounded nor fails on a budget it never asked for
              ?assertEqual({'ok', {[{<<"db_name">>, <<"db">>}]}},
                           couchbeam:db_info(Db))
      end).

new_request_budget_rejects_timeout_beyond_timer_range_test() ->
    Ceiling = 16#FFFFFFFF div 3,
    {'ok', #{'timeout_ms' := Ceiling}=Budget} =
        couchbeam_httpc:new_request_budget({Ceiling, 1}),
    ?assertEqual({'error', 'invalid_request_budget'},
                 couchbeam_httpc:new_request_budget({Ceiling + 1, 1})),
    %% the longest derived wait — the collector's delivery allowance on top
    %% of a fresh cleanup budget — must still fit `receive … after'
    Now = erlang:monotonic_time('millisecond'),
    Cleanup = Budget#{'deadline_ms' => Now + Ceiling},
    #{'deadline_ms' := Delivery} =
        couchbeam_view:bounded_cleanup_delivery_budget(Cleanup),
    ?assert(Delivery - Now =< 16#FFFFFFFF),
    ?assertEqual({'error', 'invalid_request_budget'},
                 couchbeam_httpc:new_request_budget({0, 1})),
    ?assertEqual({'error', 'invalid_request_budget'},
                 couchbeam_httpc:new_request_budget({1000, 0})).

request_bounded_budget_argument_overrides_stale_option_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Expired = #{'deadline_ms' =>
                              erlang:monotonic_time('millisecond') - 1000,
                          'timeout_ms' => 1000,
                          'max_response_bytes' => 1024},
              {'ok', Fresh} = couchbeam_httpc:new_request_budget({1000, 1024}),
              Options = [{'no_proxy_env', 'true'},
                         {'request_budget', Expired}],
              {'ok', Ref} = couchbeam_httpc:request_bounded(
                              'get', <<BaseUrl/binary, "/db">>, [], <<>>,
                              Options, Fresh),
              %% async-once protocol: status and headers precede the body
              receive
                  {'hackney_response', Ref, {'status', 200, _}} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              'ok' = hackney:stream_next(Ref),
              receive
                  {'hackney_response', Ref, {'headers', _}} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              ?assertEqual(
                 {'ok', {[{<<"db_name">>, <<"db">>}]}, byte_size(Body)},
                 couchbeam_httpc:bounded_json_body(Ref, Fresh)),
              assert_no_stray_transport_messages()
      end).

bounded_connection_refused_leaves_no_stray_messages_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    'ok' = gen_tcp:close(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    Server = couchbeam:server_connection(BaseUrl, [{'no_proxy_env', 'true'}]),
    {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
    ?assertEqual({'error', 'econnrefused'},
                 couchbeam:db_info_bounded(Db, {1000, 1024})),
    ?assertEqual({'error', 'econnrefused'},
                 couchbeam:open_doc_bounded(Db, <<"doc">>, [], {1000, 1024})),
    ?assertEqual({'error', 'econnrefused'},
                 couchbeam:save_doc_bounded(
                   Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024})),
    assert_no_stray_transport_messages().

bounded_stuck_worker_release_returns_typed_cleanup_timeout_test_() ->
    {'timeout', 30, fun bounded_stuck_worker_release_returns_typed_cleanup_timeout/0}.

bounded_stuck_worker_release_returns_typed_cleanup_timeout() ->
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
                          {'bounded_upload_test_hook', Parent},
                          {'bounded_handoff_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {300, 1024}),
                                 Parent ! {'stuck_worker_result', self(),
                                           Result}
                         end),
              WorkerPid = receive
                              {'bounded_upload_worker', Worker} -> Worker
                          after 1000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              Ref = receive
                        {'bounded_handoff_ready', HandoffRef, Caller} ->
                            HandoffRef
                    after 1000 ->
                            exit(Caller, 'kill'),
                            ?assert('false')
                    end,
              %% the worker already returned {ok, Ref} and waits for its
              %% release; freeze it so the release can never be observed
              'true' = erlang:suspend_process(WorkerPid),
              try
                  Caller ! {'bounded_handoff_continue', Ref},
                  receive
                      {'stuck_worker_result', Caller, Result} ->
                          ?assertEqual(
                             {'error', 'transport_cleanup_timeout'}, Result)
                  after 2000 ->
                          ?assert('false')
                  end
              after
                  catch erlang:resume_process(WorkerPid)
              end,
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              ?assertEqual('false', is_process_alive(WorkerPid)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

bounded_json_decoder_dies_with_its_caller_test_() ->
    {'timeout', 30, fun bounded_json_decoder_dies_with_its_caller/0}.

bounded_json_decoder_dies_with_its_caller() ->
    Body = iolist_to_binary(
             [<<"[">>, lists:duplicate(3000000, <<"0,">>), <<"0]">>]),
    {'ok', Budget} = couchbeam_httpc:new_request_budget(
                       {60000, byte_size(Body)}),
    Caller = spawn(
               fun() ->
                       _ = couchbeam_httpc:decode_bounded_json(
                             Body, byte_size(Body), Budget),
                       receive 'never' -> 'ok' end
               end),
    %% positive control: the decoder is observably running
    Decoders = await_condition(
                 fun() ->
                         case json_decoder_processes() of
                             [] -> 'false';
                             Pids -> {'true', Pids}
                         end
                 end, 5000),
    ?assertMatch([_ | _], Decoders),
    exit(Caller, 'kill'),
    ?assertEqual('true',
                 await_condition(
                   fun() ->
                           case lists:any(fun erlang:is_process_alive/1,
                                          Decoders) of
                               'true' -> 'false';
                               'false' -> {'true', 'true'}
                           end
                   end, 1000)).

bounded_save_doc_rejects_non_object_response_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"[]">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 201, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', {'invalid_response', []}},
                 couchbeam:save_doc_bounded(
                   Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024}))
      end).

bounded_open_doc_refuses_accept_param_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'accept'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'accept', <<"multipart/related">>}],
                {1000, 1024})
      end).

bounded_open_doc_rejects_invalid_budget_spec_test() ->
    assert_refused_before_transport(
      {'error', 'invalid_request_budget'},
      fun(Db) -> couchbeam:open_doc_bounded(Db, <<"doc">>, [], {0, 10}) end).

bounded_save_doc_without_id_is_refused_before_transport_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"value">>, 1}]}, [], {1000, 1024})
      end).

legacy_view_fetch_ignores_request_budget_option_test() ->
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
              {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
              ExpectedRow = {[{<<"id">>, <<"doc">>},
                              {<<"key">>, <<"doc">>},
                              {<<"value">>, {[]}}]},
              %% legacy shape, no raw byte count, no lifecycle protocol
              ?assertEqual(
                 {'ok', [ExpectedRow]},
                 couchbeam_view:fetch(
                   Db, 'all_docs', [{'request_budget', Budget}])),
              assert_no_lifecycle_messages()
      end).

bounded_open_doc_sends_query_params_test() ->
    assert_bounded_request_line(
      <<"GET /db/doc?rev=1-a HTTP/1.1">>,
      <<"{\"_id\":\"doc\",\"_rev\":\"1-a\"}">>,
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{<<"rev">>, <<"1-a">>}], {1000, 1024})
      end).

bounded_save_doc_sends_query_params_test() ->
    %% the option shape Kazoo's adapter forwards: string key, atom value
    assert_bounded_request_line(
      <<"PUT /db/doc?new_edits=true HTTP/1.1">>,
      <<"{\"ok\":true,\"id\":\"doc\",\"rev\":\"1-a\"}">>,
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{"new_edits", true}],
                {1000, 1024})
      end).

bounded_save_doc_returns_server_rev_and_sends_encoded_body_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Doc = {[{<<"_id">>, <<"doc">>}, {<<"value">>, 1}]},
    EncodedDoc = couchbeam_ejson:encode(Doc),
    Response = <<"{\"ok\":true,\"id\":\"doc\",\"rev\":\"3-c\"}">>,
    Parent = self(),
    with_http_request_server(
      fun(Socket, Request, ServerParent) ->
              assert_request_header(
                <<"Content-Type">>, <<"application/json">>, Request),
              {'ok', RequestBody} = recv_request_body(Socket, Request),
              Parent ! {'received_put_body', RequestBody},
              send_json_response(Socket, Response),
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              {'ok', Saved, Bytes} = couchbeam:save_doc_bounded(
                                       Db, Doc, [], {1000, 1024}),
              ?assertEqual(byte_size(Response), Bytes),
              ?assertEqual(<<"doc">>, couchbeam_doc:get_id(Saved)),
              ?assertEqual(<<"3-c">>, couchbeam_doc:get_rev(Saved)),
              ?assertEqual(1, couchbeam_doc:get_value(<<"value">>, Saved)),
              receive
                  {'received_put_body', RequestBody} ->
                      ?assertEqual(EncodedDoc, RequestBody)
              after 1000 ->
                      ?assert('false')
              end
      end).

bounded_db_info_maps_404_to_db_not_found_test() ->
    assert_bounded_status_mapping(
      404, {'error', 'db_not_found'},
      fun(Db) -> couchbeam:db_info_bounded(Db, {1000, 1024}) end).

bounded_save_doc_maps_409_to_conflict_test() ->
    assert_bounded_status_mapping(
      409, {'error', 'conflict'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024})
      end).

%% `db_info_bounded/2' renames this atom to `db_not_found'; the document
%% routes hand it to Kazoo verbatim, and Kazoo matches on it.
bounded_open_doc_maps_404_to_not_found_test() ->
    assert_bounded_status_mapping(
      404, {'error', 'not_found'},
      fun(Db) ->
              couchbeam:open_doc_bounded(Db, <<"doc">>, [], {1000, 1024})
      end).

bounded_save_doc_maps_404_to_not_found_test() ->
    assert_bounded_status_mapping(
      404, {'error', 'not_found'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024})
      end).

bounded_save_doc_refuses_null_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, 'null'}, {<<"value">>, 1}]}, [],
                {1000, 1024})
      end).

bounded_save_doc_refuses_empty_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<>>}, {<<"value">>, 1}]}, [],
                {1000, 1024})
      end).

bounded_save_doc_refuses_non_string_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, 42}, {<<"value">>, 1}]}, [],
                {1000, 1024})
      end).

%% an empty id would address `/db/' and hand back the database info object
bounded_open_doc_refuses_empty_doc_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) -> couchbeam:open_doc_bounded(Db, <<>>, [], {1000, 1024}) end).

bounded_open_doc_refuses_empty_string_doc_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) -> couchbeam:open_doc_bounded(Db, "", [], {1000, 1024}) end).

%% CouchDB answers `open_revs' with an array of revisions, never one object
bounded_open_doc_refuses_open_revs_param_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'open_revs'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'open_revs', 'all'}], {1000, 1024})
      end).

bounded_open_doc_refuses_binary_open_revs_param_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'open_revs'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{<<"open_revs">>, <<"all">>}], {1000, 1024})
      end).

bounded_fetch_design_view_sends_design_request_line_test() ->
    ensure_couchbeam_supervisor(),
    assert_bounded_request_line(
      <<"GET /db/_design/ddoc/_view/by_name HTTP/1.1">>,
      <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, {<<"ddoc">>, <<"by_name">>}, [], {1000, 4096})
      end).

%% A failure raised inside the fake server must reach the eunit process;
%% swallowed, it would leave the client asserting against a closed socket.
with_http_server_reports_server_side_failure_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ?assertError(
       {'assert', _},
       with_http_server(
         fun(_Socket, _Parent) -> ?assert('false') end,
         fun(BaseUrl) ->
                 Server = couchbeam:server_connection(
                            BaseUrl, [{'no_proxy_env', 'true'}]),
                 {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
                 _ = couchbeam:db_info(Db),
                 'ok'
         end)).

%% --- lifecycle owner, guardian and stream deaths (round 4) ---------------

%% The stream is held inside `bounded_encode_json/3' by the encode delay, so
%% no guardian exists when it is killed. The collector must answer at once:
%% nothing was announced, nothing can be cleaned.
bounded_view_stream_death_before_guardian_spawn_reports_stream_down_test_() ->
    {'timeout', 30, fun bounded_view_stream_death_before_guardian_spawn_reports_stream_down/0}.

bounded_view_stream_death_before_guardian_spawn_reports_stream_down() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try
        Server = couchbeam:server_connection(
                   BaseUrl,
                   [{'no_proxy_env', 'true'},
                    {'bounded_encode_test_delay_ms', 5000}]),
        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
        Before = view_stream_children(),
        Started = erlang:monotonic_time('millisecond'),
        Caller = spawn(
                   fun() ->
                           Result = couchbeam_view:fetch_bounded(
                                      Db, 'all_docs', [{'keys', [<<"k">>]}],
                                      {5000, 1024}),
                           {'messages', Messages} =
                               process_info(self(), 'messages'),
                           Parent ! {'pre_guardian_death_result', self(),
                                     Result, Messages}
                   end),
        StreamPid = await_condition(
                      fun() ->
                              case view_stream_children() -- Before of
                                  [Pid] -> {'true', Pid};
                                  _ -> 'false'
                              end
                      end, 2000),
        ?assert(is_pid(StreamPid)),
        %% the encoder worker is asleep: the stream never reached the guardian
        ?assertMatch([_ | _],
                     await_condition(
                       fun() ->
                               case json_encoder_processes() of
                                   [] -> 'false';
                                   Pids -> {'true', Pids}
                               end
                       end, 2000)),
        exit(StreamPid, 'kill'),
        receive
            {'pre_guardian_death_result', Caller, Result, Messages} ->
                %% `killed' or `noproc', depending on whether the kill
                %% landed before the collector's monitor was placed: the
                %% property is the immediate `stream_down' verdict
                ?assertMatch({'error', {'stream_down', _}}, Result),
                ?assertEqual([], lifecycle_messages(Messages))
        after 1000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end,
        Elapsed = erlang:monotonic_time('millisecond') - Started,
        %% the verdict must not wait for the 5000 ms cleanup budget
        ?assert(Elapsed < 1000),
        ?assertEqual({'error', 'timeout'}, gen_tcp:accept(ListenSocket, 20))
    after
        gen_tcp:close(ListenSocket)
    end.

view_stream_children() ->
    [Pid || {_Id, Pid, _Type, _Modules}
                <- supervisor:which_children('couchbeam_view_sup'),
            is_pid(Pid)].

%% The caller of `fetch_bounded/4' dies while the stream is blocked in
%% `couchbeam_httpc:await_bounded_request/7' — the position from which the
%% stream cannot read its owner monitor. The worker is held by a test seam
%% after its request went out (the transport exists) and before it reports
%% the ref. Kazoo kills its adapter worker exactly like this; the guardian's
%% lifecycle-owner monitor must close the transport at once.
bounded_view_collector_death_while_stream_awaits_worker_closes_transport_test_() ->
    {'timeout', 30,
     fun bounded_view_collector_death_while_stream_awaits_worker_closes_transport/0}.

bounded_view_collector_death_while_stream_awaits_worker_closes_transport() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'held_request_ready', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_worker_hold_test_hook', Parent},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 _ = couchbeam_view:fetch_bounded(
                                       Db, 'all_docs', [], {5000, 1024}),
                                 receive 'never' -> 'ok' end
                         end),
              {WorkerPid, LeasePid, GuardianPid, StreamPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Stream} ->
                          {Worker, Lease, Guardian, Stream}
                  after 2000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              receive
                  {'bounded_worker_held', WorkerPid} -> 'ok'
              after 2000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              receive
                  {'held_request_ready', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              ?assertEqual(
                 {'current_function',
                  {'couchbeam_httpc', 'await_bounded_request', 7}},
                 process_info(StreamPid, 'current_function')),
              Started = erlang:monotonic_time('millisecond'),
              exit(Caller, 'kill'),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end,
              %% closed by the owner's death, not by the 5000 ms deadline
              ?assert(erlang:monotonic_time('millisecond') - Started < 1000),
              ?assertEqual(
                 'settled',
                 await_condition(
                   fun() ->
                           case {is_process_alive(WorkerPid),
                                 is_process_alive(LeasePid),
                                 is_process_alive(GuardianPid),
                                 is_process_alive(StreamPid),
                                 ets:lookup('hackney_manager_refs', Ref)} of
                               {'false', 'false', 'false', 'false', []} ->
                                   {'true', 'settled'};
                               _ -> 'false'
                           end
                   end, 2000))
      end).

%% The caller dies while the stream is parked between chunks. Both the
%% guardian (owner monitor) and the stream (its own owner monitor) react; the
%% guardian finishes first and exits `normal', and the stream's cleanup
%% request must read that exit as the proof it is — not log a false
%% `couchbeam_bounded_owner_down_cleanup_unproven'.
bounded_view_collector_death_midstream_cleans_up_without_warning_test_() ->
    {'timeout', 30,
     fun bounded_view_collector_death_midstream_cleans_up_without_warning/0}.

bounded_view_collector_death_midstream_cleans_up_without_warning() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    FirstChunk = <<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"a\",\"key\":\"a\",\"value\":{}},">>,
    with_captured_warnings(
      fun() ->
              with_http_server(
                fun(Socket, ServerParent) ->
                        'ok' = send_chunked_headers(Socket),
                        'ok' = send_http_chunk(Socket, FirstChunk),
                        ServerParent ! {'midstream_chunk_sent', self()},
                        ServerParent ! {'peer_close_result',
                                        recv_until_closed(Socket, 3000)},
                        ServerParent ! {'server_done', self()}
                end,
                fun(BaseUrl) ->
                        collector_death_midstream_scenario(BaseUrl, Parent)
                end)
      end,
      fun(Warnings) ->
              ?assertEqual([], owner_down_unproven_warnings(Warnings))
      end).

collector_death_midstream_scenario(BaseUrl, Parent) ->
    Server = couchbeam:server_connection(
               BaseUrl,
               [{'no_proxy_env', 'true'},
                {'bounded_upload_context_test_hook', Parent}]),
    {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
    Caller = spawn(
               fun() ->
                       _ = couchbeam_view:fetch_bounded(
                             Db, 'all_docs', [], {5000, 4096}),
                       receive 'never' -> 'ok' end
               end),
    {WorkerPid, LeasePid, GuardianPid, StreamPid} =
        receive
            {'bounded_upload_context', Worker, Lease, Guardian, Stream} ->
                {Worker, Lease, Guardian, Stream}
        after 2000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end,
    Ref = owned_request_ref([WorkerPid, LeasePid]),
    receive
        {'midstream_chunk_sent', _ServerPid} -> 'ok'
    after 2000 ->
            exit(Caller, 'kill'),
            ?assert('false')
    end,
    %% the lease owns the transport and the stream waits for the next chunk
    'ok' = await_ref_owner(Ref, LeasePid),
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case process_info(StreamPid, 'current_function') of
                     {'current_function',
                      {'couchbeam_view_stream', 'bounded_loop_receive', 4}} ->
                         {'true', 'true'};
                     _ -> 'false'
                 end
         end, 2000)),
    Started = erlang:monotonic_time('millisecond'),
    exit(Caller, 'kill'),
    receive
        {'peer_close_result', PeerResult} ->
            ?assertEqual({'error', 'closed'}, PeerResult)
    after 3500 ->
            ?assert('false')
    end,
    Elapsed = erlang:monotonic_time('millisecond') - Started,
    ?assert(Elapsed < 1000),
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case {is_process_alive(StreamPid),
                       is_process_alive(GuardianPid),
                       is_process_alive(LeasePid),
                       ets:lookup('hackney_manager_refs', Ref)} of
                     {'false', 'false', 'false', []} -> {'true', 'true'};
                     _ -> 'false'
                 end
         end, 2000)).

owner_down_unproven_warnings(Warnings) ->
    [W || {'report',
           #{'event' := 'couchbeam_bounded_owner_down_cleanup_unproven'}}=W
              <- Warnings].

%% The caller dies while the stream waits for its decoder (held by the test
%% delay), a wait that reads no owner monitor. The guardian cleans up and is
%% long gone when the stream finally reacts: its cleanup request finds no
%% guardian to watch (`noproc') and must verify the closed transport against
%% the manager table instead of logging a false
%% `couchbeam_bounded_owner_down_cleanup_unproven'.
bounded_view_collector_death_during_decode_verifies_cleanup_without_warning_test_() ->
    {'timeout', 30,
     fun bounded_view_collector_death_during_decode_verifies_cleanup_without_warning/0}.

bounded_view_collector_death_during_decode_verifies_cleanup_without_warning() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    FirstChunk = <<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"a\",\"key\":\"a\",\"value\":{}},">>,
    with_captured_warnings(
      fun() ->
              with_http_server(
                fun(Socket, ServerParent) ->
                        'ok' = send_chunked_headers(Socket),
                        'ok' = send_http_chunk(Socket, FirstChunk),
                        ServerParent ! {'peer_close_result',
                                        recv_until_closed(Socket, 3000)},
                        ServerParent ! {'server_done', self()}
                end,
                fun(BaseUrl) ->
                        collector_death_during_decode_scenario(BaseUrl, Parent)
                end)
      end,
      fun(Warnings) ->
              ?assertEqual([], owner_down_unproven_warnings(Warnings))
      end).

collector_death_during_decode_scenario(BaseUrl, Parent) ->
    Server = couchbeam:server_connection(
               BaseUrl,
               [{'no_proxy_env', 'true'},
                {'bounded_upload_context_test_hook', Parent}]),
    {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
    Caller = spawn(
               fun() ->
                       _ = couchbeam_view:fetch_bounded(
                             Db, 'all_docs',
                             [{'bounded_decode_test_delay_ms', 3000}],
                             {10000, 4096}),
                       receive 'never' -> 'ok' end
               end),
    {WorkerPid, LeasePid, GuardianPid, StreamPid} =
        receive
            {'bounded_upload_context', Worker, Lease, Guardian, Stream} ->
                {Worker, Lease, Guardian, Stream}
        after 2000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end,
    Ref = owned_request_ref([WorkerPid, LeasePid]),
    'ok' = await_ref_owner(Ref, LeasePid),
    %% the decoder is asleep inside its test delay; the stream waits for it
    ?assertMatch([_ | _],
                 await_condition(
                   fun() ->
                           case view_decoder_processes() of
                               [] -> 'false';
                               Pids -> {'true', Pids}
                           end
                   end, 5000)),
    Started = erlang:monotonic_time('millisecond'),
    exit(Caller, 'kill'),
    %% the guardian closes the transport on the owner's death, not the stream
    receive
        {'peer_close_result', PeerResult} ->
            ?assertEqual({'error', 'closed'}, PeerResult)
    after 3500 ->
            ?assert('false')
    end,
    ?assert(erlang:monotonic_time('millisecond') - Started < 1000),
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case is_process_alive(GuardianPid) of
                     'false' -> {'true', 'true'};
                     'true' -> 'false'
                 end
         end, 2000)),
    %% the stream reads the owner's death at once instead of waiting for its
    %% decoder (asleep for 3000 ms in its test delay) to answer first
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case {is_process_alive(StreamPid),
                       ets:lookup('hackney_manager_refs', Ref)} of
                     {'false', []} -> {'true', 'true'};
                     _ -> 'false'
                 end
         end, 500)),
    ?assert(erlang:monotonic_time('millisecond') - Started < 1500),
    ?assertEqual([], view_decoder_processes()).

%% The caller dies while the guardian is held just before it acknowledges the
%% registered ref: the guardian cleans up on the owner's death and exits
%% `normal' without ever acknowledging. The stream, blocked in
%% `couchbeam_httpc:guardian_register_ref/5', must read that exit at once
%% instead of waiting for the acknowledgement until the deadline.
bounded_view_collector_death_during_ref_registration_test_() ->
    {'timeout', 30, fun bounded_view_collector_death_during_ref_registration/0}.

bounded_view_collector_death_during_ref_registration() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'ref_registration_request_ready', self()},
              receive 'observe_close' -> 'ok' end,
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_ref_ack_test_hook', Parent},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 _ = couchbeam_view:fetch_bounded(
                                       Db, 'all_docs', [], {5000, 1024}),
                                 receive 'never' -> 'ok' end
                         end),
              {WorkerPid, LeasePid, GuardianPid, StreamPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Stream} ->
                          {Worker, Lease, Guardian, Stream}
                  after 2000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              Ref = receive
                        {'bounded_guardian_ref_ack_ready', GuardianPid,
                         CapturedRef, StreamPid} -> CapturedRef
                    after 2000 ->
                            exit(Caller, 'kill'),
                            ?assert('false')
                    end,
              ServerPid = receive
                              {'ref_registration_request_ready', Pid} -> Pid
                          after 1000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              Started = erlang:monotonic_time('millisecond'),
              exit(Caller, 'kill'),
              ServerPid ! 'observe_close',
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end,
              ?assertEqual(
                 'settled',
                 await_condition(
                   fun() ->
                           case {is_process_alive(WorkerPid),
                                 is_process_alive(LeasePid),
                                 is_process_alive(GuardianPid),
                                 is_process_alive(StreamPid),
                                 ets:lookup('hackney_manager_refs', Ref)} of
                               {'false', 'false', 'false', 'false', []} ->
                                   {'true', 'settled'};
                               _ -> 'false'
                           end
                   end, 2000)),
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              %% the stream's verdict must not wait for the 5000 ms deadline
              ?assert(Elapsed < 1500)
      end).

%% A guardian killed before its cleanup ran must take the upload worker and
%% the lease with it, or the socket the worker owns survives them all. The
%% worker is held by a test seam after its request went out and before it
%% reports the ref to the stream — the only way to make "before the hand-off"
%% deterministic: a large unread upload does not block `hackney:request/5'
%% on this platform, the inet driver queues it and hackney returns `{ok, Ref}'
%% at once. With no ref handed over, the cancel path has nothing to verify
%% and reports the cleanup unproven.
bounded_guardian_death_before_ref_handoff_reports_unproven_cleanup_test_() ->
    {'timeout', 30,
     fun bounded_guardian_death_before_ref_handoff_reports_unproven_cleanup/0}.

bounded_guardian_death_before_ref_handoff_reports_unproven_cleanup() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'held_request_ready', self()},
              receive 'observe_close' -> 'ok' end,
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_worker_hold_test_hook', Parent},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'guardian_death_result', self(),
                                           couchbeam_view:fetch_bounded(
                                             Db, 'all_docs', [], {1000, 1024})}
                         end),
              {WorkerPid, LeasePid, GuardianPid, StreamPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Stream} ->
                          {Worker, Lease, Guardian, Stream}
                  after 2000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              receive
                  {'bounded_worker_held', WorkerPid} -> 'ok'
              after 2000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ServerPid = receive
                              {'held_request_ready', Pid} -> Pid
                          after 1000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              ?assertEqual(
                 {'current_function',
                  {'couchbeam_httpc', 'await_bounded_request', 7}},
                 process_info(StreamPid, 'current_function')),
              exit(GuardianPid, 'kill'),
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case {is_process_alive(WorkerPid),
                                 is_process_alive(LeasePid),
                                 ets:lookup('hackney_manager_refs', Ref)} of
                               {'false', 'false', []} -> {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 500)),
              receive
                  {'guardian_death_result', Caller, Result} ->
                      ?assertEqual({'error', 'transport_cleanup_timeout'},
                                   Result)
              after 2500 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ServerPid ! 'observe_close',
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% The guardian is killed while it is held just before acknowledging the
%% registered ref: the stream, waiting in `guardian_register_ref/5', reads
%% the exit, verifies the closed transport against the manager table (the
%% lease died with its guardian) and reports `request_guardian_down' at once.
bounded_guardian_death_after_ref_handoff_reports_guardian_down_test_() ->
    {'timeout', 30, fun bounded_guardian_death_after_ref_handoff_reports_guardian_down/0}.

bounded_guardian_death_after_ref_handoff_reports_guardian_down() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'ref_ack_request_ready', self()},
              receive 'observe_close' -> 'ok' end,
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_ref_ack_test_hook', Parent},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {5000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'guardian_down_result', self(),
                                           Result, Messages}
                         end),
              {WorkerPid, LeasePid, GuardianPid, StreamPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Stream} ->
                          {Worker, Lease, Guardian, Stream}
                  after 2000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              Ref = receive
                        {'bounded_guardian_ref_ack_ready', GuardianPid,
                         CapturedRef, StreamPid} -> CapturedRef
                    after 2000 ->
                            exit(Caller, 'kill'),
                            ?assert('false')
                    end,
              ServerPid = receive
                              {'ref_ack_request_ready', Pid} -> Pid
                          after 1000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              Started = erlang:monotonic_time('millisecond'),
              exit(GuardianPid, 'kill'),
              receive
                  {'guardian_down_result', Caller, Result, Messages} ->
                      ?assertEqual({'error', 'request_guardian_down'}, Result),
                      ?assertEqual([], lifecycle_messages(Messages))
              after 2500 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% the verdict must not wait for the 5000 ms deadline
              ?assert(erlang:monotonic_time('millisecond') - Started < 1000),
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case {is_process_alive(WorkerPid),
                                 is_process_alive(LeasePid),
                                 ets:lookup('hackney_manager_refs', Ref)} of
                               {'false', 'false', []} -> {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 1000)),
              ServerPid ! 'observe_close',
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% The guardian is gone before the caller closes its request, so the cleanup
%% relay finds nobody to watch (`noproc') and learns no exit reason. The
%% caller must then verify the closed transport against the manager table —
%% the lease died with the guardian and the row is gone — and must not report
%% `transport_cleanup_timeout'. What it does report is `timeout' at the
%% deadline: once the manager closed the socket under the reader, hackney's
%% async stream sends it nothing (measured on 1.25.0), so the verdict is the
%% deadline's rather than a transport error. Whether the guardian should send
%% the reader that error itself is an open API question, tracked outside this
%% repository with the rest of the port's deferred work.
bounded_body_read_after_guardian_death_verifies_cleanup_by_table_test_() ->
    {'timeout', 30, fun bounded_body_read_after_guardian_death_verifies_cleanup_by_table/0}.

bounded_body_read_after_guardian_death_verifies_cleanup_by_table() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              %% status and headers, then the body never comes
              'ok' = send_json_headers(Socket, 200, 64),
              ServerParent ! {'body_held', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'guardian_gone_result', self(),
                                           couchbeam:db_info_bounded(
                                             Db, {1500, 1024})}
                         end),
              {WorkerPid, LeasePid, GuardianPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Caller} ->
                          {Worker, Lease, Guardian}
                  after 2000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              receive
                  {'body_held', _ServerPid} -> 'ok'
              after 2000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% the caller has adopted the request and waits for the body
              'ok' = await_ref_owner(Ref, LeasePid),
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case process_info(Caller, 'current_function') of
                               {'current_function',
                                {'couchbeam_httpc', 'bounded_body', 4}} ->
                                   {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 2000)),
              exit(GuardianPid, 'kill'),
              %% the lease dies with its guardian and the manager drops the row
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case {is_process_alive(LeasePid),
                                 ets:lookup('hackney_manager_refs', Ref)} of
                               {'false', []} -> {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 1000)),
              receive
                  {'guardian_gone_result', Caller, Result} ->
                      ?assertMatch({'error', _}, Result),
                      ?assertNotEqual({'error', 'transport_cleanup_timeout'},
                                      Result)
              after 3000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% --- round 5 regressions ---------------------------------------------------

%% hackney's own timers must never win the race against the budget's
%% `receive … after': at the deadline the verdict is `timeout', not hackney's
%% `{closed, timeout}' or `connect_timeout'. The caller's own transport
%% timeouts are replaced, not merged.
request_options_backstops_hackney_timeouts_above_the_budget_test() ->
    {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
    {'ok', Options} = couchbeam_httpc:request_options(
                        [{'recv_timeout', 1}, {'connect_timeout', 1},
                         {'checkout_timeout', 1}, {'pool', 'some_pool'}],
                        Budget),
    TimeoutMs = maps:get('timeout_ms', Budget),
    [Recv] = proplists:get_all_values('recv_timeout', Options),
    [Connect] = proplists:get_all_values('connect_timeout', Options),
    %% the pool's own timer: a caller's shorter value would win the race
    %% with `{error, checkout_timeout}' instead of the budget's `timeout'
    [Checkout] = proplists:get_all_values('checkout_timeout', Options),
    ?assert(Recv > TimeoutMs),
    ?assert(Connect > TimeoutMs),
    ?assert(Checkout > TimeoutMs),
    ?assertEqual('some_pool', proplists:get_value('pool', Options)).

%% Kazoo's connection builder always passes an explicit `recv_timeout' (20 s);
%% the budget must replace it, or every longer budget is silently truncated
%% on the transport and reported as a plain deadline.
bounded_budget_overrides_explicit_caller_transport_timeouts_test_() ->
    {'timeout', 60,
     fun bounded_budget_overrides_explicit_caller_transport_timeouts/0}.

bounded_budget_overrides_explicit_caller_transport_timeouts() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    Body = <<"{\"db_name\":\"db\"}">>,
    ViewBody = <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
    lists:foreach(
      fun({ResponseBody, CallFun}) ->
              with_http_server(
                fun(Socket, Parent) ->
                        %% longer than the caller's 1000 ms transport timeouts
                        timer:sleep(2500),
                        send_json_response(Socket, ResponseBody),
                        Parent ! {'server_done', self()}
                end,
                fun(BaseUrl) ->
                        Server = couchbeam:server_connection(
                                   BaseUrl,
                                   [{'no_proxy_env', 'true'},
                                    {'recv_timeout', 1000},
                                    {'connect_timeout', 1000}]),
                        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
                        ?assertMatch({'ok', _, _}, CallFun(Db))
                end)
      end,
      [{Body, fun(Db) -> couchbeam:db_info_bounded(Db, {15000, 1024}) end},
       {ViewBody, fun(Db) ->
                          couchbeam_view:fetch_bounded(
                            Db, 'all_docs', [], {15000, 4096})
                  end}]).

%% An unencodable document is an input error with a typed verdict before any
%% connection, on both write doors.
bounded_save_doc_refuses_unencodable_document_before_transport_test() ->
    Result = refused_before_transport(
               fun(Db) ->
                       couchbeam:save_doc_bounded(
                         Db, {[{<<"_id">>, <<"doc">>}, {<<"x">>, self()}]},
                         [], {1000, 1024})
               end, []),
    ?assertMatch({'error', {'invalid_json_encoding', {'error', _}}}, Result).

%% The POST body is scanned by the pre-flight like every other option: the
%% refusal names the entry, and neither a stream under the supervisor nor an
%% encoder worker ever existed — no budget, not only no connection.
bounded_view_post_refuses_unencodable_keys_before_transport_test() ->
    ensure_couchbeam_supervisor(),
    Keys = [self()],
    StreamsBefore = view_stream_children(),
    Result = refused_before_transport(
               fun(Db) ->
                       couchbeam_view:fetch_bounded(
                         Db, 'all_docs', [{'keys', Keys}], {1000, 4096})
               end, []),
    ?assertEqual({'error', {'invalid_param', {'keys', Keys}}}, Result),
    ?assertEqual(StreamsBefore, view_stream_children()),
    ?assertEqual([], json_encoder_processes()),
    assert_no_lifecycle_messages().

%% A killed encoder is reported the moment it dies, not at the deadline.
bounded_encoder_death_is_reported_at_once_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Parent = self(),
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try
        Server = couchbeam:server_connection(
                   BaseUrl,
                   [{'no_proxy_env', 'true'},
                    {'bounded_encode_test_delay_ms', 5000}]),
        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
        Caller = spawn(
                   fun() ->
                           Parent ! {'encoder_death_result', self(),
                                     couchbeam:save_doc_bounded(
                                       Db, {[{<<"_id">>, <<"doc">>}]}, [],
                                       {5000, 1024})}
                   end),
        Encoders = await_condition(
                     fun() ->
                             case json_encoder_processes() of
                                 [] -> 'false';
                                 Pids -> {'true', Pids}
                             end
                     end, 2000),
        ?assertMatch([_ | _], Encoders),
        Started = erlang:monotonic_time('millisecond'),
        lists:foreach(fun(Pid) -> exit(Pid, 'kill') end, Encoders),
        receive
            {'encoder_death_result', Caller, Result} ->
                ?assertEqual({'error', {'json_encoding_failed', 'killed'}},
                             Result)
        after 1000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end,
        ?assert(erlang:monotonic_time('millisecond') - Started < 500),
        ?assertEqual({'error', 'timeout'}, gen_tcp:accept(ListenSocket, 20))
    after
        gen_tcp:close(ListenSocket)
    end.

bounded_db_info_reports_invalid_json_body_and_closes_transport_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, <<"not-json">>),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch({'error', {'invalid_json', _}},
                           couchbeam:db_info_bounded(Db, {1000, 1024})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end,
              assert_no_stray_transport_messages()
      end).

%% The direct caller (parent and lifecycle owner in one) dies while it waits
%% for the body: the guardian's parent monitor closes the transport at once.
bounded_direct_caller_death_during_body_read_closes_transport_test_() ->
    {'timeout', 30,
     fun bounded_direct_caller_death_during_body_read_closes_transport/0}.

bounded_direct_caller_death_during_body_read_closes_transport() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = send_json_headers(Socket, 200, 64),
              ServerParent ! {'body_held', self()},
              receive 'observe_close' -> 'ok' end,
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 _ = couchbeam:db_info_bounded(Db, {5000, 1024}),
                                 receive 'never' -> 'ok' end
                         end),
              {WorkerPid, LeasePid, GuardianPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Caller} ->
                          {Worker, Lease, Guardian}
                  after 2000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              ServerPid = receive
                              {'body_held', Pid} -> Pid
                          after 2000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              'ok' = await_ref_owner(Ref, LeasePid),
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case process_info(Caller, 'current_function') of
                               {'current_function',
                                {'couchbeam_httpc', 'bounded_body', 4}} ->
                                   {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 2000)),
              Started = erlang:monotonic_time('millisecond'),
              exit(Caller, 'kill'),
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case {is_process_alive(LeasePid),
                                 is_process_alive(GuardianPid),
                                 ets:lookup('hackney_manager_refs', Ref)} of
                               {'false', 'false', []} -> {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 2000)),
              %% torn down by the caller's death, not by the 5000 ms deadline
              ?assert(erlang:monotonic_time('millisecond') - Started < 1000),
              ServerPid ! 'observe_close',
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% Legacy `open_doc/3' asks for `multipart/related' when it fetches
%% attachments; the bounded read promises inline JSON and must not.
bounded_open_doc_attachments_param_requests_json_not_multipart_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"_id\":\"doc\",\"_attachments\":{\"a.txt\":{\"content_type\":\"text/plain\",\"data\":\"eA==\"}}}">>,
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(<<"GET /db/doc?attachments=true HTTP/1.1">>,
                                  Request),
              ?assertEqual('nomatch',
                           binary:match(string:lowercase(Request),
                                        <<"multipart/related">>)),
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              {'ok', Doc, _Bytes} = couchbeam:open_doc_bounded(
                                      Db, <<"doc">>, [{"attachments", 'true'}],
                                      {1000, 4096}),
              ?assertEqual(<<"doc">>, couchbeam_doc:get_id(Doc))
      end).

%% The document-side decode watchdog: a decoder still running at the deadline
%% is killed, and the caller gets its verdict at the deadline — not when the
%% decoder would have finished.
bounded_json_body_decode_watchdog_kills_slow_decoder_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              put('bounded_decode_test_delay_ms', 1000),
              Started = erlang:monotonic_time('millisecond'),
              try
                  ?assertEqual({'error', 'timeout'},
                               couchbeam:db_info_bounded(Db, {300, 1024}))
              after
                  erase('bounded_decode_test_delay_ms')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 700),
              %% the watchdog killed the decoder before returning
              ?assertEqual([], json_decoder_processes())
      end).

%% A 2xx whose `rev' or `id' is not a string (JSON null included) must not
%% be written into the document.
bounded_save_doc_refuses_null_rev_in_response_test() ->
    assert_bounded_save_response_refused(
      <<"{\"ok\":true,\"id\":\"doc\",\"rev\":null}">>,
      {'error', {'invalid_response', 'missing_rev'}}).

bounded_save_doc_refuses_null_id_in_response_test() ->
    assert_bounded_save_response_refused(
      <<"{\"ok\":true,\"id\":null,\"rev\":\"1-a\"}">>,
      {'error', {'invalid_response', 'missing_id'}}).

assert_bounded_save_response_refused(Response, Expected) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_server(
      fun(Socket, Parent) ->
              send_json_response(Socket, Response),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(Expected,
                           couchbeam:save_doc_bounded(
                             Db, {[{<<"_id">>, <<"doc">>}]}, [],
                             {1000, 1024}))
      end).

%% The id goes into the URL and into the JSON body; an Erlang string would
%% encode as an array of integers in the body.
bounded_save_doc_refuses_string_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, "doc"}, {<<"value">>, 1}]}, [],
                {1000, 1024})
      end).

bounded_open_doc_refuses_non_latin1_string_doc_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) -> couchbeam:open_doc_bounded(Db, [1000], [], {1000, 1024}) end).

bounded_open_doc_accepts_latin1_string_doc_id_test() ->
    assert_bounded_request_line(
      <<"GET /db/doc HTTP/1.1">>,
      <<"{\"_id\":\"doc\"}">>,
      fun(Db) -> couchbeam:open_doc_bounded(Db, "doc", [], {1000, 1024}) end).

%% `basic_auth' is a `hackney' option carried in `#db.options'; the bounded
%% request builder must let it through to the wire on both entries into
%% `request_bounded/7' — the direct doors and the view stream.
bounded_db_info_sends_basic_auth_header_test() ->
    assert_bounded_basic_auth(
      <<"{\"db_name\":\"db\"}">>,
      fun(Db) -> couchbeam:db_info_bounded(Db, {1000, 1024}) end).

bounded_fetch_sends_basic_auth_header_test() ->
    ensure_couchbeam_supervisor(),
    assert_bounded_basic_auth(
      <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, 'all_docs', [], {1000, 4096})
      end).

assert_bounded_basic_auth(Body, CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              %% base64("u:p")
              assert_request_header(
                <<"Authorization">>, <<"Basic dTpw">>, Request),
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'},
                                   {'basic_auth', {<<"u">>, <<"p">>}}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch({'ok', _, _}, CallFun(Db)),
              assert_success_left_nothing_behind()
      end).

%% Params and options must be `{Key, Value}' lists — the only shape URL
%% building accepts — and a malformed entry must not slip past the refusal
%% scans either.
bounded_open_doc_refuses_non_list_params_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', 'notalist'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(Db, <<"doc">>, 'notalist', {1000, 1024})
      end).

bounded_open_doc_refuses_bare_key_param_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', 'accept'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(Db, <<"doc">>, ['accept'], {1000, 1024})
      end).

%% The two write-option scans run in sequence: an improper list with
%% well-formed heads is refused by the shape scan before the `batch' scan
%% (`lists:any/2') could walk into its tail.
bounded_save_doc_refuses_improper_option_list_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', 'tail'}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{'x', 1} | 'tail'],
                {1000, 1024})
      end).

bounded_save_doc_refuses_three_tuple_option_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', {'batch', 'ok', 'x'}}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{'batch', 'ok', 'x'}],
                {1000, 1024})
      end).

%% the remaining key forms of the refusal matrix
bounded_open_doc_refuses_string_accept_param_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'accept'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{"accept", "application/json"}], {1000, 1024})
      end).

bounded_open_doc_refuses_string_open_revs_param_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'open_revs'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{"open_revs", "all"}], {1000, 1024})
      end).

bounded_save_doc_refuses_atom_batch_option_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'batch'}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{'batch', 'ok'}],
                {1000, 1024})
      end).

bounded_save_doc_refuses_binary_batch_option_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_param', 'batch'}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{<<"batch">>, <<"ok">>}],
                {1000, 1024})
      end).

%% The collector's delivery allowance must outlast the stream's own
%% acknowledgement deadline (cleanup deadline + T), or the stream's late
%% `transport_cleanup_timeout' report could land after the terminal flush.
bounded_cleanup_delivery_budget_sits_above_the_ack_deadline_test() ->
    Budget = #{'deadline_ms' => 1000, 'timeout_ms' => 100,
               'max_response_bytes' => 1},
    AckDeadline = 1000 + 100,
    #{'deadline_ms' := Delivery} =
        couchbeam_view:bounded_cleanup_delivery_budget(Budget),
    ?assert(Delivery > AckDeadline),
    ?assertEqual(1000 + 2 * 100, Delivery).

%% The collector's wait on the cancel path: a known guardian that announced
%% nothing gets the same delivery allowance as a `pending' cleanup — the
%% stream's own table proof starts strictly after the collector's fallback
%% and reports one cleanup budget later. `undefined' and `complete' keep the
%% bare fallback.
bounded_cleanup_wait_budget_grants_delivery_allowance_to_known_guardian_test() ->
    {'ok', Budget} = couchbeam_httpc:new_request_budget({100, 1024}),
    #{'deadline_ms' := Fallback} = Budget,
    #{'deadline_ms' := Known} =
        couchbeam_view:bounded_cleanup_wait_budget(self(), Budget),
    #{'deadline_ms' := Pending} =
        couchbeam_view:bounded_cleanup_wait_budget({'pending', Budget}, Budget),
    ?assertEqual(Fallback + 2 * 100, Known),
    ?assertEqual(Pending, Known),
    ?assertEqual(Budget,
                 couchbeam_view:bounded_cleanup_wait_budget('undefined', Budget)),
    ?assertEqual(Budget,
                 couchbeam_view:bounded_cleanup_wait_budget('complete', Budget)).

%% --- round 6 regressions ---------------------------------------------------

%% The owner dies mid-stream while the manager is suspended: the guardian
%% cannot prove its cleanup and the stream's own request times out — the one
%% path that emits `couchbeam_bounded_owner_down_cleanup_unproven'.
bounded_view_collector_death_with_unprovable_cleanup_logs_warning_test_() ->
    {'timeout', 30,
     fun bounded_view_collector_death_with_unprovable_cleanup_logs_warning/0}.

bounded_view_collector_death_with_unprovable_cleanup_logs_warning() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    FirstChunk = <<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"a\",\"key\":\"a\",\"value\":{}},">>,
    with_captured_warnings(
      fun() ->
              with_http_server(
                fun(Socket, ServerParent) ->
                        'ok' = send_chunked_headers(Socket),
                        'ok' = send_http_chunk(Socket, FirstChunk),
                        ServerParent ! {'midstream_chunk_sent', self()},
                        ServerParent ! {'peer_close_result',
                                        recv_until_closed(Socket, 4000)},
                        ServerParent ! {'server_done', self()}
                end,
                fun(BaseUrl) ->
                        collector_death_unprovable_scenario(BaseUrl, Parent)
                end)
      end,
      fun(Events) ->
              %% the stream's warning names the request too
              ?assertMatch([{'report', #{'method' := 'get',
                                         'path' := <<"/db/_all_docs">>}}],
                           owner_down_unproven_warnings(Events))
      end).

collector_death_unprovable_scenario(BaseUrl, Parent) ->
    Server = couchbeam:server_connection(
               BaseUrl,
               [{'no_proxy_env', 'true'},
                {'bounded_upload_context_test_hook', Parent}]),
    {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
    Caller = spawn(
               fun() ->
                       _ = couchbeam_view:fetch_bounded(
                             Db, 'all_docs', [], {300, 4096}),
                       receive 'never' -> 'ok' end
               end),
    {WorkerPid, LeasePid, GuardianPid, StreamPid} =
        receive
            {'bounded_upload_context', Worker, Lease, Guardian, Stream} ->
                {Worker, Lease, Guardian, Stream}
        after 2000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end,
    Ref = owned_request_ref([WorkerPid, LeasePid]),
    receive
        {'midstream_chunk_sent', _ServerPid} -> 'ok'
    after 2000 ->
            exit(Caller, 'kill'),
            ?assert('false')
    end,
    'ok' = await_ref_owner(Ref, LeasePid),
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case process_info(StreamPid, 'current_function') of
                     {'current_function',
                      {'couchbeam_view_stream', 'bounded_loop_receive', 4}} ->
                         {'true', 'true'};
                     _ -> 'false'
                 end
         end, 2000)),
    StreamMonitor = erlang:monitor('process', StreamPid),
    ManagerWatcher = suspend_manager(),
    try
        exit(Caller, 'kill'),
        %% the stream gives up on its acknowledgement one cleanup budget past
        %% the guardian's deadline (300 + 300 ms) and logs the warning
        receive
            {'DOWN', StreamMonitor, 'process', StreamPid, _Reason} -> 'ok'
        after 3000 ->
                ?assert('false')
        end
    after
        'ok' = resume_manager(ManagerWatcher)
    end,
    %% the guardian keeps retrying until the resumed manager drops the row
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case {is_process_alive(GuardianPid),
                       ets:lookup('hackney_manager_refs', Ref)} of
                     {'false', []} -> {'true', 'true'};
                     _ -> 'false'
                 end
         end, 3000)),
    receive
        {'peer_close_result', PeerResult} ->
            ?assertEqual({'error', 'closed'}, PeerResult)
    after 4500 ->
            ?assert('false')
    end.

%% A decoder killed from outside is reported the moment it dies, as a
%% transient failure — not at the deadline and not as a malformed body.
bounded_json_body_decoder_death_is_reported_at_once_test_() ->
    {'timeout', 30, fun bounded_json_body_decoder_death_is_reported_at_once/0}.

bounded_json_body_decoder_death_is_reported_at_once() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
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
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 put('bounded_decode_test_delay_ms', 5000),
                                 Parent ! {'decoder_death_result', self(),
                                           couchbeam:db_info_bounded(
                                             Db, {10000, 1024})}
                         end),
              %% held inside its test delay, before `decode_json_result/1';
              %% the caller itself waits inside `decode_with_watchdog/3' too
              Decoders = await_condition(
                           fun() ->
                                   case [P || P <- processes_in(
                                                     'couchbeam_httpc',
                                                     "decode_with_watchdog"),
                                              P =/= Caller] of
                                       [] -> 'false';
                                       Pids -> {'true', Pids}
                                   end
                           end, 2000),
              ?assertMatch([_ | _], Decoders),
              Started = erlang:monotonic_time('millisecond'),
              lists:foreach(fun(Pid) -> exit(Pid, 'kill') end, Decoders),
              receive
                  {'decoder_death_result', Caller, Result} ->
                      ?assertEqual(
                         {'error', {'json_decoding_failed', 'killed'}}, Result)
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

bounded_view_decoder_death_is_reported_at_once_test_() ->
    {'timeout', 30, fun bounded_view_decoder_death_is_reported_at_once/0}.

bounded_view_decoder_death_is_reported_at_once() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Json = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, Json),
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'view_decoder_death_result', self(),
                                           couchbeam_view:fetch_bounded(
                                             Db, 'all_docs',
                                             [{'bounded_decode_test_delay_ms',
                                               5000}],
                                             {10000, 4096})}
                         end),
              Decoders = await_condition(
                           fun() ->
                                   case view_decoder_processes() of
                                       [] -> 'false';
                                       Pids -> {'true', Pids}
                                   end
                           end, 5000),
              ?assertMatch([_ | _], Decoders),
              Started = erlang:monotonic_time('millisecond'),
              lists:foreach(fun(Pid) -> exit(Pid, 'kill') end, Decoders),
              receive
                  {'view_decoder_death_result', Caller, Result} ->
                      ?assertEqual(
                         {'error', {'json_decoding_failed', 'killed'}}, Result)
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

bounded_open_doc_refuses_atom_doc_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) -> couchbeam:open_doc_bounded(Db, 'null', [], {1000, 1024}) end).

bounded_open_doc_refuses_integer_doc_id_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) -> couchbeam:open_doc_bounded(Db, 42, [], {1000, 1024}) end).

%% Once the stream has reported `transport_cleanup_timeout' no lifecycle
%% verdict can follow (retries never notify): the collector must not burn
%% its delivery allowance before echoing the same verdict.
bounded_view_collector_returns_at_once_after_stream_cleanup_timeout_test_() ->
    {'timeout', 30,
     fun bounded_view_collector_returns_at_once_after_stream_cleanup_timeout/0}.

bounded_view_collector_returns_at_once_after_stream_cleanup_timeout() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'cleanup_timeout_request_received', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 4000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {500, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'cleanup_timeout_result', self(),
                                           Result, Messages}
                         end),
              {GuardianPid, WorkerPid} =
                  receive
                      {'bounded_guardian_started_ready', Guardian, Worker,
                       _Lease, _StreamPid} ->
                          {Guardian, Worker}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              receive
                  {'cleanup_timeout_request_received', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              Ref = owned_request_ref(WorkerPid),
              ManagerWatcher = suspend_manager(),
              try
                  receive
                      {'bounded_guardian_cleanup', 'failed', GuardianPid,
                       'undefined'} -> 'ok'
                  after 1500 ->
                          ?assert('false')
                  end,
                  receive
                      {'cleanup_timeout_result', Caller, Result, Messages} ->
                          ?assertEqual(
                             {'error', 'transport_cleanup_timeout'}, Result),
                          ?assertEqual([], lifecycle_messages(Messages))
                  after 2500 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
                  Elapsed = erlang:monotonic_time('millisecond') - Started,
                  %% deadline 500 + guardian cleanup 500 = the stream's
                  %% verdict at ~1000 ms; the collector must not add the
                  %% 2 * 500 ms delivery allowance on top
                  ?assert(Elapsed < 1400)
              after
                  'ok' = resume_manager(ManagerWatcher)
              end,
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case {is_process_alive(GuardianPid),
                                 ets:lookup('hackney_manager_refs', Ref)} of
                               {'false', []} -> {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 3000)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 4500 ->
                      ?assert('false')
              end
      end).

%% The encoder door validates its budget before spawning anything, like the
%% other exported doors; a malformed map must not leave a worker's reply in
%% the caller's mailbox.
bounded_encode_json_refuses_malformed_budget_before_spawning_test() ->
    drain_transport_messages(),
    Malformed = #{'deadline_ms' => erlang:monotonic_time('millisecond') + 1000,
                  'max_response_bytes' => 1024},
    ?assertEqual({'error', 'invalid_request_budget'},
                 couchbeam_httpc:bounded_encode_json(
                   {[{<<"_id">>, <<"doc">>}]}, Malformed)),
    ?assertEqual([], json_encoder_processes()),
    assert_no_stray_transport_messages().

%% Values `hackney_url:qs/1' cannot render are refused before the budget.
bounded_open_doc_refuses_float_param_value_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', {'rev', 1.5}}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'rev', 1.5}], {1000, 1024})
      end).

bounded_save_doc_refuses_tuple_option_value_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', {'new_edits', {'a', 'b'}}}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{'new_edits', {'a', 'b'}}],
                {1000, 1024})
      end).

%% A document is `{Proplist}'; a map or a list of documents is refused.
bounded_save_doc_refuses_map_document_test() ->
    assert_refused_before_transport(
      {'error', 'invalid_document'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, #{<<"_id">> => <<"doc">>}, [], {1000, 1024})
      end).

bounded_save_doc_refuses_document_list_test() ->
    assert_refused_before_transport(
      {'error', 'invalid_document'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, [{[{<<"_id">>, <<"doc">>}]}], [], {1000, 1024})
      end).

%% CouchDB answers with the id it stored; any other id is a misrouted reply
%% and must not rebind the caller's document.
bounded_save_doc_refuses_foreign_id_in_response_test() ->
    assert_bounded_save_response_refused(
      <<"{\"ok\":true,\"id\":\"other\",\"rev\":\"1-a\"}">>,
      {'error', {'invalid_response', {'id_mismatch', <<"doc">>, <<"other">>}}}).

%% The guardian is killed while it is held just before publishing
%% `guardian_started': its worker and lease exist, so the caller cannot prove
%% the cleanup and must not report a bare `request_guardian_down'.
bounded_guardian_death_before_started_reports_unproven_cleanup_test_() ->
    {'timeout', 30, fun bounded_guardian_death_before_started_reports_unproven_cleanup/0}.

bounded_guardian_death_before_started_reports_unproven_cleanup() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'before_started_request_received', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'before_started_result', self(),
                                           couchbeam:db_info_bounded(
                                             Db, {5000, 1024})}
                         end),
              {GuardianPid, WorkerPid, LeasePid} =
                  receive
                      {'bounded_guardian_started_ready', Guardian, Worker,
                       Lease, Caller} ->
                          {Guardian, Worker, Lease}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              receive
                  {'before_started_request_received', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              Started = erlang:monotonic_time('millisecond'),
              exit(GuardianPid, 'kill'),
              receive
                  {'before_started_result', Caller, Result} ->
                      ?assertEqual({'error', 'transport_cleanup_timeout'},
                                   Result)
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              %% the guards take the worker and the lease down with the
              %% guardian, and the manager drops the socket with the worker
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case {is_process_alive(WorkerPid),
                                 is_process_alive(LeasePid)} of
                               {'false', 'false'} -> {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 1000)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% Unparsable view options are refused before any budget or connection; the
%% legacy stream would crash on the parser's error tuple instead.
bounded_fetch_refuses_unparsable_view_options_test() ->
    ensure_couchbeam_supervisor(),
    %% the official parser answers with its message string; the door names
    %% the entry, so every refusal has the one shape
    assert_refused_before_transport(
      {'error', {'invalid_param', {'stale', 'bogus'}}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'stale', 'bogus'}], {1000, 1024})
      end).

%% Lock on root B: a `budget_timeout' that is already queued when the decoder
%% returns an incomplete continuation must be honoured by `bounded_loop_receive/4'.
%% Returning to the official `maybe_continue/1' instead would report it as an
%% unexpected message — `{'error', {Ref, 'budget_timeout'}}' — and crash the
%% stream without cleanup.
bounded_view_budget_timeout_queued_during_decode_is_honoured_test_() ->
    {'timeout', 30,
     fun bounded_view_budget_timeout_queued_during_decode_is_honoured/0}.

bounded_view_budget_timeout_queued_during_decode_is_honoured() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    %% one complete row, then the JSON stays open
    FirstChunk = <<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"a\",\"key\":\"a\",\"value\":{}},">>,
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, FirstChunk),
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 5000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'queued_timeout_result', self(),
                                           couchbeam_view:fetch_bounded(
                                             Db, 'all_docs',
                                             [{'bounded_decode_test_delay_ms',
                                               1000}],
                                             {10000, 4096})}
                         end),
              StreamPid = receive
                              {'bounded_upload_context', _W, _L, _G, Stream} ->
                                  Stream
                          after 2000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              %% the decoder is asleep inside its test delay
              ?assertMatch([_ | _],
                           await_condition(
                             fun() ->
                                     case view_decoder_processes() of
                                         [] -> 'false';
                                         Pids -> {'true', Pids}
                                     end
                             end, 5000)),
              [{StreamRef, StreamPid}] = ets:match_object(
                                           'couchbeam_view_streams',
                                           {'_', StreamPid}),
              Started = erlang:monotonic_time('millisecond'),
              %% exactly what the collector sends at its deadline, queued
              %% while the stream still waits for the decoder
              StreamPid ! {StreamRef, 'budget_timeout'},
              receive
                  {'queued_timeout_result', Caller, Result} ->
                      ?assertEqual({'error', 'timeout'}, Result)
              after 5000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              Elapsed = erlang:monotonic_time('millisecond') - Started,
              %% honoured right after the decode delay, not at the deadline
              ?assert(Elapsed < 3000),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 5500 ->
                      ?assert('false')
              end
      end).

bounded_open_doc_reports_bad_response_with_error_body_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Body = <<"{\"error\":\"x\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 500, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch(
                 {'error', {'bad_response', {500, [_ | _], Body}}},
                 couchbeam:open_doc_bounded(Db, <<"doc">>, [], {1000, 1024})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

assert_bounded_status_mapping(Status, Expected, CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, Status, 0),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(Expected, CallFun(Db)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end,
              assert_no_stray_transport_messages()
      end).

%% --- round 7 regressions ---------------------------------------------------

%% The guardian is killed while it is held just before publishing
%% `guardian_started' on the view door: the stream reports
%% `transport_cleanup_timeout' at once (`couchbeam_httpc:await_guardian_resources/4')
%% and the collector, which already knows the guardian, must not wait a
%% cleanup budget for a lifecycle verdict the dead guardian cannot send.
bounded_view_guardian_death_before_started_is_final_at_once_test_() ->
    {'timeout', 30,
     fun bounded_view_guardian_death_before_started_is_final_at_once/0}.

bounded_view_guardian_death_before_started_is_final_at_once() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'view_before_started_request_received', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_started_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {1000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'view_before_started_result',
                                           self(), Result, Messages}
                         end),
              {GuardianPid, WorkerPid, LeasePid, StreamPid} =
                  receive
                      {'bounded_guardian_started_ready', Guardian, Worker,
                       Lease, Stream} ->
                          {Guardian, Worker, Lease, Stream}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              ?assert(StreamPid =/= Caller),
              receive
                  {'view_before_started_request_received', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              Started = erlang:monotonic_time('millisecond'),
              exit(GuardianPid, 'kill'),
              receive
                  {'view_before_started_result', Caller, Result, Messages} ->
                      ?assertEqual({'error', 'transport_cleanup_timeout'},
                                   Result),
                      ?assertEqual([], lifecycle_messages(Messages)),
                      ?assertEqual([], [M || M <- Messages,
                                             is_stray_transport_message(M)])
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% the verdict must not wait for the 1000 ms cleanup budget
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              assert_process_gone(WorkerPid),
              assert_process_gone(LeasePid),
              assert_process_gone(StreamPid),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% Keys are rendered by `hackney_url:qs/1' exactly like values; a key it
%% cannot render is refused before the budget, on both document doors.
bounded_open_doc_refuses_tuple_param_key_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', {{'a', 'b'}, <<"x">>}}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{{'a', 'b'}, <<"x">>}], {1000, 1024})
      end).

bounded_save_doc_refuses_float_option_key_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', {1.5, <<"x">>}}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{1.5, <<"x">>}],
                {1000, 1024})
      end).

%% The view door refuses, before any budget or connection, what would
%% otherwise crash in the calling process: a non-list, a parsed pair
%% `hackney_url:qs/1' cannot render, and a key value jsx cannot encode.
bounded_fetch_refuses_non_list_options_test() ->
    ensure_couchbeam_supervisor(),
    assert_refused_before_transport(
      {'error', {'invalid_param', 'notalist'}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', 'notalist', {1000, 1024})
      end).

bounded_fetch_refuses_unrenderable_option_value_test() ->
    ensure_couchbeam_supervisor(),
    assert_refused_before_transport(
      {'error', {'invalid_param', {'limit', 1.5}}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'limit', 1.5}], {1000, 1024})
      end).

bounded_fetch_refuses_unrenderable_string_option_key_test() ->
    ensure_couchbeam_supervisor(),
    assert_refused_before_transport(
      {'error', {'invalid_param', {[1000], <<"x">>}}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{[1000], <<"x">>}], {1000, 1024})
      end).

%% CVE-2026-47075 (hackney < 4.0.1): `hackney_url:make_url/3' passes a raw
%% query string through unencoded, so a CR/LF in it splits the request. The
%% doors hand hackney `{Key, Value}' pairs, which `hackney_url:qs/1'
%% percent-encodes (measured on 1.20.1 and 1.25.0: the wire carries
%% `%0D%0A'), so the bytes do not reach the request line today. The refusal
%% makes that a contract of the door instead of a property of the
%% dependency's encoder, on every shape the scan renders; a tab, a space or
%% a percent sign is data and stays accepted.
invalid_query_param_refuses_line_terminators_test_() ->
    Injected = <<"1-x\r\nX-Injected: yes">>,
    [?_assertEqual('undefined',
                   couchbeam_httpc:invalid_query_param(
                     [{'rev', <<"1-x y%z\t">>}, {'k', "a b"}, {'n', 1}])),
     ?_assertEqual({'invalid_param', {'rev', Injected}},
                   couchbeam_httpc:invalid_query_param([{'rev', Injected}])),
     ?_assertEqual({'invalid_param', {'rev', <<"1\r">>}},
                   couchbeam_httpc:invalid_query_param([{'rev', <<"1\r">>}])),
     ?_assertEqual({'invalid_param', {'rev', <<"1\n">>}},
                   couchbeam_httpc:invalid_query_param([{'rev', <<"1\n">>}])),
     ?_assertEqual({'invalid_param', {<<"re\nv">>, <<"1">>}},
                   couchbeam_httpc:invalid_query_param(
                     [{<<"re\nv">>, <<"1">>}])),
     ?_assertEqual({'invalid_param', {'rev', "1\r\n"}},
                   couchbeam_httpc:invalid_query_param([{'rev', "1\r\n"}])),
     ?_assertEqual({'invalid_param', {'re\nv', <<"1">>}},
                   couchbeam_httpc:invalid_query_param([{'re\nv', <<"1">>}])),
     ?_assertEqual({'invalid_param', {'rev', 'o\rk'}},
                   couchbeam_httpc:invalid_query_param([{'rev', 'o\rk'}]))].

bounded_open_doc_refuses_crlf_in_param_value_test() ->
    Injected = <<"1-x\r\nX-Injected: yes">>,
    assert_refused_before_transport(
      {'error', {'invalid_param', {'rev', Injected}}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'rev', Injected}], {1000, 1024})
      end).

bounded_save_doc_refuses_lf_in_option_key_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', {<<"batch\n">>, <<"ok">>}}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [{<<"batch\n">>, <<"ok">>}],
                {1000, 1024})
      end).

bounded_fetch_refuses_cr_in_view_option_value_test() ->
    ensure_couchbeam_supervisor(),
    assert_refused_before_transport(
      {'error', {'invalid_param', {'startkey_docid', <<"a\rb">>}}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'startkey_docid', <<"a\rb">>}],
                {1000, 1024})
      end).

%% The scan names the first offending entry, refuses an improper half
%% without crashing on it (`andalso' stops before `lists:member/2'), and
%% covers a Latin-1 string key like every other shape.
invalid_query_param_names_the_first_line_terminated_entry_test_() ->
    [?_assertEqual({'invalid_param', {'rev', <<"a\rb">>}},
                   couchbeam_httpc:invalid_query_param(
                     [{'ok', <<"1">>}, {'rev', <<"a\rb">>}, {'k', "x\n"}])),
     ?_assertEqual({'invalid_param', {"re\nv", <<"1">>}},
                   couchbeam_httpc:invalid_query_param([{"re\nv", <<"1">>}])),
     ?_assertEqual({'invalid_param', {'rev', [$a | $b]}},
                   couchbeam_httpc:invalid_query_param([{'rev', [$a | $b]}]))].

%% What the accepted shapes look like on the wire: a space, a percent sign
%% and a tab are data and travel percent-encoded (`hackney_url:qs/1').
bounded_open_doc_sends_accepted_param_shapes_encoded_test() ->
    assert_bounded_request_line(
      <<"GET /db/doc?rev=1-x+y%25z%09 HTTP/1.1">>,
      <<"{\"_id\":\"doc\",\"_rev\":\"1-x\"}">>,
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'rev', <<"1-x y%z\t">>}], {1000, 1024})
      end).

%% The view door scans the PARSED pairs: a control character inside a `key'
%% is JSON-escaped by the parser (`couchbeam_ejson:encode/1' → `\n', two
%% characters), so it is not a line terminator any more and the request is
%% sent, with the escaped form percent-encoded.
bounded_fetch_sends_json_escaped_control_char_in_key_test() ->
    ensure_couchbeam_supervisor(),
    assert_bounded_request_line(
      <<"GET /db/_all_docs?startkey=%22a%5Cnb%22 HTTP/1.1">>,
      <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'startkey', <<"a\nb">>}], {1000, 4096})
      end).

%% The legacy door has no scan: on it the line terminator reaches the wire
%% percent-encoded, by `hackney_url:qs/1' alone. This is the encoder property
%% the bounded doors refuse to rest on, observed rather than asserted.
legacy_open_doc_leaves_line_terminators_to_the_encoder_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(
                <<"GET /db/doc?rev=a%0D%0Ab HTTP/1.1">>, Request),
              send_json_response(Socket, <<"{\"_id\":\"doc\"}">>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch({'ok', _},
                           couchbeam:open_doc(
                             Db, <<"doc">>, [{'rev', <<"a\r\nb">>}]))
      end).

%% The path halves. On the pinned hackney (1.25.0; the parser is spelled
%% `parse_path'/`parse_fragment' in 1.20.1 and behaves the same, `qs/1',
%% `urlencode/2' and `pathencode/1' are byte-identical) a database, design
%% or view name carrying `?' and then CR/LF lands on the request line raw —
%% `GET /db?x HTTP/1.1\r\nX-Injected: yes\r\nX:/doc HTTP/1.1' — because
%% hackney cuts the URL at the first `?' and encodes only the path half;
%% `legacy_db_info_leaves_the_path_split_to_the_encoder_test' observes it. A
%% document id is urlencoded by `couchbeam_util:encode_docid/1'; the door
%% refuses it under the same contract rather than rest on that.
bounded_db_info_refuses_db_name_with_line_terminator_test() ->
    Name = <<"db?x HTTP/1.1\r\nX-Injected: yes\r\nX:">>,
    assert_refused_before_transport(
      {'error', {'unsafe_db_name', Name}},
      fun(Db) ->
              couchbeam:db_info_bounded(Db#db{name=Name}, {1000, 1024})
      end).

bounded_open_doc_refuses_db_name_with_line_terminator_test() ->
    Name = <<"db?x\nX: y">>,
    assert_refused_before_transport(
      {'error', {'unsafe_db_name', Name}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db#db{name=Name}, <<"doc">>, [], {1000, 1024})
      end).

bounded_save_doc_refuses_db_name_with_line_terminator_test() ->
    Name = <<"db\r">>,
    assert_refused_before_transport(
      {'error', {'unsafe_db_name', Name}},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db#db{name=Name}, {[{<<"_id">>, <<"doc">>}]}, [],
                {1000, 1024})
      end).

bounded_fetch_refuses_db_name_with_line_terminator_test() ->
    ensure_couchbeam_supervisor(),
    Name = <<"db?x\r\nX: y">>,
    assert_refused_before_transport(
      {'error', {'unsafe_db_name', Name}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db#db{name=Name}, 'all_docs', [], {1000, 4096})
      end).

bounded_open_doc_refuses_doc_id_with_line_terminator_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc\r\nX: y">>, [], {1000, 1024})
      end).

bounded_open_doc_refuses_string_doc_id_with_line_terminator_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:open_doc_bounded(Db, "doc\n", [], {1000, 1024})
      end).

bounded_save_doc_refuses_id_with_line_terminator_test() ->
    assert_refused_before_transport(
      {'error', 'missing_doc_id'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc\nX: y">>}]}, [], {1000, 1024})
      end).

bounded_fetch_refuses_design_name_with_line_terminator_test() ->
    ensure_couchbeam_supervisor(),
    ViewName = {<<"d?x HTTP/1.1\r\nX-Injected: yes\r\nX:">>, <<"v">>},
    assert_refused_before_transport(
      {'error', {'invalid_view_name', ViewName}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, ViewName, [], {1000, 4096})
      end).

bounded_fetch_refuses_view_name_with_line_terminator_test() ->
    ensure_couchbeam_supervisor(),
    ViewName = {<<"d">>, "v\n"},
    assert_refused_before_transport(
      {'error', {'invalid_view_name', ViewName}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, ViewName, [], {1000, 4096})
      end).

%% A space, a tab or a dot segment in a name is refused at the door, before
%% any budget — not at the transport door after one (on the view door, after
%% the stream started).
bounded_doors_refuse_names_with_a_space_a_tab_or_a_dot_segment_test_() ->
    [{"db_info: " ++ Label,
      ?_assertEqual({'error', {'unsafe_db_name', Name}},
                    refused_before_transport(
                      fun(Db) ->
                              couchbeam:db_info_bounded(Db#db{name=Name},
                                                        {1000, 1024})
                      end, []))}
     || {Label, Name} <- [{"space", <<"db x">>}, {"tab", "db\tx"},
                          {"dot", <<".">>}, {"dot dot", ".."}]]
    ++
    [{"open_doc: " ++ Label,
      ?_assertEqual({'error', {'unsafe_db_name', Name}},
                    refused_before_transport(
                      fun(Db) ->
                              couchbeam:open_doc_bounded(
                                Db#db{name=Name}, <<"doc">>, [], {1000, 1024})
                      end, []))}
     || {Label, Name} <- [{"space", <<"db x">>}, {"dot dot", <<"..">>}]]
    ++
    [{"save_doc: " ++ Label,
      ?_assertEqual({'error', {'unsafe_db_name', Name}},
                    refused_before_transport(
                      fun(Db) ->
                              couchbeam:save_doc_bounded(
                                Db#db{name=Name}, {[{<<"_id">>, <<"doc">>}]},
                                [], {1000, 1024})
                      end, []))}
     || {Label, Name} <- [{"tab", <<"db\tx">>}, {"dot", "."}]]
    ++
    [{"fetch, view half: " ++ Label,
      fun() ->
              ensure_couchbeam_supervisor(),
              ?assertEqual({'error', {'invalid_view_name', ViewName}},
                           refused_before_transport(
                             fun(Db) ->
                                     couchbeam_view:fetch_bounded(
                                       Db, ViewName, [], {1000, 4096})
                             end, []))
      end}
     || {Label, ViewName} <- [{"space in the design name",
                              {<<"d x">>, <<"v">>}},
                              {"tab in the view name", {<<"d">>, "v\tx"}},
                              {"dot design name", {<<".">>, <<"v">>}},
                              {"dot dot view name", {<<"d">>, <<"..">>}}]].

%% `{list, Name}' goes into the path of a `{Design, View}' request; a name
%% `hackney_url:fix_path/1' has no clause for (integer, atom) would crash
%% after the budget clock started, a `?'/`#' would re-address it, a
%% line-terminated one would split it (that one the query scan refuses
%% first: the parser keeps the pair as a query half too).
bounded_fetch_refuses_unusable_list_name_test_() ->
    [{Label,
      fun() ->
              ensure_couchbeam_supervisor(),
              assert_refused_before_transport(
                {'error', {'invalid_param', {'list', Name}}},
                fun(Db) ->
                        couchbeam_view:fetch_bounded(
                          Db, {<<"d">>, <<"v">>}, [{'list', Name}],
                          {1000, 4096})
                end)
      end}
     || {Label, Name} <- [{"integer", 42},
                          {"atom", 'l'},
                          {"question mark", <<"l?x">>},
                          {"slash", <<"l/x">>},
                          {"line terminator", <<"l\r">>}]].

%% What `make_view/4' would read through `proplists:get_value/2': a bare
%% `list' atom is `true' to it (`fix_path(true)' crashes after the budget), a
%% `list'-keyed tuple of another size is dropped by the parser. Both are
%% refused by the entry as passed.
bounded_fetch_refuses_malformed_list_entries_test_() ->
    [{Label,
      fun() ->
              ensure_couchbeam_supervisor(),
              assert_refused_before_transport(
                {'error', {'invalid_param', Entry}},
                fun(Db) ->
                        couchbeam_view:fetch_bounded(
                          Db, {<<"d">>, <<"v">>}, [{'limit', 1}, Entry],
                          {1000, 4096})
                end)
      end}
     || {Label, Entry} <- [{"bare atom", 'list'},
                           {"three-tuple", {'list', 'a', 'b'}}]].

%% The positive control for the guard above: a list name that is one path
%% segment reaches the wire in the path (and, as the parser keeps the pair,
%% in the query too); on `all_docs' it is never placed into the path, so it
%% is only a query half there and an integer passes the door.
bounded_fetch_sends_usable_list_name_in_the_path_test() ->
    ensure_couchbeam_supervisor(),
    assert_bounded_request_line(
      <<"GET /db/_design/d/_list/l/v?list=l HTTP/1.1">>,
      <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, {<<"d">>, <<"v">>}, [{'list', <<"l">>}], {1000, 4096})
      end).

bounded_fetch_all_docs_sends_list_option_as_a_query_half_test() ->
    ensure_couchbeam_supervisor(),
    assert_bounded_request_line(
      <<"GET /db/_all_docs?list=42 HTTP/1.1">>,
      <<"{\"total_rows\":0,\"offset\":0,\"rows\":[]}">>,
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'list', 42}], {1000, 4096})
      end).

%% Atom halves are refused by the door, not left to `hackney_url:fix_path/1'
%% after the budget: the address scan and the query scan disagree on atoms
%% (a query half renders an atom, a path segment does not).
bounded_fetch_refuses_atom_design_name_half_test() ->
    ensure_couchbeam_supervisor(),
    ViewName = {'d', <<"v">>},
    assert_refused_before_transport(
      {'error', {'invalid_view_name', ViewName}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, ViewName, [], {1000, 4096})
      end).

%% `?', `#' and emptiness re-address the request without any line
%% terminator: hackney cuts the URL at either character, and an empty
%% segment collapses the path — `PUT /doc' would create a database.
bounded_doors_refuse_db_names_that_are_not_one_path_segment_test_() ->
    [{Label,
      fun() ->
              ensure_couchbeam_supervisor(),
              assert_refused_before_transport(
                {'error', {'unsafe_db_name', Name}},
                fun(Db) -> Call(Db#db{name=Name}) end)
      end}
     || {Label, Name, Call} <-
            [{"db_info: empty", <<>>,
              fun(Db) -> couchbeam:db_info_bounded(Db, {1000, 1024}) end},
             {"open_doc: hash", <<"db#x">>,
              fun(Db) ->
                      couchbeam:open_doc_bounded(Db, <<"doc">>, [],
                                                 {1000, 1024})
              end},
             {"save_doc: question mark", <<"db?x">>,
              fun(Db) ->
                      couchbeam:save_doc_bounded(
                        Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 1024})
              end},
             {"fetch: string with hash", "db#x",
              fun(Db) ->
                      couchbeam_view:fetch_bounded(Db, 'all_docs', [],
                                                   {1000, 4096})
              end}]].

%% A slash in a segment re-addresses the request (`pathencode/1' keeps it);
%% Kazoo's percent-encoded database names (`account%2F…') are one segment
%% and pass, as the wire shows.
bounded_open_doc_refuses_slash_in_db_name_test() ->
    Name = <<"db/x">>,
    assert_refused_before_transport(
      {'error', {'unsafe_db_name', Name}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db#db{name=Name}, <<"doc">>, [], {1000, 1024})
      end).

bounded_fetch_refuses_slash_in_design_name_test() ->
    ensure_couchbeam_supervisor(),
    ViewName = {<<"d/x">>, <<"v">>},
    assert_refused_before_transport(
      {'error', {'invalid_view_name', ViewName}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, ViewName, [], {1000, 4096})
      end).

bounded_open_doc_sends_a_percent_encoded_db_name_as_one_segment_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(
                <<"GET /account%2Fab%2Fcd%2Fef/doc HTTP/1.1">>, Request),
              send_json_response(Socket, <<"{\"_id\":\"doc\"}">>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(
                             Server, <<"account%2Fab%2Fcd%2Fef">>),
              ?assertMatch({'ok', _, _},
                           couchbeam:open_doc_bounded(
                             Db, <<"doc">>, [], {1000, 1024})),
              assert_success_left_nothing_behind()
      end).

%% `?' and `#' alone re-address a view request the same way they do a
%% database: `GET /db/_design/d?x/_view/v' answers the design document.
bounded_fetch_refuses_view_name_halves_that_are_not_one_segment_test_() ->
    [{Label,
      fun() ->
              ensure_couchbeam_supervisor(),
              assert_refused_before_transport(
                {'error', {'invalid_view_name', ViewName}},
                fun(Db) ->
                        couchbeam_view:fetch_bounded(Db, ViewName, [],
                                                     {1000, 4096})
                end)
      end}
     || {Label, ViewName} <- [{"design: question mark", {<<"d?x">>, <<"v">>}},
                              {"view: hash", {<<"d">>, <<"v#x">>}},
                              {"design: string with hash", {"d#x", <<"v">>}}]].

%% The document id keeps the wider contract on purpose: `?' and `#' in it
%% are urlencoded by `couchbeam_util:encode_docid/1' and address the
%% document, not a query or a fragment.
bounded_open_doc_sends_an_encoded_doc_id_with_query_characters_test() ->
    assert_bounded_request_line(
      <<"GET /db/doc%3Fx%23y HTTP/1.1">>,
      <<"{\"_id\":\"doc?x#y\"}">>,
      fun(Db) ->
              couchbeam:open_doc_bounded(Db, <<"doc?x#y">>, [], {1000, 1024})
      end).

bounded_fetch_refuses_empty_view_name_half_test() ->
    ensure_couchbeam_supervisor(),
    ViewName = {<<"d">>, <<>>},
    assert_refused_before_transport(
      {'error', {'invalid_view_name', ViewName}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, ViewName, [], {1000, 4096})
      end).

%% The order of refusals is a contract, not a comment: the database name is
%% judged before the document, the view and the budget spec.
bounded_doors_refuse_the_db_name_before_anything_else_test_() ->
    Name = <<"db\r">>,
    [{"open_doc: before the doc id",
      ?_assertEqual({'error', {'unsafe_db_name', Name}},
                    refused_before_transport(
                      fun(Db) ->
                              couchbeam:open_doc_bounded(
                                Db#db{name=Name}, <<>>, [], {1000, 1024})
                      end, []))},
     {"db_info: before the budget spec",
      ?_assertEqual({'error', {'unsafe_db_name', Name}},
                    refused_before_transport(
                      fun(Db) ->
                              couchbeam:db_info_bounded(Db#db{name=Name},
                                                        'bogus')
                      end, []))},
     {"save_doc: before the document shape",
      ?_assertEqual({'error', {'unsafe_db_name', Name}},
                    refused_before_transport(
                      fun(Db) ->
                              couchbeam:save_doc_bounded(
                                Db#db{name=Name}, 'not_a_doc', [],
                                {1000, 1024})
                      end, []))},
     {"fetch: before the view name",
      fun() ->
              ensure_couchbeam_supervisor(),
              ?assertEqual({'error', {'unsafe_db_name', Name}},
                           refused_before_transport(
                             fun(Db) ->
                                     couchbeam_view:fetch_bounded(
                                       Db#db{name=Name}, 'bogus', [],
                                       {1000, 4096})
                             end, []))
      end}].

%% A first argument that is not a `#db{}' is a `function_clause' on every
%% door: the type is the contract.
bounded_doors_do_not_accept_a_non_db_first_argument_test_() ->
    [?_assertError('function_clause',
                   couchbeam:db_info_bounded('not_a_db', {1000, 1024})),
     ?_assertError('function_clause',
                   couchbeam:open_doc_bounded('not_a_db', <<"doc">>, [],
                                              {1000, 1024})),
     ?_assertError('function_clause',
                   couchbeam:save_doc_bounded('not_a_db',
                                              {[{<<"_id">>, <<"doc">>}]},
                                              [], {1000, 1024})),
     ?_assertError('function_clause',
                   couchbeam_view:fetch_bounded('not_a_db', 'all_docs', [],
                                                {1000, 4096}))].

%% What the doors refuse, observed on a legacy door: the split lands on the
%% wire, and the injected line arrives at the server as a header of its own.
legacy_db_info_leaves_the_path_split_to_the_encoder_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(<<"GET /db?x HTTP/1.1">>, Request),
              assert_request_header(<<"X-Injected">>, <<"yes">>, Request),
              send_json_response(Socket, <<"{\"db_name\":\"db\"}">>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(
                             Server,
                             <<"db?x HTTP/1.1\r\nX-Injected: yes\r\nX:">>),
              ?assertMatch({'ok', _}, couchbeam:db_info(Db))
      end).

%% The two predicates the doors are built on, by shape.
request_line_safe_and_addressable_by_shape_test_() ->
    Safe = fun couchbeam_httpc:request_line_safe/1,
    Addr = fun couchbeam_httpc:addressable/1,
    [?_assert(Safe(<<"db">>)), ?_assert(Safe("db")), ?_assert(Safe(<<>>)),
     ?_assert(Safe(<<"a b?c#d/e">>)), ?_assert(Safe("\351")),
     ?_assertNot(Safe(<<"a\rb">>)), ?_assertNot(Safe("a\n")),
     ?_assertNot(Safe(["d", <<"b">>])), ?_assertNot(Safe([1000])),
     ?_assertNot(Safe('db')), ?_assertNot(Safe({'hackney_url', 'http'})),
     ?_assert(Addr(<<"db">>)), ?_assert(Addr("db")),
     ?_assert(Addr("db\351")),
     ?_assertNot(Addr(<<"%2E">>)), ?_assertNot(Addr(<<"%2e%2E">>)),
     ?_assert(Addr(<<"account%2Fab%2Fcd">>)),
     ?_assertNot(Addr(<<>>)), ?_assertNot(Addr("")),
     ?_assertNot(Addr(<<"/">>)), ?_assertNot(Addr("//")),
     ?_assertNot(Addr(<<"db/x">>)), ?_assertNot(Addr(<<"db?x">>)),
     ?_assertNot(Addr(<<"db#x">>)), ?_assertNot(Addr(<<"db\n">>)),
     ?_assertNot(Addr(<<"db x">>)), ?_assertNot(Addr("d\tx")),
     ?_assertNot(Addr(<<".">>)), ?_assertNot(Addr("..")),
     ?_assert(Addr(<<"...">>)), ?_assert(Addr(<<".x">>)),
     ?_assert(Addr(<<"x.">>)),
     ?_assertNot(Addr('db')), ?_assertNot(Addr(42))].

%% The transport-door helpers by shape (TEST exports): the header scan names
%% a pair by its name and a non-pair whole, accepts hackney's parameterised
%% value (pairs and bare keys) but never as a name, and the cookie option is
%% judged by what `hackney_cookie:setcookie/3' writes or refuses; a method
%% is letters, by code point.
transport_door_helpers_by_shape_test_() ->
    Cyrillic = list_to_atom([$G, 16#415, $T]),
    [?_assertEqual('safe', couchbeam_httpc:unsafe_header([])),
     ?_assertEqual('safe', couchbeam_httpc:unsafe_header(
                             [{<<"X-A">>, {<<"v">>, [{<<"k">>, <<"1">>}]}},
                              {'x', 1}, {"X-B", "y"}])),
     ?_assertEqual('safe', couchbeam_httpc:unsafe_header(
                             [{<<"X-A">>, {<<"v">>, [<<"k">>, "j",
                                                     {<<"a">>, <<"b">>}]}}])),
     ?_assertEqual({'unsafe', <<"X-A">>},
                   couchbeam_httpc:unsafe_header(
                     [{<<"X-A">>, {<<"v">>, [<<"k\n">>]}}])),
     ?_assertEqual({'unsafe', <<"X-A">>},
                   couchbeam_httpc:unsafe_header(
                     [{<<"X-A">>, {<<"v">>, [{'a', 'b', 'c'}]}}])),
     ?_assertEqual({'unsafe', {<<"X-A">>, []}},
                   couchbeam_httpc:unsafe_header([{{<<"X-A">>, []}, <<"1">>}])),
     ?_assertEqual({'unsafe', <<"X-A">>},
                   couchbeam_httpc:unsafe_header(
                     [{<<"X-A">>, {<<"v">>, [{<<"k">>, <<"1\r">>}]}}])),
     ?_assertEqual({'unsafe', <<"X-A">>},
                   couchbeam_httpc:unsafe_header([{<<"X-A">>, 1.5}])),
     ?_assertEqual({'unsafe', 'bare'},
                   couchbeam_httpc:unsafe_header(
                     [{<<"X-A">>, <<"1">>}, 'bare'])),
     ?_assertEqual({'unsafe', {'x', 'y', 'z'}},
                   couchbeam_httpc:unsafe_header([{'x', 'y', 'z'}])),
     ?_assertEqual('safe', couchbeam_httpc:unsafe_cookie([])),
     ?_assertEqual('safe', couchbeam_httpc:unsafe_cookie(
                             [{'cookie', [{<<"a">>, <<"1">>}, <<"b=2">>]}])),
     ?_assertEqual('safe', couchbeam_httpc:unsafe_cookie(
                             [{'cookie', {<<"a">>, <<"1=2">>,
                                          [{'domain', <<"x">>},
                                           {'path', "/p"},
                                           {'secure', 'true'}]}}])),
     ?_assertEqual('safe', couchbeam_httpc:unsafe_cookie(
                             [{'cookie', <<"a=1; b=2 c">>}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie([{'cookie', 'not_a_cookie'}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie(
                     [{'cookie', {<<"a">>, <<"1">>,
                                  [{'domain', <<"x\r\nX: y">>}]}}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie(
                     [{'cookie', {<<"a">>, <<"1">>,
                                  [{'path', <<"/\nX: y">>}]}}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie(
                     [{'cookie', {<<"a">>, <<"1 2">>}}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie(
                     [{'cookie', {<<"a=b">>, <<"1">>}}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie([{'cookie', {'a', <<"1">>}}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie([{'cookie', [[<<"a=1">>]]}])),
     ?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie(
                     [{'cookie', {<<"a">>, <<"1">>, 'x'}}])),
     ?_assert(couchbeam_httpc:usable_method('get')),
     ?_assert(couchbeam_httpc:usable_method(<<"COPY">>)),
     ?_assert(couchbeam_httpc:usable_method("put")),
     ?_assertNot(couchbeam_httpc:usable_method(<<"GET /x">>)),
     ?_assertNot(couchbeam_httpc:usable_method(<<"GET\r\n">>)),
     ?_assertNot(couchbeam_httpc:usable_method(<<"GET\n">>)),
     ?_assertNot(couchbeam_httpc:usable_method("get\n")),
     ?_assertNot(couchbeam_httpc:usable_method(Cyrillic)),
     ?_assertNot(couchbeam_httpc:usable_method('')),
     ?_assertNot(couchbeam_httpc:usable_method([$G | $E])),
     ?_assertNot(couchbeam_httpc:usable_method(42)),
     ?_assertNot(couchbeam_httpc:usable_method(<<>>))].

%% The transport door is the one point every bounded caller passes (the
%% consumer's maintenance module builds its `_find'/`_bulk_docs' URLs itself
%% and calls `db_request_bounded/7'; `request_bounded/6,7' is exported too):
%% a method, a URL or a header that would split or truncate the request line
%% is refused there whatever built it. The URL is named by its path only —
%% the consumer carries the CouchDB credentials in it — and the refusal
%% happens before any connection.
db_request_bounded_refuses_unsafe_request_lines_test_() ->
    Cyrillic = list_to_atom([$G, 16#415, $T]),
    Cases =
        [{"URL that hackney cannot parse",
          fun(Url) ->
                  {'post', binary:replace(Url, <<"http://">>,
                                            <<"http://u:p?x@">>), []}
          end,
          [<<"db">>, <<"_find">>],
          {'unsafe_url', 'undefined'}},
         {"URL with CR/LF in the path",
          fun(Url) -> {'post', Url, []} end,
          [<<"db\r\nX: y">>, <<"_find">>],
          {'unsafe_url', <<"/db\r\nX: y/_find">>}},
         {"URL with CR/LF past a question mark",
          fun(Url) -> {'post', Url, []} end,
          [<<"db?x\r\nX: y">>, <<"_find">>],
          {'unsafe_url', <<"/db">>}},
         {"URL with a hash: the fragment is never sent",
          fun(Url) -> {'post', Url, []} end,
          [<<"db#x">>, <<"_find">>],
          {'unsafe_url', <<"/db">>}},
         {"string URL with a line terminator",
          fun(Url) -> {'post', binary_to_list(Url), []} end,
          [<<"db?x\nX: y">>, <<"_find">>],
          {'unsafe_url', <<"/db">>}},
         {"method with a line terminator",
          fun(Url) -> {<<"POST\r\nX: y">>, Url, []} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_method', <<"POST\r\nX: y">>}},
         {"caller header value with a line terminator",
          fun(Url) ->
                  {'post', Url, [{<<"X-A">>, <<"1\r\nX-Injected: yes">>}]}
          end,
          [<<"db">>, <<"_find">>],
          {'unsafe_header', <<"X-A">>}},
         {"caller header name with a line terminator",
          fun(Url) -> {'post', Url, [{<<"X-A\n">>, <<"1">>}]} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_header', <<"X-A\n">>}},
         {"header named by the all-clear atom `safe', with a line terminator",
          fun(Url) -> {'post', Url, [{'safe', <<"1\r\nX: y">>}]} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_header', 'safe'}},
         {"header name in hackney's parameterised form",
          fun(Url) -> {'post', Url, [{{<<"X-A">>, []}, <<"1">>}]} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_header', {<<"X-A">>, []}}},
         {"method with a bare trailing LF",
          fun(Url) -> {<<"GET\n">>, Url, []} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_method', <<"GET\n">>}},
         {"atom method beyond Latin-1",
          fun(Url) -> {Cyrillic, Url, []} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_method', Cyrillic}},
         {"URL with a tab in the query",
          fun(Url) -> {'post', <<Url/binary, "?a\tb">>, []} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_url', <<"/db/_find">>}},
         {"method with a space",
          fun(Url) -> {<<"GET /x HTTP/1.1">>, Url, []} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_method', <<"GET /x HTTP/1.1">>}},
         {"integer method",
          fun(Url) -> {42, Url, []} end,
          [<<"db">>, <<"_find">>],
          {'unsafe_method', 42}},
         {"URL with a space in the path: refused by segment policy",
          fun(Url) -> {'post', Url, []} end,
          [<<"db x">>, <<"_find">>],
          {'unsafe_url', <<"/db x/_find">>}}],
    [{Label,
      fun() ->
              Verdict = refused_before_transport(
                          fun(#db{server=Server, options=Opts}) ->
                                  Url = hackney_url:make_url(
                                          couchbeam_httpc:server_url(Server),
                                          Parts, []),
                                  {Method, ReqUrl, Headers} = Shape(Url),
                                  {'ok', Budget} =
                                      couchbeam_httpc:new_request_budget(
                                        {1000, 1024}),
                                  couchbeam_httpc:db_request_bounded(
                                    Method, ReqUrl, Headers, <<"{}">>, Opts,
                                    [200], Budget)
                          end, []),
              ?assertEqual({'error', Expected}, Verdict)
      end}
     || {Label, Shape, Parts, Expected} <- Cases].

%% The netloc is not the request line: the consumer keeps its CouchDB
%% credentials there raw, hackney turns them into a `basic_auth' option and
%% an `Authorization' header, and a password with a space reaches the wire
%% as base64 — no reason to refuse the request. The door judges
%% `#hackney_url.raw_path', see `unsafe_request_line/2'.
bounded_request_leaves_credentials_with_a_space_alone_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(<<"GET /db HTTP/1.1">>, Request),
              assert_request_header(
                <<"Authorization">>,
                <<"Basic ", (base64:encode(<<"u:p w">>))/binary>>, Request),
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              <<"http://", HostPort/binary>> = BaseUrl,
              Server = couchbeam:server_connection(
                         <<"http://u:p w@", HostPort/binary>>,
                         [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch({'ok', _, _},
                           couchbeam:db_info_bounded(Db, {1000, 1024})),
              assert_success_left_nothing_behind()
      end).

%% The `cookie' request option becomes a `Cookie' header inside hackney,
%% after any header scan of the caller's list; every shape hackney accepts
%% is judged here first.
db_request_bounded_refuses_a_line_terminated_cookie_option_test_() ->
    [{Label,
      fun() ->
              Verdict = refused_before_transport(
                          fun(#db{server=Server, options=Opts}) ->
                                  Url = hackney_url:make_url(
                                          couchbeam_httpc:server_url(Server),
                                          [<<"db">>, <<"_find">>], []),
                                  {'ok', Budget} =
                                      couchbeam_httpc:new_request_budget(
                                        {1000, 1024}),
                                  couchbeam_httpc:db_request_bounded(
                                    'post', Url, [], <<"{}">>,
                                    [{'cookie', Cookie} | Opts], [200], Budget)
                          end, []),
              ?assertEqual({'error', {'unsafe_header', <<"Cookie">>}}, Verdict)
      end}
     || {Label, Cookie} <- [{"binary", <<"a=1\r\nX: y">>},
                            {"pair", {<<"a">>, <<"1\nX: y">>}},
                            {"triple", {<<"a\r">>, <<"1">>, []}},
                            {"triple with a path option",
                             {<<"a">>, <<"1">>, [{'path', <<"/\r\nX: y">>}]}},
                            {"triple with a domain option",
                             {<<"a">>, <<"1">>, [{'domain', <<"x\nX: y">>}]}},
                            {"pair whose value hackney refuses (a space)",
                             {<<"a">>, <<"1 2">>}},
                            {"pair whose name hackney refuses (a semicolon)",
                             {<<"a;b">>, <<"1">>}},
                            {"list of pairs",
                             [{<<"a">>, <<"1">>},
                              {<<"b">>, <<"2\r\nX: y">>}]}]].

%% The header the module adds itself is as caller-controlled as the ones
%% handed in: `X-Kazoo-Log-ID' is the process call id, which the consumer
%% sets from the request it serves.
db_request_bounded_refuses_a_line_terminated_call_id_header_test() ->
    Previous = kz_log:get_callid(),
    _ = kz_log:put_callid(<<"req\r\nX-Injected: yes">>),
    try
        Verdict = refused_before_transport(
                    fun(#db{server=Server, options=Opts}) ->
                            Url = hackney_url:make_url(
                                    couchbeam_httpc:server_url(Server),
                                    [<<"db">>, <<"_find">>], []),
                            {'ok', Budget} =
                                couchbeam_httpc:new_request_budget(
                                  {1000, 1024}),
                            couchbeam_httpc:db_request_bounded(
                              'post', Url, [], <<"{}">>, Opts, [200], Budget)
                    end, []),
        ?assertEqual({'error', {'unsafe_header', <<"X-Kazoo-Log-ID">>}},
                     Verdict)
    after
        _ = kz_log:put_callid(Previous)
    end.

%% A refusal raised inside the view stream — `couchbeam_view_stream' runs
%% the transport door in the stream process, with the caller's call id
%% adopted through the `kz_log_id' stream option — reaches the
%% `fetch_bounded/4' caller as the door's own reason, with nothing left
%% behind: no connection, no lifecycle message, no process of the bounded
%% transport.
bounded_fetch_refuses_a_line_terminated_call_id_inside_the_stream_test() ->
    ensure_couchbeam_supervisor(),
    Previous = kz_log:get_callid(),
    _ = kz_log:put_callid(<<"req\r\nX-Injected: yes">>),
    try
        Verdict = refused_before_transport(
                    fun(Db) ->
                            couchbeam_view:fetch_bounded(
                              Db, 'all_docs', [], {1000, 1024})
                    end, []),
        ?assertEqual({'error', {'unsafe_header', <<"X-Kazoo-Log-ID">>}},
                     Verdict),
        assert_no_lifecycle_messages(),
        assert_no_bounded_processes_left()
    after
        _ = kz_log:put_callid(Previous)
    end.

%% The budget shape is judged before the request line on the transport door
%% (`db_request_bounded/7' pattern-matches the map first).
db_request_bounded_judges_the_budget_before_the_url_test() ->
    Verdict = refused_before_transport(
                fun(#db{server=Server, options=Opts}) ->
                        Url = hackney_url:make_url(
                                couchbeam_httpc:server_url(Server),
                                [<<"db?x\r\nX: y">>, <<"_find">>], []),
                        couchbeam_httpc:db_request_bounded(
                          'post', Url, [], <<"{}">>, Opts, [200], 'bogus')
                end, []),
    ?assertEqual({'error', 'invalid_request_budget'}, Verdict).

bounded_fetch_refuses_unencodable_key_value_test() ->
    ensure_couchbeam_supervisor(),
    Pid = self(),
    assert_refused_before_transport(
      {'error', {'invalid_param', {'startkey', Pid}}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(
                Db, 'all_docs', [{'limit', 10}, {'startkey', Pid}],
                {1000, 1024})
      end).

%% An expired budget must not spawn the encoder at all: the caller is traced
%% for `spawn' events while it makes the call.
bounded_encode_json_expired_budget_spawns_no_worker_test() ->
    drain_transport_messages(),
    {'ok', Budget} = couchbeam_httpc:new_request_budget({1, 1024}),
    timer:sleep(5),
    Parent = self(),
    Caller = spawn(
               fun() ->
                       receive 'encode_now' -> 'ok' end,
                       Parent ! {'expired_encode_result', self(),
                                 couchbeam_httpc:bounded_encode_json(
                                   {[{<<"_id">>, <<"doc">>}]}, Budget,
                                   [{'bounded_encode_test_delay_ms', 500}])}
               end),
    Session = trace:session_create('bounded_encode_spawn_probe', self(), []),
    try
        1 = trace:process(Session, Caller, 'true', ['procs']),
        Caller ! 'encode_now',
        receive
            {'expired_encode_result', Caller, Result} ->
                ?assertEqual({'error', 'timeout'}, Result)
        after 1000 ->
                exit(Caller, 'kill'),
                ?assert('false')
        end,
        ?assertEqual([], caller_spawn_events(Caller)),
        ?assertEqual([], json_encoder_processes())
    after
        trace:session_destroy(Session),
        drain_transport_messages()
    end.

caller_spawn_events(Caller) ->
    receive
        {'trace', Caller, 'spawn', SpawnedPid, MFA} ->
            [{SpawnedPid, MFA} | caller_spawn_events(Caller)]
    after 100 ->
            []
    end.

%% `invalid_request_budget' is a typed refusal on every door, not only on
%% `open_doc_bounded/4', and the ceiling is refused at the API boundary too.
bounded_db_info_rejects_invalid_budget_spec_test() ->
    assert_refused_before_transport(
      {'error', 'invalid_request_budget'},
      fun(Db) -> couchbeam:db_info_bounded(Db, {0, 10}) end).

bounded_save_doc_rejects_invalid_budget_spec_test() ->
    assert_refused_before_transport(
      {'error', 'invalid_request_budget'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"doc">>}]}, [], {1000, 0})
      end).

bounded_fetch_rejects_invalid_budget_spec_test() ->
    ensure_couchbeam_supervisor(),
    assert_refused_before_transport(
      {'error', 'invalid_request_budget'},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, 'all_docs', [], {0, 10})
      end).

bounded_db_info_rejects_budget_above_ceiling_test() ->
    assert_refused_before_transport(
      {'error', 'invalid_request_budget'},
      fun(Db) ->
              couchbeam:db_info_bounded(Db, {16#FFFFFFFF div 3 + 1, 1024})
      end).

%% A row cut inside its object — inside a string, then inside a number — must
%% cross the chunk boundary intact: the jsx continuation carrying the partial
%% row is handed from one decoder process to the next.
bounded_view_decodes_row_split_inside_object_across_chunks_test() ->
    ensure_couchbeam_supervisor(),
    Chunks = [<<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"al">>,
              <<"pha\",\"key\":12">>,
              <<"34,\"value\":{\"n\":\"x\"}},{\"id\":\"beta\",\"key\":5,\"va">>,
              <<"lue\":{}}]}">>],
    BodySize = lists:sum([byte_size(Chunk) || Chunk <- Chunks]),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_chunked_headers(Socket),
              lists:foreach(fun(Chunk) ->
                                    'ok' = send_http_chunk(Socket, Chunk),
                                    timer:sleep(10)
                            end, Chunks),
              'ok' = send_http_chunk(Socket, <<>>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'ok', [{[{<<"id">>, <<"alpha">>}, {<<"key">>, 1234},
                          {<<"value">>, {[{<<"n">>, <<"x">>}]}}]},
                        {[{<<"id">>, <<"beta">>}, {<<"key">>, 5},
                          {<<"value">>, {[]}}]}],
                  BodySize},
                 couchbeam_view:fetch_bounded(
                   Db, 'all_docs', [], {2000, 1024}))
      end).

%% `_design/…' ids keep their slash on the wire through the bounded doors,
%% as `couchbeam_util:encode_docid/1' does for the legacy ones.
bounded_open_doc_sends_design_doc_request_line_test() ->
    assert_bounded_request_line(
      <<"GET /db/_design/ddoc HTTP/1.1">>,
      <<"{\"_id\":\"_design/ddoc\",\"views\":{}}">>,
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"_design/ddoc">>, [], {1000, 1024})
      end).

bounded_save_doc_sends_design_doc_request_line_test() ->
    assert_bounded_request_line(
      <<"PUT /db/_design/ddoc HTTP/1.1">>,
      <<"{\"ok\":true,\"id\":\"_design/ddoc\",\"rev\":\"1-a\"}">>,
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"_id">>, <<"_design/ddoc">>}, {<<"views">>, {[]}}]},
                [], {1000, 1024})
      end).

%% The direct door twin of the `status' phase above: the peer closes before
%% the status line while the caller is held before its hand-off; the manager
%% has forgotten the ref by the time `controlling_process' runs, and the
%% verdict is still hackney's `closed', not `{request_ownership, badarg}'.
bounded_db_info_peer_close_before_handoff_reports_transport_reason_test_() ->
    {'timeout', 30, fun bounded_db_info_peer_close_before_handoff_reports_transport_reason/0}.

bounded_db_info_peer_close_before_handoff_reports_transport_reason() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
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
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {1000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'peer_close_result_direct', self(),
                                           Result, Messages}
                         end),
              receive
                  {'bounded_handoff_ready', Ref, Caller} ->
                      hold_handoff_until_transport_gone('status', Ref),
                      Caller ! {'bounded_handoff_continue', Ref}
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              receive
                  {'peer_close_result_direct', Caller, Result, Messages} ->
                      ?assertEqual({'error', 'closed'}, Result),
                      ?assertEqual([], [M || M <- Messages,
                                             is_stray_transport_message(M)])
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

%% The other order of the same status-phase peer close: the hand-off came
%% first, the status line never comes, and the peer closes afterwards —
%% hackney reports `closed' to the stream waiting in `do_init_stream/2',
%% which relays it verbatim and at once, not as the deadline's `timeout'.
bounded_view_status_transport_error_after_handoff_closes_peer_test_() ->
    {'timeout', 30, fun bounded_view_status_transport_error_after_handoff_closes_peer/0}.

bounded_view_status_transport_error_after_handoff_closes_peer() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'status_server_ready', self()},
              receive 'close_now' -> 'ok' end,
              'ok' = gen_tcp:shutdown(Socket, 'write'),
              ServerParent ! {'peer_close_result', recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'status_after_handoff_result',
                                           self(),
                                           couchbeam_view:fetch_bounded(
                                             Db, 'all_docs', [], {1000, 1024})}
                         end),
              {WorkerPid, LeasePid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, _Guardian,
                       _Stream} ->
                          {Worker, Lease}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              ServerPid = receive
                              {'status_server_ready', Pid} -> Pid
                          after 1000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              %% the lease owns the request: the hand-off is done
              'ok' = await_ref_owner(Ref, LeasePid),
              ServerPid ! 'close_now',
              receive
                  {'status_after_handoff_result', Caller, Result} ->
                      ?assertEqual({'error', 'closed'}, Result)
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

%% Chunked transfer on the document door: hackney hands over de-framed body
%% pieces, so the cumulative byte count is the payload alone.
bounded_open_doc_counts_chunked_body_without_framing_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Chunks = [<<"{\"_id\":\"doc\",">>, <<"\"value\":1}">>],
    BodySize = lists:sum([byte_size(Chunk) || Chunk <- Chunks]),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_chunked_headers(Socket),
              lists:foreach(fun(Chunk) ->
                                    'ok' = send_http_chunk(Socket, Chunk),
                                    timer:sleep(10)
                            end, Chunks),
              'ok' = send_http_chunk(Socket, <<>>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'ok', {[{<<"_id">>, <<"doc">>}, {<<"value">>, 1}]},
                  BodySize},
                 couchbeam:open_doc_bounded(Db, <<"doc">>, [], {2000, 1024})),
              assert_no_stray_transport_messages()
      end).

%% --- round 8 regressions ---------------------------------------------------

%% The guardian is killed from outside after the stream entered its body
%% loop. Its dependents die with it, and the manager closes the socket under
%% the stream (`hackney_manager:handle_owner_exit/4' terminates the async
%% stream and closes the transport), which then hears nothing until its
%% deadline. `fail_stream/2' proves the cleanup against the manager table
%% (`couchbeam_httpc:recover_after_guardian_exit/4') and must announce it to
%% the collector, which learnt the guardian's pid and would otherwise wait a
%% whole cleanup budget for a verdict the dead guardian cannot send: the
%% deadline's `timeout' at the deadline — not `transport_cleanup_timeout' a
%% budget later — with nothing left behind.
bounded_view_guardian_death_during_body_is_final_at_deadline_test_() ->
    {'timeout', 30,
     fun bounded_view_guardian_death_during_body_is_final_at_deadline/0}.

bounded_view_guardian_death_during_body_is_final_at_deadline() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    FirstChunk = <<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"first\",\"key\":\"first\",\"value\":{}},">>,
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, FirstChunk),
              ServerParent ! {'body_guardian_death_chunk_sent', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {1000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'body_guardian_death_result',
                                           self(), Result, Messages}
                         end),
              {WorkerPid, LeasePid, GuardianPid, StreamPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Stream} ->
                          {Worker, Lease, Guardian, Stream}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              ?assert(StreamPid =/= Caller),
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              'ok' = await_ref_owner(Ref, LeasePid),
              receive
                  {'body_guardian_death_chunk_sent', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% the stream is past `do_init_stream/2' and waits for the
              %% next transport message: the death is a mid-body one
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case process_info(StreamPid, 'current_function') of
                               {'current_function',
                                {'couchbeam_view_stream',
                                 'bounded_loop_receive', 4}} ->
                                   {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 1000)),
              exit(GuardianPid, 'kill'),
              receive
                  {'body_guardian_death_result', Caller, Result, Messages} ->
                      ?assertEqual({'error', 'timeout'}, Result),
                      ?assertEqual([], lifecycle_messages(Messages)),
                      ?assertEqual([], [M || M <- Messages,
                                             is_stray_transport_message(M)])
              after 1600 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% at the 1000 ms deadline, not a 1000 ms cleanup budget later
              ?assert(erlang:monotonic_time('millisecond') - Started < 1500),
              assert_process_gone(WorkerPid),
              assert_process_gone(LeasePid),
              assert_process_gone(StreamPid),
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% The status-phase twin of the scenario above: the guardian is killed while
%% the stream waits for the status line inside `do_init_stream/2'. Its lease
%% dies with it, `hackney_manager' tears the transport down under the
%% stream, which hears nothing until its deadline; `cancel_result/3' proves
%% the cleanup against the table and must announce it, or the collector —
%% which knows the guardian — waits a whole cleanup budget for a verdict
%% nobody can send: `timeout' at T, not `transport_cleanup_timeout' at 2T,
%% with nothing left behind.
bounded_view_guardian_death_during_status_is_final_at_deadline_test_() ->
    {'timeout', 30,
     fun bounded_view_guardian_death_during_status_is_final_at_deadline/0}.

bounded_view_guardian_death_during_status_is_final_at_deadline() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              %% the request head is already read by `with_http_server/2';
              %% the status line is never sent
              ServerParent ! {'status_guardian_death_request_read', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {1000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'status_guardian_death_result',
                                           self(), Result, Messages}
                         end),
              {WorkerPid, LeasePid, GuardianPid, StreamPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Stream} ->
                          {Worker, Lease, Guardian, Stream}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              ?assert(StreamPid =/= Caller),
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              'ok' = await_ref_owner(Ref, LeasePid),
              receive
                  {'status_guardian_death_request_read', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% the hand-off is done and the stream waits for the status
              %% line inside `do_init_stream/2': a status-phase death
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case process_info(StreamPid, 'current_function') of
                               {'current_function',
                                {'couchbeam_view_stream', 'do_init_stream',
                                 2}} ->
                                   {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 1000)),
              exit(GuardianPid, 'kill'),
              receive
                  {'status_guardian_death_result', Caller, Result, Messages} ->
                      ?assertEqual({'error', 'timeout'}, Result),
                      ?assertEqual([], lifecycle_messages(Messages)),
                      ?assertEqual([], [M || M <- Messages,
                                             is_stray_transport_message(M)])
              after 1600 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% at the 1000 ms deadline, not a 1000 ms cleanup budget later
              ?assert(erlang:monotonic_time('millisecond') - Started < 1500),
              assert_process_gone(WorkerPid),
              assert_process_gone(LeasePid),
              assert_process_gone(StreamPid),
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% The guardian is killed mid-body while the manager is suspended, so its
%% row stays: at the deadline the stream proves nothing against the table
%% and reports `transport_cleanup_timeout' one cleanup budget later — on a
%% budget that started strictly after the collector's own fallback. The
%% collector must wait for that report (the delivery allowance
%% `bounded_cleanup_wait_budget/2' grants a known guardian) rather than time
%% out an instant before it and leave it in the caller's mailbox. The
%% mailbox is read after a grace period: the stream is alive when the call
%% returns, so a snapshot taken at once would not see a late report.
bounded_view_guardian_death_with_stuck_manager_row_leaves_no_late_report_test_() ->
    {'timeout', 30,
     fun bounded_view_guardian_death_with_stuck_manager_row_leaves_no_late_report/0}.

bounded_view_guardian_death_with_stuck_manager_row_leaves_no_late_report() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    FirstChunk = <<"{\"total_rows\":2,\"offset\":0,\"rows\":[{\"id\":\"first\",\"key\":\"first\",\"value\":{}},">>,
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, FirstChunk),
              ServerParent ! {'stuck_row_chunk_sent', self()},
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 5000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {500, 1024}),
                                 Elapsed = erlang:monotonic_time('millisecond')
                                     - Started,
                                 timer:sleep(150),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'stuck_row_result', self(), Result,
                                           Elapsed, Messages}
                         end),
              {WorkerPid, LeasePid, GuardianPid, StreamPid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, Guardian,
                       Stream} ->
                          {Worker, Lease, Guardian, Stream}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              'ok' = await_ref_owner(Ref, LeasePid),
              receive
                  {'stuck_row_chunk_sent', _ServerPid} -> 'ok'
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case process_info(StreamPid, 'current_function') of
                               {'current_function',
                                {'couchbeam_view_stream',
                                 'bounded_loop_receive', 4}} ->
                                   {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 1000)),
              ManagerWatcher = suspend_manager(),
              try
                  exit(GuardianPid, 'kill'),
                  receive
                      {'stuck_row_result', Caller, Result, Elapsed,
                       Messages} ->
                          ?assertEqual({'error', 'transport_cleanup_timeout'},
                                       Result),
                          %% the stream's report — deadline plus its cleanup
                          %% budget — not the collector's own allowance
                          ?assert(Elapsed >= 900),
                          ?assert(Elapsed < 1600),
                          ?assertEqual([], lifecycle_messages(Messages)),
                          ?assertEqual([], [M || M <- Messages,
                                                 is_stray_transport_message(M)])
                  after 3000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end
              after
                  'ok' = resume_manager(ManagerWatcher)
              end,
              assert_process_gone(WorkerPid),
              assert_process_gone(LeasePid),
              assert_process_gone(StreamPid),
              %% resumed, the manager works through the lease's exit
              ?assertEqual(
                 'true',
                 await_condition(
                   fun() ->
                           case ets:lookup('hackney_manager_refs', Ref) of
                               [] -> {'true', 'true'};
                               _ -> 'false'
                           end
                   end, 2000)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 5500 ->
                      ?assert('false')
              end
      end).

%% The other side of the same class: the view was read completely, the
%% stream asked the guardian to clean up, and the guardian is killed while
%% that request is in flight (held by the cleanup hook, so the relay already
%% watches it). The relay reports the death with its reason,
%% `close_request/1' proves the cleanup by the table, and the stream
%% announces the proof before `done': the collector, which knows the
%% guardian, answers at once with the rows.
bounded_view_guardian_death_during_cleanup_after_done_succeeds_test_() ->
    {'timeout', 30,
     fun bounded_view_guardian_death_during_cleanup_after_done_succeeds/0}.

bounded_view_guardian_death_during_cleanup_after_done_succeeds() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Parent = self(),
    Json = <<"{\"total_rows\":1,\"offset\":0,\"rows\":[{\"id\":\"doc\",\"key\":\"doc\",\"value\":{}}]}">>,
    with_http_server(
      fun(Socket, ServerParent) ->
              send_json_response(Socket, Json),
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_cleanup_hold_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam_view:fetch_bounded(
                                            Db, 'all_docs', [], {1000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'cleanup_guardian_death_result',
                                           self(), Result, Messages}
                         end),
              {GuardianPid, Ref} =
                  receive
                      {'bounded_guardian_cleanup_held', Guardian, HeldRef}
                        when is_reference(HeldRef) ->
                          {Guardian, HeldRef}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              exit(GuardianPid, 'kill'),
              receive
                  {'cleanup_guardian_death_result', Caller, Result,
                   Messages} ->
                      ?assertEqual(
                         {'ok', [{[{<<"id">>, <<"doc">>}, {<<"key">>, <<"doc">>},
                                   {<<"value">>, {[]}}]}],
                          byte_size(Json)},
                         Result),
                      ?assertEqual([], lifecycle_messages(Messages)),
                      ?assertEqual([], [M || M <- Messages,
                                             is_stray_transport_message(M)])
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              %% not a cleanup budget later
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% The direct-door form of the same death: the body was read, the caller
%% asked the guardian to clean up, and the guardian is killed while the relay
%% watches it (held by the cleanup hook). The relay reports the death with
%% its reason and `close_request/1' proves the cleanup against the manager
%% table, exactly as for a guardian gone before the relay could watch it
%% (`bounded_body_read_after_guardian_death_verifies_cleanup_by_table'):
%% the verdict is the result, not `transport_cleanup_timeout'.
bounded_db_info_guardian_death_during_cleanup_recovers_by_table_test_() ->
    {'timeout', 30,
     fun bounded_db_info_guardian_death_during_cleanup_recovers_by_table/0}.

bounded_db_info_guardian_death_during_cleanup_recovers_by_table() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Parent = self(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, ServerParent) ->
              send_json_response(Socket, Body),
              ServerParent ! {'peer_close_result',
                              recv_until_closed(Socket, 3000)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_guardian_cleanup_hold_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {1000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'direct_cleanup_guardian_death_result',
                                           self(), Result, Messages}
                         end),
              {GuardianPid, Ref} =
                  receive
                      {'bounded_guardian_cleanup_held', Guardian, HeldRef}
                        when is_reference(HeldRef) ->
                          {Guardian, HeldRef}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              exit(GuardianPid, 'kill'),
              receive
                  {'direct_cleanup_guardian_death_result', Caller, Result,
                   Messages} ->
                      ?assertEqual(
                         {'ok', {[{<<"db_name">>, <<"db">>}]}, byte_size(Body)},
                         Result),
                      ?assertEqual([], [M || M <- Messages,
                                             is_stray_transport_message(M)])
              after 1000 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 3500 ->
                      ?assert('false')
              end
      end).

%% Document-door twins of `assert_view_transport_error_closed/1', with the
%% hand-off already done: the peer closes before the status line, between
%% the status line and the end of the headers, or inside the body, and the
%% reader's `{'error', Reason}' clauses (`bounded_response_status/2',
%% `bounded_response_headers/3', `bounded_body_control/5') must report
%% hackney's reason verbatim and at once — not as an unexpected message, not
%% as the deadline's `timeout' a second later — with the row gone.
bounded_open_doc_status_transport_error_after_handoff_test_() ->
    {'timeout', 30, fun bounded_open_doc_status_transport_error_after_handoff/0}.

bounded_open_doc_status_transport_error_after_handoff() ->
    assert_doc_transport_error_after_handoff('status').

bounded_open_doc_headers_transport_error_after_handoff_test_() ->
    {'timeout', 30, fun bounded_open_doc_headers_transport_error_after_handoff/0}.

bounded_open_doc_headers_transport_error_after_handoff() ->
    assert_doc_transport_error_after_handoff('headers').

bounded_open_doc_body_transport_error_after_handoff_test_() ->
    {'timeout', 30, fun bounded_open_doc_body_transport_error_after_handoff/0}.

bounded_open_doc_body_transport_error_after_handoff() ->
    assert_doc_transport_error_after_handoff('body').

assert_doc_transport_error_after_handoff(Phase) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              'ok' = maybe_send_transport_error_headers(Socket, Phase),
              ServerParent ! {'doc_transport_error_server_ready', self()},
              receive 'close_now' -> 'ok' end,
              'ok' = gen_tcp:shutdown(Socket, 'write'),
              ServerParent ! {'peer_close_result', recv_until_closed(Socket)},
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:open_doc_bounded(
                                            Db, <<"doc">>, [], {1000, 1024}),
                                 {'messages', Messages} =
                                     process_info(self(), 'messages'),
                                 Parent ! {'doc_transport_error_result',
                                           self(), Result, Messages}
                         end),
              {WorkerPid, LeasePid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, _Guardian,
                       Caller} ->
                          {Worker, Lease}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              ServerPid = receive
                              {'doc_transport_error_server_ready', Pid} -> Pid
                          after 1000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              %% the lease owns the request: the hand-off is done, and the
              %% reader's own clauses are what will see the error
              'ok' = await_ref_owner(Ref, LeasePid),
              ServerPid ! 'close_now',
              receive
                  {'doc_transport_error_result', Caller, Result, Messages} ->
                      assert_transport_error_reason(Phase, Result),
                      ?assertEqual([], [M || M <- Messages,
                                             is_stray_transport_message(M)])
              after 1500 ->
                      exit(Caller, 'kill'),
                      ?assert('false')
              end,
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              ?assertEqual([], ets:lookup('hackney_manager_refs', Ref)),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end
      end).

%% The document door caps the cumulative body, not each piece: two pieces
%% each under the cap whose sum is over it are refused, as
%% `bounded_view_discards_rows_emitted_before_oversize_test' pins for the
%% view door's separate counter. hackney hands the reader one message per
%% socket read, so a large document arrives exactly this way.
bounded_open_doc_refuses_body_over_cap_across_pieces_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    FirstPiece = <<"{\"_id\":\"doc\",\"payload\":\"">>,
    SecondPiece = <<"xxxxxxxxxxxxxxxx\"}">>,
    MaxBytes = byte_size(FirstPiece) + 1,
    ?assert(byte_size(SecondPiece) =< MaxBytes),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_chunked_headers(Socket),
              'ok' = send_http_chunk(Socket, FirstPiece),
              timer:sleep(10),
              'ok' = send_http_chunk(Socket, SecondPiece),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertEqual(
                 {'error', 'response_too_large'},
                 couchbeam:open_doc_bounded(
                   Db, <<"doc">>, [], {1000, MaxBytes})),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end,
              assert_no_stray_transport_messages()
      end).

%% `hackney_bstr:to_binary/1' renders an atom in Latin-1: an atom whose name
%% has code points above 255 is refused before the budget, as a key and as a
%% value alike.
bounded_open_doc_refuses_non_latin1_atom_param_key_test() ->
    Key = list_to_atom([1000]),
    assert_refused_before_transport(
      {'error', {'invalid_param', {Key, <<"x">>}}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{Key, <<"x">>}], {1000, 1024})
      end).

bounded_open_doc_refuses_non_latin1_atom_param_value_test() ->
    Value = list_to_atom([1000]),
    assert_refused_before_transport(
      {'error', {'invalid_param', {'rev', Value}}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'rev', Value}], {1000, 1024})
      end).

%% An improper list is refused before the budget — with its tail on the
%% document door, as a whole on the view door and as `invalid_document' for
%% a property list — instead of crashing in `lists:dropwhile/2',
%% `parse_view_options/1' or `lists:keyfind/3'.
bounded_open_doc_refuses_improper_param_list_test() ->
    assert_refused_before_transport(
      {'error', {'invalid_param', 'junk'}},
      fun(Db) ->
              couchbeam:open_doc_bounded(
                Db, <<"doc">>, [{'rev', <<"1-a">>} | 'junk'], {1000, 1024})
      end).

bounded_fetch_refuses_improper_option_list_test() ->
    ensure_couchbeam_supervisor(),
    Options = [{'limit', 1} | 'junk'],
    assert_refused_before_transport(
      {'error', {'invalid_param', Options}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, 'all_docs', Options, {1000, 1024})
      end).

bounded_save_doc_refuses_improper_props_test() ->
    assert_refused_before_transport(
      {'error', 'invalid_document'},
      fun(Db) ->
              couchbeam:save_doc_bounded(
                Db, {[{<<"value">>, 1} | 'junk']}, [], {1000, 1024})
      end).

%% The view name is validated before the budget: a half `hackney_url' cannot
%% render, or an atom other than `all_docs', would otherwise fail after the
%% budget clock started (`make_view/4').
bounded_fetch_refuses_float_view_name_part_test() ->
    ensure_couchbeam_supervisor(),
    assert_refused_before_transport(
      {'error', {'invalid_view_name', {1.5, <<"v">>}}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, {1.5, <<"v">>}, [], {1000, 1024})
      end).

bounded_fetch_refuses_bare_atom_view_name_test() ->
    ensure_couchbeam_supervisor(),
    assert_refused_before_transport(
      {'error', {'invalid_view_name', 'bogus'}},
      fun(Db) ->
              couchbeam_view:fetch_bounded(Db, 'bogus', [], {1000, 1024})
      end).

%% A capture handler left behind by a scenario killed before its `after'
%% must not fail the next capture.
with_captured_warnings_tolerates_leftover_handler_test() ->
    HandlerId = 'couchbeam_bounded_warning_capture',
    _ = logger:remove_handler(HandlerId),
    'ok' = logger:add_handler(HandlerId, ?MODULE,
                              #{'level' => 'notice',
                                'config' => #{'pid' => self()}}),
    with_captured_warnings(fun() -> 'ok' end, fun(_Warnings) -> 'ok' end),
    ?assertMatch({'error', {'not_found', _}},
                 logger:get_handler_config(HandlerId)).

%% The bounded transport reaches into hackney's private protocol — the
%% `{Ref, {Owner, Stream, Info}}' row layout of `hackney_manager_refs',
%% `controlling_process' answered by linking the new owner, `sys:get_state/2'
%% as a cleanup barrier, the transport error reported to `stream_to' before
%% the manager forgets the ref, the manager closing the socket when the
%% tracked owner dies — verified by hand against 1.25.0 (this repository's
%% lock) and 1.20.1 (the version the consumer, kazoo_5027, pinned when this
%% line was cut: a snapshot of 2026-09-05, not maintained here). A move past
%% either version must fail here and send its author back to those
%% internals rather than trust the suite's silence.
hackney_version_is_verified_test() ->
    'ok' = case application:load('hackney') of
               'ok' -> 'ok';
               {'error', {'already_loaded', 'hackney'}} -> 'ok'
           end,
    {'ok', Vsn} = application:get_key('hackney', 'vsn'),
    ?assert(lists:member(Vsn, ["1.25.0", "1.20.1"])).

%% CouchDB answers 202 with a revision when fewer than the write quorum of
%% nodes acknowledged the write; the bounded write takes it like a 201.
bounded_save_doc_accepts_202_with_rev_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"ok\":true,\"id\":\"doc\",\"rev\":\"1-a\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 202, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              {'ok', Saved, Bytes} = couchbeam:save_doc_bounded(
                                       Db, {[{<<"_id">>, <<"doc">>}]}, [],
                                       {1000, 1024}),
              ?assertEqual(byte_size(Body), Bytes),
              ?assertEqual(<<"doc">>, couchbeam_doc:get_id(Saved)),
              ?assertEqual(<<"1-a">>, couchbeam_doc:get_rev(Saved)),
              assert_no_stray_transport_messages()
      end).

%% The view door answers a non-2xx other than 404 in the official shape,
%% `{http_error, Status, Reason}', without reading the body
%% (`couchbeam_view_stream:do_init_stream/2'); Kazoo receives it from
%% `kz_couch_view:fetch_results_bounded/4'. Pinned as the current contract;
%% whether the doors should agree on one shape is recorded as deferred.
bounded_view_reports_401_as_http_error_test() ->
    assert_view_http_error_status(401).

bounded_view_reports_500_as_http_error_test() ->
    assert_view_http_error_status(500).

%% --- round 10 regressions --------------------------------------------------

%% Official `db_resp/2' answers an empty `Expect' with the response itself,
%% whatever the status. `db_request_bounded/7' is exported, so a caller of the
%% primitive sees the divergence directly: without the twin clause every
%% status, 200 included, would come back as `bad_response'.
db_request_bounded_accepts_any_status_with_empty_expect_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"db_name\":\"db\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 500, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
              {'ok', Status, _Headers, Ref} =
                  couchbeam_httpc:db_request_bounded(
                    'get', <<BaseUrl/binary, "/db">>, [], <<>>,
                    [{'no_proxy_env', 'true'}], [], Budget),
              ?assertEqual(500, Status),
              ?assertEqual(
                 {'ok', {[{<<"db_name">>, <<"db">>}]}, byte_size(Body)},
                 couchbeam_httpc:bounded_json_body(Ref, Budget)),
              assert_no_stray_transport_messages()
      end).

%% Clause order, official verbatim: the status mapping stands above the empty
%% `Expect', so a 404 is `not_found' with any `Expect' at all.
db_request_bounded_still_maps_404_with_empty_expect_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, 404, 0),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
              ?assertEqual(
                 {'error', 'not_found'},
                 couchbeam_httpc:db_request_bounded(
                   'get', <<BaseUrl/binary, "/db">>, [], <<>>,
                   [{'no_proxy_env', 'true'}], [], Budget)),
              assert_no_stray_transport_messages()
      end).

%% `hackney_version_is_verified_test' pins the version string; this one pins
%% the two facts the cleanup proof actually rides on, so a patch release that
%% keeps the version and moves either of them is caught by the suite instead
%% of by a Kazoo node.
hackney_manager_internals_are_verified_test_() ->
    {'timeout', 30, fun hackney_manager_internals_are_verified/0}.

hackney_manager_internals_are_verified() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    Body = <<"{\"db_name\":\"db\"}">>,
    Parent = self(),
    with_http_server(
      fun(Socket, ServerParent) ->
              ServerParent ! {'internals_request_ready', self()},
              receive 'send_internals_response' -> 'ok' end,
              'ok' = send_json_headers(Socket, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              ServerParent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl,
                         [{'no_proxy_env', 'true'},
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Caller = spawn(
                         fun() ->
                                 Result = couchbeam:db_info_bounded(
                                            Db, {2000, 1024}),
                                 Parent ! {'internals_result', self(), Result}
                         end),
              {WorkerPid, LeasePid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, _Guardian,
                       Caller} ->
                          {Worker, Lease}
                  after 1000 ->
                          exit(Caller, 'kill'),
                          ?assert('false')
                  end,
              ServerPid = receive
                              {'internals_request_ready', Pid} -> Pid
                          after 1000 ->
                                  exit(Caller, 'kill'),
                                  ?assert('false')
                          end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
              'ok' = await_ref_owner(Ref, LeasePid),
              %% (1) the row layout `await_owned_transport_cleanup/3' matches
              ?assertMatch([{Ref, {LeasePid, _Stream, _Info}}],
                           ets:lookup('hackney_manager_refs', Ref)),
              %% (2) `{controlling_process, ...}' links the owner it is handed,
              %%     which is what makes the manager clean up after a killed
              %%     lease
              {'links', Links} = process_info(LeasePid, 'links'),
              ?assert(lists:member(whereis('hackney_manager'), Links)),
              ServerPid ! 'send_internals_response',
              receive
                  {'internals_result', Caller, Result} ->
                      ?assertEqual(
                         {'ok', {[{<<"db_name">>, <<"db">>}]}, byte_size(Body)},
                         Result)
              after 2000 ->
                      ?assert('false')
              end
      end).

%% `bounded_encode_json/2' is exported and pinned by the export test, but the
%% delegation body itself ran on no success path: the only caller of the arity
%% handed it a malformed budget and landed in the arity-3 catch-all.
bounded_encode_json_arity_two_encodes_with_default_options_test() ->
    {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
    ?assertEqual({'ok', <<"{\"a\":1}">>},
                 couchbeam_httpc:bounded_encode_json({[{<<"a">>, 1}]}, Budget)).

%% A scenario killed by its eunit timeout skips its own `after'; the resume of
%% the shared `hackney_manager' must not depend on it.
with_suspended_manager_resumes_after_owner_kill_test_() ->
    {'timeout', 30, fun with_suspended_manager_resumes_after_owner_kill/0}.

with_suspended_manager_resumes_after_owner_kill() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    Parent = self(),
    Caller = spawn(
               fun() ->
                       Watcher = suspend_manager(),
                       Parent ! {'manager_suspended', self(), Watcher},
                       receive 'never' -> 'ok' end
               end),
    Watcher = receive
                  {'manager_suspended', Caller, WatcherPid} -> WatcherPid
              after 1000 ->
                      ?assert('false')
              end,
    ?assertEqual('suspended', manager_sys_state()),
    %% The third internal `couchbeam_httpc' names: a suspended manager still
    %% answers `sys:get_state/2'. `manager_cleanup_barrier/1' is built on it,
    %% and a hackney release that stopped answering would hang that barrier
    %% instead of failing here.
    ?assert(is_tuple(sys:get_state('hackney_manager', 1000))),
    exit(Caller, 'kill'),
    try
        ?assertEqual(
           'running',
           await_condition(
             fun() ->
                     case manager_sys_state() of
                         'running' -> {'true', 'running'};
                         'suspended' -> 'false'
                     end
             end, 2000))
    after
        %% This is the one scenario whose failure would poison every later
        %% one, so it resumes the manager itself rather than leave the proof
        %% of the watcher's job undone for the rest of the run.
        _ = catch sys:resume('hackney_manager')
    end,
    assert_process_gone(Watcher).

%% The module header promises a loopback listener; the fixtures have to bind
%% one, not merely dial one.
fixture_listener_binds_loopback_only_test() ->
    {'ok', ListenSocket} = listen_loopback(),
    try
        ?assertMatch({'ok', {{127, 0, 0, 1}, _Port}},
                     inet:sockname(ListenSocket))
    after
        gen_tcp:close(ListenSocket)
    end.

assert_view_http_error_status(Status) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    ensure_couchbeam_supervisor(),
    drain_transport_messages(),
    Body = <<"{\"error\":\"e\"}">>,
    with_http_server(
      fun(Socket, Parent) ->
              'ok' = send_json_headers(Socket, Status, byte_size(Body)),
              'ok' = gen_tcp:send(Socket, Body),
              Parent ! {'peer_close_result', recv_until_closed(Socket)},
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Result = couchbeam_view:fetch_bounded(
                         Db, 'all_docs', [], {1000, 1024}),
              ?assertMatch({'error', {'http_error', Status, _Reason}}, Result),
              ?assert(erlang:monotonic_time('millisecond') - Started < 500),
              receive
                  {'peer_close_result', PeerResult} ->
                      ?assertEqual({'error', 'closed'}, PeerResult)
              after 1500 ->
                      ?assert('false')
              end,
              assert_no_stray_transport_messages(),
              assert_no_lifecycle_messages()
      end).

assert_refused_before_transport(Expected, CallFun) ->
    assert_refused_before_transport(Expected, CallFun, []).

assert_refused_before_transport(Expected, CallFun, ExtraOptions) ->
    ?assertEqual(Expected, refused_before_transport(CallFun, ExtraOptions)).

%% Run `CallFun' against a listening socket nobody answers and hand back its
%% result; the call must have refused before connecting (no `accept') and
%% left no transport message behind.
refused_before_transport(CallFun, ExtraOptions) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try
        Server = couchbeam:server_connection(
                   BaseUrl, [{'no_proxy_env', 'true'} | ExtraOptions]),
        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
        Result = CallFun(Db),
        ?assertEqual({'error', 'timeout'}, gen_tcp:accept(ListenSocket, 20)),
        assert_no_stray_transport_messages(),
        Result
    after
        gen_tcp:close(ListenSocket)
    end.

assert_bounded_request_line(RequestLine, Body, CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(RequestLine, Request),
              send_json_response(Socket, Body),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              ?assertMatch({'ok', _, _}, CallFun(Db)),
              assert_success_left_nothing_behind()
      end).

%% A successful call leaves nothing behind either: no late transport message,
%% no guardian lifecycle message, no process of the bounded transport.
assert_success_left_nothing_behind() ->
    assert_no_stray_transport_messages(),
    assert_no_lifecycle_messages(),
    assert_no_bounded_processes_left().

%% Guardian, lease, upload worker, their guards, the cleanup relay, the
%% decoder and, on the view door, the stream: none may outlive a successful
%% call. Settled rather than sampled at once — the guardian answers its
%% acknowledgement before it exits.
assert_no_bounded_processes_left() ->
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case bounded_transport_processes() of
                     [] -> {'true', 'true'};
                     _ -> 'false'
                 end
         end, 1000)).

bounded_transport_processes() ->
    processes_in('couchbeam_httpc', "") ++
        processes_in('couchbeam_view_stream', "").

%% Read the request body announced by Content-Length; `Request' already holds
%% the head and possibly the first body bytes.
recv_request_body(Socket, Request) ->
    [Head, Rest] = binary:split(Request, <<"\r\n\r\n">>),
    ContentLength = request_content_length(Head),
    recv_exact(Socket, Rest, ContentLength).

request_content_length(Head) ->
    Lines = binary:split(Head, <<"\r\n">>, ['global']),
    [Length] = [binary_to_integer(string:trim(Value))
                || Line <- Lines,
                   [Name, Value] <- [binary:split(Line, <<":">>)],
                   string:lowercase(string:trim(Name)) =:= <<"content-length">>],
    Length.

recv_exact(_Socket, Acc, Length) when byte_size(Acc) >= Length ->
    <<Body:Length/binary, _/binary>> = Acc,
    {'ok', Body};
recv_exact(Socket, Acc, Length) ->
    case gen_tcp:recv(Socket, 0, 1000) of
        {'ok', Chunk} -> recv_exact(Socket, <<Acc/binary, Chunk/binary>>, Length);
        Error -> Error
    end.

%% No transport-tagged message may remain in the API caller's mailbox after a
%% bounded call returned: no worker/guardian token tuples, no monitor DOWN,
%% no late hackney messages. The snapshot is taken without a delay, which is
%% sound only when every producer is known dead before the check — a message
%% from a dead local process is already in the queue. Where a producer is
%% still alive when the call returns (the stream on the cancel path with a
%% dead guardian), the scenario reads the mailbox after a grace period.
%% Call this before the code under test: the whole suite shares one eunit
%% process, so a leftover row or `done' from an earlier test would otherwise
%% fail an unrelated later one. It empties the mailbox completely — there is
%% nothing a test legitimately holds across this point.
drain_transport_messages() ->
    receive
        _Message -> drain_transport_messages()
    after 0 ->
            'ok'
    end.

%% Every fixture listener is bound to the loopback address explicitly. The
%% clients only ever dial `127.0.0.1', so a listener on `INADDR_ANY' is not
%% observable from inside the suite -- it is observable from the network of
%% whoever runs it, for as long as the scenario lasts.
listen_loopback() ->
    gen_tcp:listen(0, ['binary', {'active', 'false'}, {'reuseaddr', 'true'},
                       {'ip', {127, 0, 0, 1}}]).

%% `sys:suspend/1' parks a process the whole suite shares, and eunit kills a
%% test that overruns its timeout with an untrappable signal
%% (`eunit_proc:kill_task/2'), which skips every `after' block. A resume that
%% lives only in the scenario's `after' would therefore leave `hackney_manager'
%% suspended for the rest of the run, and each following scenario would die on
%% its own timeout with nothing pointing at the cause. The watcher below is
%% monitored on the scenario process and resumes on any death, including that
%% kill; the scenario still resumes on its normal path, and a `resume' of a
%% running process is a no-op (`sys:do_cmd/6'), so the double call is safe.
suspend_manager() ->
    Caller = self(),
    'ok' = sys:suspend('hackney_manager'),
    spawn(fun() -> resume_manager_on_owner_death(Caller) end).

resume_manager(Watcher) ->
    'ok' = sys:resume('hackney_manager'),
    Watcher ! 'released',
    'ok'.

resume_manager_on_owner_death(Caller) ->
    MonitorRef = erlang:monitor('process', Caller),
    receive
        'released' ->
            erlang:demonitor(MonitorRef, ['flush']),
            'ok';
        {'DOWN', MonitorRef, 'process', Caller, _Reason} ->
            _ = catch sys:resume('hackney_manager'),
            'ok'
    end.

%% `running' | `suspended' -- the only observable that tells the two apart:
%% `sys:get_state/2' answers in both states.
manager_sys_state() ->
    {'status', _Pid, {'module', _Mod}, [_PDict, SysState | _Rest]} =
        sys:get_status('hackney_manager', 1000),
    SysState.

assert_no_stray_transport_messages() ->
    {'messages', Messages} = process_info(self(), 'messages'),
    ?assertEqual([], [M || M <- Messages, is_stray_transport_message(M)]).

is_stray_transport_message({'DOWN', _, 'process', _, _}) -> 'true';
is_stray_transport_message({'hackney_response', _, _}) -> 'true';
is_stray_transport_message(Message) when is_tuple(Message),
                                         tuple_size(Message) >= 2 ->
    is_reference(element(1, Message));
is_stray_transport_message(_Message) -> 'false'.

assert_no_lifecycle_messages() ->
    {'messages', Messages} = process_info(self(), 'messages'),
    ?assertEqual([], lifecycle_messages(Messages)).

lifecycle_messages(Messages) ->
    [M || {Tag, _, _}=M <- Messages,
          Tag =:= 'bounded_transport_guardian' orelse
              Tag =:= 'bounded_transport_cleanup_started' orelse
              Tag =:= 'bounded_transport_cleanup'].

%% processes currently inside couchbeam_httpc:decode_json_result/1
json_decoder_processes() ->
    [Pid || Pid <- erlang:processes(),
            Pid =/= self(),
            is_json_decoder(process_info(Pid, 'current_stacktrace'))].

is_json_decoder({'current_stacktrace', Frames}) ->
    lists:any(fun({'couchbeam_httpc', 'decode_json_result', 1, _}) -> 'true';
                 (_) -> 'false'
              end, Frames);
is_json_decoder(_Other) -> 'false'.

%% The process may still be logging or notifying when a hook message it
%% sent arrives; its exit is a condition to wait for, not a fact to assert.
assert_process_gone(Pid) ->
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case is_process_alive(Pid) of
                     'false' -> {'true', 'true'};
                     'true' -> 'false'
                 end
         end, 2000)).

%% Poll `Fun' until it answers `{true, Value}'; on timeout answer `false',
%% which every caller asserts against.
await_condition(Fun, TimeoutMs) ->
    await_condition_until(
      Fun, erlang:monotonic_time('millisecond') + TimeoutMs).

await_condition_until(Fun, DeadlineMs) ->
    case Fun() of
        {'true', Value} -> Value;
        'false' ->
            case erlang:monotonic_time('millisecond') < DeadlineMs of
                'true' ->
                    timer:sleep(5),
                    await_condition_until(Fun, DeadlineMs);
                'false' ->
                    'false'
            end
    end.

%% --- eunit timeout budget gate ---------------------------------------------

%% eunit runs every `*_test/0' under `?DEFAULT_TEST_TIMEOUT' -- 5000 ms
%% (`eunit-2.9.1/src/eunit_internal.hrl', applied by `eunit_proc:handle_test/2')
%% -- and enforces it with `exit(Pid, kill)' (`eunit_proc:kill_task/2'), which
%% skips every `after' block the scenario relies on, `resume_manager/1'
%% included. A scenario whose own waits can outlast that belongs in a
%% `{timeout, N, ...}' generator, like the long ones already are. This gate
%% computes each test's worst-case wait budget from this module's own abstract
%% code -- its `receive ... after <literal>' and `timer:sleep(<literal>)' plus
%% the budgets of every function it calls -- so the rule is computed on every
%% run instead of remembered by whoever adds the next scenario.
-define(EUNIT_DEFAULT_TEST_TIMEOUT_MS, 5000).

test_wait_budgets_stay_under_eunit_default_test() ->
    Over = [{Name, Budget}
            || {Name, Budget} <- test_wait_budgets(),
               Budget >= ?EUNIT_DEFAULT_TEST_TIMEOUT_MS],
    ?assertEqual([], Over).

%% The model is itself a claim, and a rule dropped from it makes the gate
%% quietly weaker rather than red -- so `wait_budget_model_counts_call_timeouts_test'
%% pins one number per rule below.
wait_budget_model_counts_call_timeouts_test() ->
    %% `gen_tcp:recv(Socket, 0, 1000)' -- a timeout as a call argument
    ?assertEqual(1000, wait_budget_of({'recv_request', 2})),
    %% a literal handed to a waiting helper of this module
    ?assertEqual(1000, wait_budget_of({'recv_until_closed', 1})),
    %% `await_condition(Fun, 2000)' plus the helper's own `timer:sleep(5)'
    ?assertEqual(2005, wait_budget_of({'assert_process_gone', 1})),
    %% `erlang:monotonic_time('millisecond') + 1000' -- a deadline, not a wait
    ?assertEqual(1000, wait_budget_of({'owned_request_ref', 1})).

test_wait_budgets() ->
    {Own, Calls, Forms} = wait_budget_model(),
    [{Name, wait_budget({Name, 0}, Own, Calls, [])}
     || {'function', _Line, Name, 0, _Clauses} <- Forms,
        lists:suffix("_test", atom_to_list(Name))].

wait_budget_of(Key) ->
    {Own, Calls, _Forms} = wait_budget_model(),
    wait_budget(Key, Own, Calls, []).

wait_budget_model() ->
    Forms = module_function_forms(),
    {maps:from_list([{function_key(F), own_wait_budget(F)} || F <- Forms]),
     maps:from_list([{function_key(F), called_functions(F)} || F <- Forms]),
     Forms}.

module_function_forms() ->
    {'ok', {_Module, [{'abstract_code', {_Vsn, Forms}}]}} =
        beam_lib:chunks(code:which(?MODULE), ['abstract_code']),
    [Form || {'function', _, _, _, _}=Form <- Forms].

function_key({'function', _Line, Name, Arity, _Clauses}) -> {Name, Arity}.

wait_budget(Key, Own, Calls, Seen) ->
    case lists:member(Key, Seen) of
        'true' -> 0;
        'false' ->
            maps:get(Key, Own, 0)
                + lists:sum([wait_budget(Called, Own, Calls, [Key | Seen])
                             || Called <- maps:get(Key, Calls, [])])
    end.

own_wait_budget(Form) ->
    fold_abstract(fun node_wait_budget/2, 0, Form).

node_wait_budget({'receive', _, _Clauses, {'integer', _, Timeout}, _After}, Acc) ->
    Acc + Timeout;
node_wait_budget({'call', _, {'remote', _, {'atom', _, 'timer'},
                             {'atom', _, 'sleep'}},
                  [{'integer', _, Ms}]}, Acc) ->
    Acc + Ms;
%% A timeout handed to a call is a wait too. Without these the model scored a
%% scenario built on `gen_tcp:recv/3' or `await_condition/2' at zero and the
%% gate guaranteed less than it claimed (round 11, blind-hunter and
%% verification-gap independently). The lists are explicit rather than
%% inferred: a new waiting helper has to be added here, and the self-check
%% test above fails the moment a rule is dropped.
node_wait_budget({'call', _, {'remote', _, {'atom', _, Module},
                              {'atom', _, Function}}, Args}, Acc) ->
    Acc + argument_timeout(
            waiting_remote_call(Module, Function, length(Args)), Args);
node_wait_budget({'call', _, {'atom', _, Function}, Args}, Acc) ->
    Acc + argument_timeout(waiting_local_call(Function, length(Args)), Args);
%% `erlang:monotonic_time(_) + N' is a deadline, and the loop it bounds waits
%% up to N (`owned_request_ref/1', `await_ref_owner/2').
node_wait_budget({'op', _, '+',
                  {'call', _, {'remote', _, {'atom', _, 'erlang'},
                               {'atom', _, 'monotonic_time'}}, _Unit},
                  {'integer', _, Ms}}, Acc) ->
    Acc + Ms;
node_wait_budget(_Node, Acc) ->
    Acc.

waiting_remote_call('gen_tcp', 'recv', 3) -> 3;
waiting_remote_call('gen_tcp', 'accept', 2) -> 2;
waiting_remote_call('sys', 'get_status', 2) -> 2;
waiting_remote_call('sys', 'get_state', 2) -> 2;
waiting_remote_call(_Module, _Function, _Arity) -> 'undefined'.

waiting_local_call('await_condition', 2) -> 2;
waiting_local_call('recv_until_closed', 2) -> 2;
waiting_local_call(_Function, _Arity) -> 'undefined'.

argument_timeout('undefined', _Args) ->
    0;
argument_timeout(Position, Args) ->
    case lists:nth(Position, Args) of
        {'integer', _, Ms} -> Ms;
        _Expression -> 0
    end.

called_functions(Form) ->
    lists:usort(
      fold_abstract(
        fun({'call', _, {'atom', _, Name}, Args}, Acc) ->
                [{Name, length(Args)} | Acc];
           ({'fun', _, {'function', Name, Arity}}, Acc) ->
                [{Name, Arity} | Acc];
           (_Node, Acc) ->
                Acc
        end, [], Form)).

fold_abstract(Fun, Acc0, Node) when is_tuple(Node) ->
    lists:foldl(fun(Element, Acc) -> fold_abstract(Fun, Acc, Element) end,
                Fun(Node, Acc0), tuple_to_list(Node));
fold_abstract(Fun, Acc0, Nodes) when is_list(Nodes) ->
    lists:foldl(fun(Element, Acc) -> fold_abstract(Fun, Acc, Element) end,
                Acc0, Nodes);
fold_abstract(_Fun, Acc, _Node) ->
    Acc.

%% --- logger capture -------------------------------------------------------

with_captured_warnings(ScenarioFun, AssertFun) ->
    HandlerId = 'couchbeam_bounded_warning_capture',
    %% A capture killed by its `_test_' timeout before the `after' below ran
    %% leaves the handler installed; the next capture must not fail on
    %% `{already_exist, _}' in a scenario that has nothing to do with it.
    _ = logger:remove_handler(HandlerId),
    'ok' = logger:add_handler(HandlerId, ?MODULE,
                              #{'level' => 'notice',
                                'config' => #{'pid' => self()}}),
    try
        ScenarioFun(),
        timer:sleep(50),
        AssertFun(captured_warnings([]))
    after
        _ = logger:remove_handler(HandlerId)
    end.

captured_warnings(Acc) ->
    receive
        {'captured_warning', Msg} -> captured_warnings([Msg | Acc])
    after 0 ->
            lists:reverse(Acc)
    end.

cleanup_unproven_warnings(Warnings) ->
    [W || W <- Warnings, is_cleanup_unproven_warning(W)].

%% The guardian logs a structured report, so the match is on a stable key
%% rather than on formatted text.
is_cleanup_unproven_warning(
  {'report', #{'event' := 'couchbeam_bounded_cleanup_unproven'}}) ->
    'true';
is_cleanup_unproven_warning(_Other) ->
    'false'.

cleanup_recovered_notices(Events) ->
    [E || {'report', #{'event' := 'couchbeam_bounded_cleanup_recovered'}}=E
              <- Events].

%% logger handler callback
log(#{'level' := Level, 'msg' := Msg}, #{'config' := #{'pid' := Pid}})
  when Level =:= 'warning'; Level =:= 'notice' ->
    Pid ! {'captured_warning', Msg},
    'ok';
log(_LogEvent, _Config) ->
    'ok'.

%% The server reads the request head and nothing else. On this platform that
%% does not block the upload worker inside `hackney:request/5' — the inet
%% driver queues the body and hackney answers `{ok, Ref}' — so what these
%% scenarios pin is the deadline verdict and the transport cleanup on a
%% request the server never answers, not a worker stuck in a send; their
%% names say so.
assert_unanswered_request_deadline(CallFun) ->
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
                          {'bounded_upload_context_test_hook', Parent}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
              Started = erlang:monotonic_time('millisecond'),
              Caller = spawn(
                         fun() ->
                                 Parent ! {'upload_result', self(),
                                           CallFun(Db)}
                         end),
              %% the fifth element is the caller on the document door and
              %% the stream on the view door; both doors come through here
              {WorkerPid, LeasePid} =
                  receive
                      {'bounded_upload_context', Worker, Lease, _Guardian,
                       _Parent} ->
                          {Worker, Lease}
                  after 1000 ->
                          ?assert('false')
                  end,
              receive
                  {'upload_backpressured', _ServerPid} -> 'ok'
              after 1000 ->
                      ?assert('false')
              end,
              Ref = owned_request_ref([WorkerPid, LeasePid]),
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
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    BaseUrl = iolist_to_binary(
                [<<"http://127.0.0.1:">>, integer_to_binary(Port)]),
    try
        Server = couchbeam:server_connection(
                   BaseUrl,
                   [{'no_proxy_env', 'true'},
                    {'bounded_encode_test_delay_ms', 1000}]),
        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
        Started = erlang:monotonic_time('millisecond'),
        ?assertEqual({'error', 'timeout'}, CallFun(Db)),
        Elapsed = erlang:monotonic_time('millisecond') - Started,
        ?assert(Elapsed < 500),
        ?assertEqual({'error', 'timeout'}, gen_tcp:accept(ListenSocket, 20))
    after
        gen_tcp:close(ListenSocket)
    end.

measured_send_capacity() ->
    {'ok', ListenSocket} = listen_loopback(),
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

%% The ref of the request one of `Owners' holds. hackney tracks a request
%% under its current owner — the upload worker until the hand-off, the lease
%% afterwards — and the hand-off follows `hackney:request/5' returning, not
%% anything the server does, so a lookup keyed on the worker alone races it:
%% round 8 lost that race in one scoped run out of three. Scenarios that
%% know the lease name both; the ones that hold the guardian before
%% `guardian_started' (no hand-off until they continue it) may name the
%% worker alone.
owned_request_ref(Owner) when is_pid(Owner) ->
    owned_request_ref([Owner]);
owned_request_ref(Owners) ->
    owned_request_ref(
      Owners, erlang:monotonic_time('millisecond') + 1000).

owned_request_ref(Owners, DeadlineMs) ->
    Rows = lists:append(
             [ets:match_object('hackney_manager_refs', {'_', {Owner, '_', '_'}})
              || Owner <- Owners]),
    case Rows of
        [{Ref, _} | _] -> Ref;
        [] ->
            case erlang:monotonic_time('millisecond') < DeadlineMs of
                'true' ->
                    erlang:yield(),
                    owned_request_ref(Owners, DeadlineMs);
                'false' ->
                    ?assert('false')
            end
    end.

await_ref_owner(Ref, OwnerPid) ->
    await_ref_owner(
      Ref, OwnerPid, erlang:monotonic_time('millisecond') + 1000).

await_ref_owner(Ref, OwnerPid, DeadlineMs) ->
    case ets:lookup('hackney_manager_refs', Ref) of
        [{Ref, {OwnerPid, _, _}}] -> 'ok';
        _ ->
            case erlang:monotonic_time('millisecond') < DeadlineMs of
                'true' ->
                    erlang:yield(),
                    await_ref_owner(Ref, OwnerPid, DeadlineMs);
                'false' ->
                    ?assert('false')
            end
    end.

has_cleanup_ack(Messages) ->
    lists:any(
      fun({'bounded_guardian_cleanup_ack', _, _, _}) -> 'true';
         ({_, 'guardian_cleanup_result', _}) -> 'true';
         (_) -> 'false'
      end, Messages).

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
              Started = erlang:monotonic_time('millisecond'),
              {Caller, Ref} = start_captured_view_call(
                                fun() ->
                                        couchbeam_view:fetch_bounded(
                                          Db, 'all_docs', [], {1000, 1024})
                                end,
                                fun(HeldRef) ->
                                        hold_handoff_until_transport_gone(
                                          Phase, HeldRef)
                                end),
              receive
                  {'captured_view_result', Caller, Ref, Result, RefAbsent} ->
                      %% hackney's transport reason, verbatim and at once —
                      %% not the deadline's `timeout' a second later
                      assert_transport_error_reason(Phase, Result),
                      ?assert(erlang:monotonic_time('millisecond') - Started
                              < 500),
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

%% What hackney 1.25.0 reports when the peer closes: a bare `closed' while
%% the status line or the headers are awaited, `{closed, Buffer}' once the
%% body is streamed. Shared by the view door and the document doors.
assert_transport_error_reason('status', Result) ->
    ?assertEqual({'error', 'closed'}, Result);
assert_transport_error_reason('headers', Result) ->
    ?assertEqual({'error', 'closed'}, Result);
assert_transport_error_reason('body', Result) ->
    ?assertMatch({'error', {'closed', _}}, Result).

%% In the `status' phase the peer closed before any status line, so hackney
%% reports the error and the manager forgets the ref while the stream is
%% still held before its hand-off: `controlling_process' then answers
%% `badarg', and the verdict must still be the transport reason
%% (`couchbeam_httpc:transport_verdict_before_handoff/2'). Waiting for the
%% row to vanish makes that order the one under test rather than a race with
%% the hand-off-first order, which the `body' phase covers.
hold_handoff_until_transport_gone('status', Ref) ->
    ?assertEqual(
       'true',
       await_condition(
         fun() ->
                 case ets:lookup('hackney_manager_refs', Ref) of
                     [] -> {'true', 'true'};
                     _ -> 'false'
                 end
         end, 1000));
hold_handoff_until_transport_gone('body', _Ref) ->
    'ok'.

start_captured_view_call(CallFun) ->
    start_captured_view_call(CallFun, fun(_Ref) -> 'ok' end).

start_captured_view_call(CallFun, BeforeContinue) ->
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
            BeforeContinue(Ref),
            StreamPid ! {'bounded_handoff_continue', Ref},
            {Caller, Ref}
    after 1000 ->
            exit(Caller, 'kill'),
            ?assert('false')
    end.

maybe_send_transport_error_headers(_Socket, 'status') ->
    'ok';
maybe_send_transport_error_headers(Socket, 'headers') ->
    %% the status line and one header, never the blank line that ends the
    %% head: hackney reports the status at once and then waits for the rest
    gen_tcp:send(
      Socket, <<"HTTP/1.1 200 Result\r\nContent-Type: application/json\r\n">>);
maybe_send_transport_error_headers(Socket, 'body') ->
    send_json_headers(Socket, 32).

assert_bounded_kazoo_headers(RequestLine, Body, CallFun) ->
    {'ok', _} = application:ensure_all_started('hackney'),
    drain_transport_messages(),
    with_kazoo_context(
      fun() ->
              with_http_request_server(
                fun(Socket, Request, Parent) ->
                        assert_request_line(RequestLine, Request),
                        assert_request_header(
                          <<"X-Kazoo-Application">>, <<"bounded_header_test">>,
                          Request),
                        assert_request_header(
                          <<"X-Kazoo-Log-ID">>, <<"bounded-call-id">>,
                          Request),
                        send_json_response(Socket, Body),
                        Parent ! {'server_done', self()}
                end,
                fun(BaseUrl) ->
                        Server = couchbeam:server_connection(
                                   BaseUrl, [{'no_proxy_env', 'true'}]),
                        {'ok', Db} = couchbeam:open_db(Server, <<"db">>),
                        ?assertMatch({'ok', _, _}, CallFun(Db)),
                        assert_success_left_nothing_behind()
                end)
      end).

assert_exports(Module, Expected) ->
    Exports = Module:module_info('exports'),
    lists:foreach(fun(Export) -> ?assert(lists:member(Export, Exports)) end,
                  Expected).

dead_pid() ->
    Pid = spawn(fun() -> 'ok' end),
    MonitorRef = erlang:monitor('process', Pid),
    receive
        {'DOWN', MonitorRef, 'process', Pid, _Reason} -> Pid
    end.

await_stream_entry(Table, Ref) ->
    await_stream_entry(
      Table, Ref, erlang:monotonic_time('millisecond') + 1000).

await_stream_entry(Table, Ref, DeadlineMs) ->
    case ets:lookup(Table, Ref) of
        [_]=Entry -> Entry;
        [] ->
            case erlang:monotonic_time('millisecond') < DeadlineMs of
                'true' ->
                    erlang:yield(),
                    await_stream_entry(Table, Ref, DeadlineMs);
                'false' ->
                    []
            end
    end.

with_kazoo_context(Fun) ->
    PreviousApplication = erlang:get('kz_application'),
    PreviousCallId = erlang:get('callid'),
    erlang:put('kz_application', 'bounded_header_test'),
    kz_log:put_callid(<<"bounded-call-id">>),
    try Fun()
    after
        restore_process_value('kz_application', PreviousApplication),
        restore_process_value('callid', PreviousCallId)
    end.

restore_process_value(Key, 'undefined') ->
    erlang:erase(Key),
    'ok';
restore_process_value(Key, Value) ->
    erlang:put(Key, Value),
    'ok'.

assert_request_line(Expected, Request) ->
    case binary:match(Request, <<Expected/binary, "\r\n">>) of
        'nomatch' -> erlang:error({'unexpected_request', Expected, Request});
        _ -> 'ok'
    end.

assert_request_header(Name, Value, Request) ->
    Expected = <<Name/binary, ": ", Value/binary>>,
    ?assertNotEqual(
       'nomatch',
       binary:match(string:lowercase(Request), string:lowercase(Expected))).

with_http_server(ServerFun, ClientFun) ->
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    Parent = self(),
    ServerPid = spawn(
                  fun() ->
                          case gen_tcp:accept(ListenSocket) of
                              {'ok', Socket} ->
                                  case recv_request(Socket, <<>>) of
                                      {'ok', _Request} ->
                                          run_server_fun(
                                            Parent,
                                            fun() ->
                                                    ServerFun(Socket, Parent)
                                            end);
                                      _ ->
                                          'ok'
                                  end,
                                  _ = gen_tcp:shutdown(Socket, 'write'),
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
    end,
    assert_server_did_not_fail().

with_http_request_server(ServerFun, ClientFun) ->
    {'ok', ListenSocket} = listen_loopback(),
    {'ok', {_Address, Port}} = inet:sockname(ListenSocket),
    Parent = self(),
    ServerPid = spawn(
                  fun() ->
                          case gen_tcp:accept(ListenSocket) of
                              {'ok', Socket} ->
                                  case recv_request(Socket, <<>>) of
                                      {'ok', Request} ->
                                          run_server_fun(
                                            Parent,
                                            fun() ->
                                                    ServerFun(Socket, Request,
                                                              Parent)
                                            end);
                                      _ ->
                                          'ok'
                                  end,
                                  _ = gen_tcp:shutdown(Socket, 'write'),
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
    end,
    assert_server_did_not_fail().

%% An assertion raised inside the spawned server process would otherwise be
%% invisible: the client would only see a closed connection and report some
%% unrelated timeout. Ship the failure to the eunit process instead.
run_server_fun(Parent, Fun) ->
    try Fun()
    catch
        Class:Reason:Stacktrace ->
            Parent ! {'server_failed', Class, Reason, Stacktrace},
            'ok'
    end.

assert_server_did_not_fail() ->
    receive
        {'server_failed', Class, Reason, Stacktrace} ->
            erlang:raise(Class, Reason, Stacktrace)
    after 0 ->
            'ok'
    end.

recv_request(Socket, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        'nomatch' ->
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
       <<"\r\nConnection: close\r\n\r\n">>]).

send_chunked_headers(Socket) ->
    gen_tcp:send(
      Socket,
      <<"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n">>).

send_http_chunk(Socket, Chunk) ->
    Size = integer_to_binary(byte_size(Chunk), 16),
    gen_tcp:send(Socket, [Size, <<"\r\n">>, Chunk, <<"\r\n">>]).

collect_legacy_stream(Ref, Acc) ->
    receive
        {Ref, 'done'} ->
            {'ok', lists:reverse(Acc)};
        {Ref, {'row', Row}} ->
            collect_legacy_stream(Ref, [Row | Acc]);
        {Ref, {'error', Error}} ->
            {'error', Error}
    after 15000 ->
            {'error', 'test_timeout'}
    end.

recv_until_closed(Socket) ->
    recv_until_closed(Socket, 1000).

recv_until_closed(Socket, Timeout) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
        {'ok', _Data} -> recv_until_closed(Socket, Timeout);
        Error -> Error
    end.

%% The supervisor must not be linked to the eunit process: a view stream
%% crash escalating past the restart intensity would take the whole run down
%% silently. Start it under the application controller instead.
ensure_couchbeam_supervisor() ->
    {'ok', _Started} = application:ensure_all_started('couchbeam'),
    'ok'.

assert_no_late_view_message() ->
    receive
        {Ref, Message} when is_reference(Ref) ->
            ?assertEqual('no_late_view_message', Message)
    after 100 ->
            'ok'
    end.

discard_messages() ->
    receive
        _ -> discard_messages()
    end.

%% Cookie options must be renderable before a worker or budget starts.
bounded_cookie_option_shapes_test_() ->
    Bad = [[{'secure', 'false'}], [{'http_only', 'false'}],
           [{'secure', 1}], [{'http_only', <<"true">>}],
           [{'max_age', -1}], [{'max_age', 1.5}], [{'max_age', <<"1">>}],
           [{'domain', 'host'}], [{'path', 999}], [{'path', {'v', []}}],
           [{'domain', 'x', 'y'}], [{'secure', 'true', 'extra'}],
           [{'domain', <<"host">>} | 'improper'],
           [{'path', <<"/">>} | 'improper']],
    Good = [[{'secure', 'true'}, {'http_only', 'true'}],
            [{'max_age', 0}], [{'max_age', 123}],
            [{'domain', "host"}, {'path', <<"/p">>}]],
    [?_assertEqual({'unsafe', <<"Cookie">>}, couchbeam_httpc:unsafe_cookie(
                    [{'cookie', {<<"a">>, <<"1">>, Opts}}])) || Opts <- Bad]
        ++ [?_assertEqual('safe', couchbeam_httpc:unsafe_cookie(
                           [{'cookie', {<<"a">>, <<"1">>, Opts}}]))
            || Opts <- Good].

bounded_header_scalar_depth_test_() ->
    [?_assertEqual({'unsafe', <<"X-A">>},
                   couchbeam_httpc:unsafe_header([{<<"X-A">>, Value}]))
     || Value <- [{{'v', []}, []}, {'v', [{'k', {'v', []}}]},
                  {'v', [{{'k', []}, 'v'}]}]].

bounded_header_name_boundaries_test_() ->
    [?_assertEqual({'unsafe', Name},
                   couchbeam_httpc:unsafe_header([{Name, <<"v">>}]))
     || Name <- [<<>>, "", '', <<"X:A">>, <<"X A">>, "X\tA"]].

bounded_raw_path_control_bytes_test_() ->
    [?_test(assert_refused_before_transport(
              {'error', {'unsafe_url', <<"/db", C, "/_find">>}},
              fun(#db{server=Server, options=Opts}) ->
                      Url = <<(couchbeam_httpc:server_url(Server))/binary,
                              "/db", C, "/_find">>,
                      {'ok', Budget} = couchbeam_httpc:new_request_budget(
                                         {1000, 1024}),
                      couchbeam_httpc:db_request_bounded(
                        'get', Url, [], <<>>, Opts, [200], Budget)
              end))
     || C <- [0, 11, 12, 31, 127]].

bounded_doc_id_segments_test_() ->
    Ids = [<<".">>, <<"..">>, <<"x/../y">>, <<"x/./y">>, <<"_design/">>,
           <<"%2e">>, <<"%2E%2E">>, <<"x/%2e%2e/y">>,
           <<"_design%2F">>, ".", "..", "x/%2E/y"],
    [?_test(assert_refused_before_transport(
              {'error', 'missing_doc_id'},
              fun(Db) -> couchbeam:open_doc_bounded(
                           Db, Id, [], {1000, 1024}) end)) || Id <- Ids]
        ++ [?_test(assert_refused_before_transport(
                     {'error', 'missing_doc_id'},
                     fun(Db) -> couchbeam:save_doc_bounded(
                                  Db, {[{<<"_id">>, Id}]}, [],
                                  {1000, 1024}) end)) || Id <- Ids].

bounded_proxy_option_is_refused_test() ->
    assert_refused_before_transport(
      {'error', {'unsupported_option', 'proxy'}},
      fun(#db{server=Server, options=Opts}) ->
              Url = hackney_url:make_url(couchbeam_httpc:server_url(Server),
                                        [<<"db">>], []),
              {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
              couchbeam_httpc:db_request_bounded(
                'get', Url, [], <<>>, [{'proxy', <<"http://proxy">>} | Opts],
                [200], Budget)
      end).

legacy_db_info_encodes_path_space_as_plus_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(<<"GET /db+x HTTP/1.1">>, Request),
              send_json_response(Socket, <<"{}">>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db x">>),
              ?assertMatch({'ok', _}, couchbeam:db_info(Db))
      end).

bounded_db_name_latin1_binary_wire_test() ->
    {'ok', _} = application:ensure_all_started('hackney'),
    with_http_request_server(
      fun(Socket, Request, Parent) ->
              assert_request_line(<<"GET /db%E9 HTTP/1.1">>, Request),
              send_json_response(Socket, <<"{}">>),
              Parent ! {'server_done', self()}
      end,
      fun(BaseUrl) ->
              Server = couchbeam:server_connection(
                         BaseUrl, [{'no_proxy_env', 'true'}]),
              {'ok', Db} = couchbeam:open_db(Server, <<"db", 233>>),
              ?assertMatch({'ok', _, _},
                           couchbeam:db_info_bounded(Db, {1000, 1024}))
      end).


bounded_transport_extra_header_sources_test_() ->
    [?_test(assert_refused_before_transport(
       {'error', Expected},
       fun(#db{server=Server, options=Opts}) ->
           Url = <<(couchbeam_httpc:server_url(Server))/binary, "/db">>,
           {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
           couchbeam_httpc:db_request_bounded(
             'get', Url, Headers, Body, Extra ++ Opts, [200], Budget)
       end))
     || {Headers, Body, Extra, Expected} <-
        [{[], <<>>, [{'path_encode_fun', fun(Path) -> Path end}],
          {'unsupported_option', 'path_encode_fun'}},
         {[], {'stream_multipart', 0, <<"bad\r\nboundary">>}, [],
          {'unsafe_header', <<"Content-Type">>}},
         {[{<<"X-A">>, <<"v">>} | 'improper'], <<>>, [],
          {'unsafe_header', 'undefined'}}]].

bounded_header_control_bytes_test_() ->
    [?_assertEqual({'unsafe', <<"X-A">>},
                   couchbeam_httpc:unsafe_header([{<<"X-A">>, <<C>>}]))
     || C <- [0, 1, 11, 12, 31, 127]]
        ++ [?_assertEqual({'unsafe', <<"X", C>>},
                          couchbeam_httpc:unsafe_header(
                            [{<<"X", C>>, <<"v">>}]))
            || C <- [0, 1, 31, 127, $(, $), $/, $=, 233]]
        ++ [?_assertEqual('safe', couchbeam_httpc:unsafe_header(
                                    [{<<"X-A">>, <<"a\tb", 233>>}]))].

bounded_address_control_bytes_test_() ->
    [?_assertNot(couchbeam_httpc:addressable(<<"db", C>>))
     || C <- [0, 1, 11, 12, 31, 127]].

bounded_cookie_and_parameter_headers_wire_test_() ->
    [?_test(begin
        {'ok', _} = application:ensure_all_started('hackney'),
        with_http_request_server(
          fun(Socket, Request, Parent) ->
              assert_request_header(<<"X-Params">>, <<"v;k=42;bare">>, Request),
              lists:foreach(fun(Value) ->
                  ?assertNotEqual('nomatch', binary:match(Request, Value))
              end, Expected),
              send_json_response(Socket, <<"{}">>),
              Parent ! {'server_done', self()}
          end,
          fun(BaseUrl) ->
              {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
              {'ok', 200, _, Ref} = couchbeam_httpc:db_request_bounded(
                'get', <<BaseUrl/binary, "/db">>,
                [{<<"X-Params">>, {'v', [{'k', 42}, 'bare']}}], <<>>,
                [{'cookie', Cookie}, {'no_proxy_env', 'true'}], [200], Budget),
              ?assertMatch({'ok', _, _},
                           couchbeam_httpc:bounded_json_body(Ref, Budget)),
              assert_success_left_nothing_behind()
          end)
    end)
     || {Cookie, Expected} <-
        [{{<<"a">>, <<"1">>}, [<<"a=1; Version=1">>]},
         {{<<"a">>, <<"1">>, [{'domain', "host"}, {'path', <<"/p">>},
                                {'secure', 'true'}, {'http_only', 'true'}]},
          [<<"a=1; Version=1; Domain=host; Path=/p; Secure; HttpOnly">>]},
         {[<<"raw=2">>, {<<"a">>, <<"1">>},
            {<<"b">>, <<"3">>, [{'path', "/"}]}],
          [<<"raw=2">>, <<"a=1; Version=1">>, <<"b=3; Version=1; Path=/">>]}]].

bounded_direct_request_url_refusal_test_() ->
    [?_test(assert_refused_before_transport(
      {'error', {'unsafe_url', <<"/db">>}},
      fun(#db{server=Server, options=Opts}) ->
          Url = <<(couchbeam_httpc:server_url(Server))/binary, "/db?q=x y">>,
          {'ok', Budget} = couchbeam_httpc:new_request_budget({1000, 1024}),
          case Arity of
              6 -> couchbeam_httpc:request_bounded(
                     'get', Url, [], <<>>, Opts, Budget);
              7 -> couchbeam_httpc:request_bounded(
                     'get', Url, [], <<>>, Opts, Budget, self())
          end
      end)) || Arity <- [6, 7]].


bounded_atom_header_controls_test() ->
    Name = <<"X-A">>,
    Value = list_to_atom([$a, 1]),
    ?assertEqual({'unsafe', Name},
                 couchbeam_httpc:unsafe_header([{Name, Value}])).

bounded_cookie_control_values_test_() ->
    [?_assertEqual({'unsafe', <<"Cookie">>},
                   couchbeam_httpc:unsafe_cookie([{'cookie', Cookie}]))
     || Cookie <- [<<"a=", 0>>, {<<"a">>, <<"1">>, [{'domain', <<"h", 0>>}]},
                   {<<"a">>, <<"1">>, [{'path', <<"/", 127>>}]}]].
