%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_httpc).

-export([request/5,
         db_request/5, db_request/6,
         json_body/1,
         bounded_json_body/2,
         new_request_budget/1,
         cancel_request/1,
         db_resp/2,
         make_headers/4,
         maybe_oauth_header/4]).
-export_type([request_budget_spec/0, request_budget/0]).
%% urls utils
-export([server_url/1, db_url/1, doc_url/2]).
%% atts utols
-export([reply_att/1, wait_mp_doc/2, len_doc_to_mp_stream/3, send_mp_doc/5]).

-include("couchbeam.hrl").

-type request_budget_spec() :: {pos_integer(), pos_integer()}.
-type request_budget() :: #{'deadline_ms' := integer(),
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
    Resp = request(Method, Url, Headers, Body, Options),
    case couchbeam_util:get_value('request_budget', Options) of
        #{'deadline_ms' := _, 'max_response_bytes' := _}=Budget ->
            db_resp_bounded(Resp, Expect, Budget);
        _ ->
            db_resp(Resp, Expect)
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
        {'ok', Body, Bytes} -> decode_bounded_json(Body, Bytes);
        {'error', _}=Error -> Error
    end;
bounded_json_body(Ref, _) ->
    close_request(Ref),
    {'error', 'invalid_request_budget'}.

-spec cancel_request(reference()) -> 'ok'.
cancel_request(Ref) ->
    close_request(Ref).

-spec bounded_binary_body(reference(), request_budget()) ->
          {'ok', binary(), non_neg_integer()} | {'error', term()}.
bounded_binary_body(Ref, Budget) ->
    bounded_body(Ref, Budget, <<>>, 0).

-spec bounded_body(reference(), request_budget(), binary(), non_neg_integer()) ->
          {'ok', binary(), non_neg_integer()} | {'error', term()}.
bounded_body(Ref, Budget, Acc, Bytes) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 ->
            _ = hackney:setopts(Ref, [{'recv_timeout', TimeoutMs}]),
            bounded_body_chunk(hackney:stream_body(Ref), Ref,
                               Budget, Acc, Bytes);
        _ ->
            close_request(Ref),
            {'error', 'timeout'}
    end.

-spec bounded_body_chunk(term(), reference(), request_budget(), binary(),
                         non_neg_integer()) ->
          {'ok', binary(), non_neg_integer()} | {'error', term()}.
bounded_body_chunk({'ok', Chunk}, Ref,
                   #{'max_response_bytes' := MaxBytes}=Budget, Acc, Bytes) ->
    NewBytes = Bytes + byte_size(Chunk),
    case NewBytes =< MaxBytes of
        'true' -> bounded_body(Ref, Budget, <<Acc/binary, Chunk/binary>>, NewBytes);
        'false' ->
            close_request(Ref),
            {'error', 'response_too_large'}
    end;
bounded_body_chunk('done', Ref, Budget, Acc, Bytes) ->
    case remaining_timeout(Budget) of
        TimeoutMs when TimeoutMs > 0 -> {'ok', Acc, Bytes};
        _ ->
            close_request(Ref),
            {'error', 'timeout'}
    end;
bounded_body_chunk({'error', 'timeout'}, Ref, _Budget, _Acc, _Bytes) ->
    close_request(Ref),
    {'error', 'timeout'};
bounded_body_chunk({'error', Reason}, Ref, _Budget, _Acc, _Bytes) ->
    close_request(Ref),
    {'error', Reason}.

-spec decode_bounded_json(binary(), non_neg_integer()) ->
          {'ok', term(), non_neg_integer()} | {'error', term()}.
decode_bounded_json(Body, Bytes) ->
    try couchbeam_ejson:decode(Body) of
        Json -> {'ok', Json, Bytes}
    catch
        'error':Reason -> {'error', {'invalid_json', Reason}}
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
    case catch hackney:cancel_request(Ref) of
        {'ok', {Transport, Socket, _Buffer, _ResponseState}}
          when Socket =/= 'nil' ->
            _ = catch Transport:close(Socket),
            'ok';
        _ ->
            _ = catch hackney:close(Ref),
            'ok'
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
