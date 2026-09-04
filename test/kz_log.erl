%%% Test stub of Kazoo's `kz_log', compiled in the test profile only: `test/'
%%% is not part of the dependency's production build, and Kazoo's vendored
%%% copy compiles `src/' alone (`deps/couchbeam/ebin' carries no test beam),
%%% so on a Kazoo code path the real module is what is loaded and this file
%%% must never be. It mirrors the two calls `src/' makes and their Kazoo
%%% semantics: `get_callid/0' answers the call id stored in the process
%%% dictionary under `callid', or Kazoo's default `<<"00000000000">>' when
%%% none is set; `put_callid/1' stores it and answers it.
-module(kz_log).

-export([get_callid/0, put_callid/1]).

-spec get_callid() -> binary().
get_callid() ->
    case erlang:get('callid') of
        'undefined' -> <<"00000000000">>;
        CallId -> CallId
    end.

-spec put_callid(binary()) -> binary().
put_callid(CallId) ->
    erlang:put('callid', CallId),
    CallId.
