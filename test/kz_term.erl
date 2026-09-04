%%% Test stub of Kazoo's `kz_term', compiled in the test profile only: `test/'
%%% is not part of the dependency's production build, and Kazoo's vendored
%%% copy compiles `src/' alone (`deps/couchbeam/ebin' carries no test beam),
%%% so on a Kazoo code path the real module is what is loaded and this file
%%% must never be. It mirrors the one call `src/' makes, `to_binary/1' on the
%%% application name that becomes the `X-Kazoo-Application' header, for the
%%% shapes that name can take: a binary as is, an atom by its name, a string
%%% as characters, an integer in decimal.
-module(kz_term).

-export([to_binary/1]).

-spec to_binary(term()) -> binary().
to_binary(Value) when is_binary(Value) ->
    Value;
to_binary(Value) when is_atom(Value) ->
    atom_to_binary(Value, 'utf8');
to_binary(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value);
to_binary(Value) when is_integer(Value) ->
    integer_to_binary(Value).
