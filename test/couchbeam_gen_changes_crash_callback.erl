-module(couchbeam_gen_changes_crash_callback).

%% Declared so the compiler checks this module against
%% `gen_changes:behaviour_info(callbacks)': the six callbacks below are the
%% whole set, and signature drift in `gen_changes' has to surface here.
-behaviour(gen_changes).

-export([init/1,
         handle_change/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-spec init(pid()) -> {'ok', pid()}.
init(Parent) ->
    {'ok', Parent}.

-spec handle_change(term(), pid()) -> no_return().
handle_change(_Change, _Parent) ->
    erlang:error('intentional_callback_crash').

-spec handle_call(term(), term(), pid()) -> {'reply', term(), pid()}.
handle_call(Request, _From, Parent) ->
    {'reply', Request, Parent}.

-spec handle_cast(term(), pid()) -> {'noreply', pid()}.
handle_cast(_Message, Parent) ->
    {'noreply', Parent}.

-spec handle_info(term(), pid()) -> {'noreply', pid()}.
handle_info(_Message, Parent) ->
    {'noreply', Parent}.

-spec terminate(term(), pid()) -> 'ok'.
terminate(Reason, Parent) ->
    Parent ! {'gen_changes_terminated', Reason},
    'ok'.
