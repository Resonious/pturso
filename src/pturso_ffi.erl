%% @doc FFI module bridging Gleam types to Erlang implementation.
-module(pturso_ffi).

-export([
    start/1,
    stop/1,
    select/4,
    execute/4
]).

%%====================================================================
%% API
%%====================================================================

-spec start(binary()) -> {ok, {connection, pid()}} | {error, binary()}.
start(BinaryPath) when is_binary(BinaryPath) ->
    case pturso_port:start_link(binary_to_list(BinaryPath)) of
        {ok, Pid} ->
            {ok, {connection, Pid}};
        {error, Reason} ->
            {error, list_to_binary(io_lib:format("~p", [Reason]))}
    end.

-spec stop({connection, pid()}) -> nil.
stop({connection, Pid}) ->
    pturso_port:stop(Pid),
    nil.

-spec select({connection, pid()}, binary(), binary(), list()) ->
    {ok, list()} | {error, binary()}.
select({connection, Pid}, Db, Query, Params) ->
    ErlParams = [gleam_param_to_erl(P) || P <- Params],
    Request = {select, Db, Query, ErlParams},
    case pturso_port:call(Pid, Request) of
        {rows_result, {ok, Rows}} ->
            DynamicRows = [[erl_value_to_dynamic(V) || V <- Row] || Row <- Rows],
            {ok, DynamicRows};
        {rows_result, {error, Reason}} ->
            {error, Reason};
        {bad_request, Reason} ->
            {error, Reason}
    end.

-spec execute({connection, pid()}, binary(), binary(), list()) ->
    {ok, integer()} | {error, binary()}.
execute({connection, Pid}, Db, Query, Params) ->
    ErlParams = [gleam_param_to_erl(P) || P <- Params],
    Request = {insert, Db, Query, ErlParams},
    case pturso_port:call(Pid, Request) of
        {updated, {ok, Count}} ->
            {ok, Count};
        {updated, {error, Reason}} ->
            {error, Reason};
        {bad_request, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% Convert Gleam Param to Erlang bincode value
gleam_param_to_erl(null) -> null;
gleam_param_to_erl({int, N}) -> {integer, N};
gleam_param_to_erl({float, F}) -> {real, F};
gleam_param_to_erl({string, S}) -> {text, S};
gleam_param_to_erl({blob, B}) -> {blob, B}.

%% Convert Erlang bincode value to raw Erlang term for Gleam Dynamic
erl_value_to_dynamic(null) -> nil;
erl_value_to_dynamic({integer, N}) -> N;
erl_value_to_dynamic({real, F}) -> F;
erl_value_to_dynamic({text, S}) -> S;
erl_value_to_dynamic({blob, B}) -> B.
