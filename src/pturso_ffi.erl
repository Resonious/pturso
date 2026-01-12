%% @doc FFI module bridging Gleam types to Erlang implementation.
-module(pturso_ffi).

-export([
    start/1,
    acquire_binary/0,
    stop/1,
    now_ms/0,
    select/4,
    execute/4,
    run/3
]).

-define(GITHUB_API_RELEASES, "https://api.github.com/repos/Resonious/pturso/releases").

-define(ETS_TABLE, pturso_ports).

%%====================================================================
%% API
%%====================================================================

-spec start(binary()) -> {ok, {connection, pid()}} | {error, binary()}.
start(BinaryPath) when is_binary(BinaryPath) ->
    ensure_ets_table(),
    case ets:lookup(?ETS_TABLE, BinaryPath) of
        [{_, Pid}] ->
            case is_process_alive(Pid) of
                true ->
                    {ok, {connection, Pid}};
                false ->
                    ets:delete(?ETS_TABLE, BinaryPath),
                    start_new_port(BinaryPath)
            end;
        [] ->
            start_new_port(BinaryPath)
    end.

-spec stop({connection, pid()}) -> nil.
stop({connection, Pid}) ->
    %% Remove from ETS if present
    ets:match_delete(?ETS_TABLE, {'_', Pid}),
    pturso_port:stop(Pid),
    nil.

-spec now_ms() -> integer().
now_ms() -> erlang:monotonic_time(millisecond).

-spec select({connection, pid()}, binary(), binary(), list()) ->
    {ok, list()} | {error, binary()}.
select({connection, Pid}, Db, Query, Params) ->
    ErlParams = [gleam_param_to_erl(P) || P <- Params],
    Request = {select, Db, Query, ErlParams},
    case pturso_port:call(Pid, Request) of
        {rows_result, {ok, Rows}} ->
            %% Return rows as tuples so decode.field(index, decoder) works
            DynamicRows = [list_to_tuple([erl_value_to_dynamic(V) || V <- Row]) || Row <- Rows],
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

-spec run({connection, pid()}, binary(), binary()) -> {ok, nil} | {error, binary()}.
run({connection, Pid}, Db, Sql) ->
    Request = {run, Db, Sql},
    case pturso_port:call(Pid, Request) of
        {run_result, ok} ->
            {ok, nil};
        {run_result, {error, Reason}} ->
            {error, Reason};
        {bad_request, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

ensure_ets_table() ->
    case ets:whereis(?ETS_TABLE) of
        undefined ->
            ets:new(?ETS_TABLE, [set, public, named_table]);
        _ ->
            ok
    end.

start_new_port(BinaryPath) ->
    case pturso_port:start_link(binary_to_list(BinaryPath)) of
        {ok, Pid} ->
            ets:insert(?ETS_TABLE, {BinaryPath, Pid}),
            {ok, {connection, Pid}};
        {error, Reason} ->
            {error, list_to_binary(io_lib:format("~p", [Reason]))}
    end.

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

%%====================================================================
%% Binary acquisition
%%====================================================================

%% @doc Acquire the erso binary by downloading from GitHub releases.
-spec acquire_binary() -> {ok, binary()} | {error, binary()}.
acquire_binary() ->
    CachePath = get_cache_path(),
    case filelib:is_regular(CachePath) of
        true ->
            %% Already downloaded
            {ok, list_to_binary(CachePath)};
        false ->
            case download_from_github(CachePath) of
                ok ->
                    {ok, list_to_binary(CachePath)};
                {error, Reason} ->
                    {error, list_to_binary(io_lib:format(
                        "Failed to download erso binary from GitHub: ~p",
                        [Reason]))}
            end
    end.

%% Get the cache path for downloaded binary
get_cache_path() ->
    CacheDir = get_cache_dir(),
    filename:join(CacheDir, "erso").

get_cache_dir() ->
    HomeDir = case os:getenv("HOME") of
        false -> "/tmp";
        H -> H
    end,
    CacheBase = case os:getenv("XDG_CACHE_HOME") of
        false -> filename:join(HomeDir, ".cache");
        X -> X
    end,
    CacheDir = filename:join(CacheBase, "pturso"),
    ok = filelib:ensure_dir(filename:join(CacheDir, "dummy")),
    CacheDir.

%% Download binary from GitHub releases
download_from_github(DestPath) ->
    ensure_http_started(),
    Platform = get_platform(),
    case get_latest_release_asset_url(Platform) of
        {ok, AssetUrl} ->
            download_asset(AssetUrl, DestPath);
        {error, _} = Err ->
            Err
    end.

ensure_http_started() ->
    case application:ensure_all_started(inets) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    case application:ensure_all_started(ssl) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok.

%% Get the download URL for the platform asset from the latest release
get_latest_release_asset_url(Platform) ->
    Headers = [{"User-Agent", "pturso-erlang"}],
    HttpOpts = [{autoredirect, true}],
    Opts = [{body_format, binary}],
    case httpc:request(get, {?GITHUB_API_RELEASES, Headers}, HttpOpts, Opts) of
        {ok, {{_, 200, _}, _, Body}} ->
            find_asset_url(Body, Platform);
        {ok, {{_, StatusCode, _}, _, _}} ->
            {error, {api_error, StatusCode}};
        {error, Reason} ->
            {error, Reason}
    end.

%% Parse the releases JSON and find the asset URL for the given platform
find_asset_url(JsonBody, Platform) ->
    %% Simple pattern matching to find the browser_download_url for our platform
    %% Looking for: "browser_download_url": "...erso-x86_64-linux..."
    PlatformBin = list_to_binary(Platform),
    Pattern = <<"\"browser_download_url\"\\s*:\\s*\"([^\"]*", PlatformBin/binary, "[^\"]*)\"">>,
    case re:run(JsonBody, Pattern, [{capture, [1], binary}]) of
        {match, [Url]} ->
            {ok, binary_to_list(Url)};
        nomatch ->
            {error, {asset_not_found, Platform}}
    end.

%% Download the asset from the given URL
download_asset(Url, DestPath) ->
    Headers = [{"User-Agent", "pturso-erlang"}],
    HttpOpts = [{autoredirect, true}, {relaxed, true}],
    Opts = [{body_format, binary}],
    case httpc:request(get, {Url, Headers}, HttpOpts, Opts) of
        {ok, {{_, 200, _}, _Headers, Body}} ->
            ok = file:write_file(DestPath, Body),
            ok = file:change_mode(DestPath, 8#755),
            ok;
        {ok, {{_, StatusCode, _}, _, _}} ->
            {error, {download_error, StatusCode}};
        {error, Reason} ->
            {error, Reason}
    end.

%% Detect current platform for GitHub release artifact name
get_platform() ->
    Arch = erlang:system_info(system_architecture),
    case Arch of
        "x86_64" ++ _ -> "erso-x86_64-linux";
        "aarch64" ++ _ -> "erso-aarch64-linux";
        "arm64" ++ _ -> "erso-aarch64-linux";
        _ -> "erso-x86_64-linux"  %% Default fallback
    end.
