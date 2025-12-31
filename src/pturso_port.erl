%% @doc Port management for communication with the Rust Turso backend.
%% Handles spawning the external process, framing messages, and tracking request IDs.
%%
%% Message format (from Rust port.rs):
%% - 4-byte big-endian length prefix (includes request ID + payload)
%% - 4-byte big-endian request ID
%% - payload (bincode-serialized data)
-module(pturso_port).

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    start_link/2,
    stop/1,
    call/2,
    call/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {
    port :: port(),
    next_id :: non_neg_integer(),
    pending :: #{non_neg_integer() => {pid(), reference()}}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(string()) -> {ok, pid()} | {error, term()}.
start_link(BinaryPath) ->
    start_link(BinaryPath, []).

-spec start_link(string(), list()) -> {ok, pid()} | {error, term()}.
start_link(BinaryPath, Options) ->
    gen_server:start_link(?MODULE, {BinaryPath, Options}, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

-spec call(pid(), pturso_bincode:request()) -> pturso_bincode:response().
call(Pid, Request) ->
    call(Pid, Request, 5000).

-spec call(pid(), pturso_bincode:request(), timeout()) -> pturso_bincode:response().
call(Pid, Request, Timeout) ->
    gen_server:call(Pid, {call, Request}, Timeout).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({BinaryPath, _Options}) ->
    Port = open_port(
        {spawn_executable, BinaryPath},
        [binary, {packet, 4}, exit_status, use_stdio]
    ),
    {ok, #state{
        port = Port,
        next_id = 1,
        pending = #{}
    }}.

handle_call({call, Request}, From, State) ->
    #state{port = Port, next_id = Id, pending = Pending} = State,

    %% Encode the request
    Payload = pturso_bincode:encode_request(Request),

    %% Build message: request ID (4 bytes big-endian) + payload
    Msg = <<Id:32/big-unsigned, Payload/binary>>,

    %% Send to port ({packet, 4} handles length framing)
    port_command(Port, Msg),

    %% Track pending request
    NewPending = Pending#{Id => From},
    NewState = State#state{
        next_id = Id + 1,
        pending = NewPending
    },
    {noreply, NewState}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Port, {data, Data}}, #state{port = Port, pending = Pending} = State) ->
    %% Parse response: 4-byte request ID + payload
    <<Id:32/big-unsigned, Payload/binary>> = Data,

    %% Decode the response
    Response = pturso_bincode:decode_response(Payload),

    %% Find and reply to the waiting caller
    case maps:take(Id, Pending) of
        {From, NewPending} ->
            gen_server:reply(From, Response),
            {noreply, State#state{pending = NewPending}};
        error ->
            %% Unknown response ID - shouldn't happen
            error_logger:warning_msg("Received response for unknown request ID ~p~n", [Id]),
            {noreply, State}
    end;

handle_info({Port, {exit_status, Status}}, #state{port = Port} = State) ->
    %% Port process exited
    {stop, {port_exit, Status}, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{port = Port}) ->
    catch port_close(Port),
    ok.
