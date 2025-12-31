%% @doc Integration tests for pturso with the Rust binary.
-module(pturso_integration_test).

-include_lib("eunit/include/eunit.hrl").

-define(BINARY_PATH, "rust/target/debug/erso").

%%====================================================================
%% Test fixtures
%%====================================================================

start_server() ->
    {ok, Pid} = pturso_port:start_link(?BINARY_PATH),
    Pid.

stop_server(Pid) ->
    pturso_port:stop(Pid).

%%====================================================================
%% Tests
%%====================================================================

basic_select_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"SELECT 1 returns correct value",
           fun() ->
               Resp = pturso_port:call(Pid, {select, <<":memory:">>, <<"SELECT 1">>, []}),
               ?assertMatch({rows_result, {ok, [[{integer, 1}]]}}, Resp)
           end}
         ]
     end}.

create_table_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"CREATE TABLE succeeds",
           fun() ->
               Resp = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)">>, []}),
               ?assertMatch({updated, {ok, _}}, Resp)
           end}
         ]
     end}.

insert_and_select_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"INSERT and SELECT roundtrip",
           fun() ->
               %% Create table
               {updated, {ok, _}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"CREATE TABLE users (id INTEGER, name TEXT)">>, []}),

               %% Insert a row
               {updated, {ok, 1}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"INSERT INTO users VALUES (?, ?)">>,
                   [{integer, 42}, {text, <<"Alice">>}]}),

               %% Select it back
               Resp = pturso_port:call(Pid, {select, <<":memory:">>,
                   <<"SELECT id, name FROM users">>, []}),
               ?assertMatch({rows_result, {ok, [[{integer, 42}, {text, <<"Alice">>}]]}}, Resp)
           end}
         ]
     end}.

null_values_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"NULL values work correctly",
           fun() ->
               {updated, {ok, _}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"CREATE TABLE nullable (val TEXT)">>, []}),

               {updated, {ok, 1}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"INSERT INTO nullable VALUES (?)">>, [null]}),

               Resp = pturso_port:call(Pid, {select, <<":memory:">>,
                   <<"SELECT val FROM nullable">>, []}),
               ?assertMatch({rows_result, {ok, [[null]]}}, Resp)
           end}
         ]
     end}.

real_values_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"REAL values work correctly",
           fun() ->
               {updated, {ok, _}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"CREATE TABLE floats (val REAL)">>, []}),

               {updated, {ok, 1}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"INSERT INTO floats VALUES (?)">>, [{real, 3.14159}]}),

               {rows_result, {ok, [[{real, Val}]]}} = pturso_port:call(Pid, {select, <<":memory:">>,
                   <<"SELECT val FROM floats">>, []}),
               ?assert(abs(Val - 3.14159) < 0.00001)
           end}
         ]
     end}.

blob_values_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"BLOB values work correctly",
           fun() ->
               {updated, {ok, _}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"CREATE TABLE blobs (data BLOB)">>, []}),

               BlobData = <<0, 1, 2, 255, 128>>,
               {updated, {ok, 1}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"INSERT INTO blobs VALUES (?)">>, [{blob, BlobData}]}),

               Resp = pturso_port:call(Pid, {select, <<":memory:">>,
                   <<"SELECT data FROM blobs">>, []}),
               ?assertMatch({rows_result, {ok, [[{blob, BlobData}]]}}, Resp)
           end}
         ]
     end}.

error_handling_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"Invalid SQL returns error",
           fun() ->
               Resp = pturso_port:call(Pid, {select, <<":memory:">>,
                   <<"SELECT * FROM nonexistent_table">>, []}),
               ?assertMatch({rows_result, {error, _}}, Resp)
           end}
         ]
     end}.

multiple_rows_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(Pid) ->
         [
          {"Multiple rows work correctly",
           fun() ->
               {updated, {ok, _}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                   <<"CREATE TABLE items (id INTEGER)">>, []}),

               lists:foreach(fun(I) ->
                   {updated, {ok, _}} = pturso_port:call(Pid, {insert, <<":memory:">>,
                       <<"INSERT INTO items VALUES (?)">>, [{integer, I}]})
               end, [1, 2, 3]),

               {rows_result, {ok, Rows}} = pturso_port:call(Pid, {select, <<":memory:">>,
                   <<"SELECT id FROM items ORDER BY id">>, []}),
               ?assertEqual([[{integer, 1}], [{integer, 2}], [{integer, 3}]], Rows)
           end}
         ]
     end}.
