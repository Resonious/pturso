%% @doc Tests for bincode encoding/decoding.
-module(pturso_bincode_test).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Primitive encoding tests
%%====================================================================

encode_u32_test() ->
    ?assertEqual(<<0, 0, 0, 0>>, pturso_bincode:encode_u32(0)),
    ?assertEqual(<<1, 0, 0, 0>>, pturso_bincode:encode_u32(1)),
    ?assertEqual(<<255, 255, 255, 255>>, pturso_bincode:encode_u32(16#FFFFFFFF)).

encode_u64_test() ->
    ?assertEqual(<<0, 0, 0, 0, 0, 0, 0, 0>>, pturso_bincode:encode_u64(0)),
    ?assertEqual(<<1, 0, 0, 0, 0, 0, 0, 0>>, pturso_bincode:encode_u64(1)),
    ?assertEqual(<<255, 255, 255, 255, 0, 0, 0, 0>>, pturso_bincode:encode_u64(16#FFFFFFFF)).

encode_i64_test() ->
    ?assertEqual(<<0, 0, 0, 0, 0, 0, 0, 0>>, pturso_bincode:encode_i64(0)),
    ?assertEqual(<<1, 0, 0, 0, 0, 0, 0, 0>>, pturso_bincode:encode_i64(1)),
    %% -1 in two's complement
    ?assertEqual(<<255, 255, 255, 255, 255, 255, 255, 255>>, pturso_bincode:encode_i64(-1)).

encode_f64_test() ->
    %% 0.0 in IEEE 754
    ?assertEqual(<<0, 0, 0, 0, 0, 0, 0, 0>>, pturso_bincode:encode_f64(0.0)),
    %% 1.0 in IEEE 754 little-endian
    ?assertEqual(<<0, 0, 0, 0, 0, 0, 240, 63>>, pturso_bincode:encode_f64(1.0)).

encode_string_test() ->
    %% Empty string
    ?assertEqual(<<0, 0, 0, 0, 0, 0, 0, 0>>, pturso_bincode:encode_string(<<>>)),
    %% "hi"
    ?assertEqual(<<2, 0, 0, 0, 0, 0, 0, 0, $h, $i>>, pturso_bincode:encode_string(<<"hi">>)),
    %% UTF-8 test
    Hello = unicode:characters_to_binary("hello"),
    Expected = <<5, 0, 0, 0, 0, 0, 0, 0, "hello">>,
    ?assertEqual(Expected, pturso_bincode:encode_string(Hello)).

encode_vec_test() ->
    %% Empty vector
    ?assertEqual(<<0, 0, 0, 0, 0, 0, 0, 0>>,
                 pturso_bincode:encode_vec(fun pturso_bincode:encode_u32/1, [])),
    %% [1, 2, 3]
    ?assertEqual(<<3, 0, 0, 0, 0, 0, 0, 0,   % length = 3
                   1, 0, 0, 0,               % 1
                   2, 0, 0, 0,               % 2
                   3, 0, 0, 0>>,             % 3
                 pturso_bincode:encode_vec(fun pturso_bincode:encode_u32/1, [1, 2, 3])).

%%====================================================================
%% Primitive decoding tests
%%====================================================================

decode_u32_test() ->
    ?assertEqual({0, <<>>}, pturso_bincode:decode_u32(<<0, 0, 0, 0>>)),
    ?assertEqual({1, <<>>}, pturso_bincode:decode_u32(<<1, 0, 0, 0>>)),
    ?assertEqual({1, <<"rest">>}, pturso_bincode:decode_u32(<<1, 0, 0, 0, "rest">>)).

decode_u64_test() ->
    ?assertEqual({0, <<>>}, pturso_bincode:decode_u64(<<0, 0, 0, 0, 0, 0, 0, 0>>)),
    ?assertEqual({1, <<>>}, pturso_bincode:decode_u64(<<1, 0, 0, 0, 0, 0, 0, 0>>)).

decode_i64_test() ->
    ?assertEqual({0, <<>>}, pturso_bincode:decode_i64(<<0, 0, 0, 0, 0, 0, 0, 0>>)),
    ?assertEqual({-1, <<>>}, pturso_bincode:decode_i64(<<255, 255, 255, 255, 255, 255, 255, 255>>)).

decode_f64_test() ->
    {F, <<>>} = pturso_bincode:decode_f64(<<0, 0, 0, 0, 0, 0, 240, 63>>),
    ?assert(abs(F - 1.0) < 0.0001).

decode_string_test() ->
    ?assertEqual({<<>>, <<>>}, pturso_bincode:decode_string(<<0, 0, 0, 0, 0, 0, 0, 0>>)),
    ?assertEqual({<<"hi">>, <<>>}, pturso_bincode:decode_string(<<2, 0, 0, 0, 0, 0, 0, 0, $h, $i>>)).

decode_vec_test() ->
    ?assertEqual({[], <<>>},
                 pturso_bincode:decode_vec(fun pturso_bincode:decode_u32/1,
                                           <<0, 0, 0, 0, 0, 0, 0, 0>>)),
    ?assertEqual({[1, 2, 3], <<>>},
                 pturso_bincode:decode_vec(fun pturso_bincode:decode_u32/1,
                                           <<3, 0, 0, 0, 0, 0, 0, 0,
                                             1, 0, 0, 0,
                                             2, 0, 0, 0,
                                             3, 0, 0, 0>>)).

%%====================================================================
%% Value encoding/decoding tests
%%====================================================================

encode_value_null_test() ->
    ?assertEqual(<<0, 0, 0, 0>>, pturso_bincode:encode_value(null)).

encode_value_integer_test() ->
    ?assertEqual(<<1, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0>>,
                 pturso_bincode:encode_value({integer, 42})).

encode_value_real_test() ->
    Encoded = pturso_bincode:encode_value({real, 1.0}),
    ?assertEqual(<<2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63>>, Encoded).

encode_value_text_test() ->
    ?assertEqual(<<3, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, $h, $i>>,
                 pturso_bincode:encode_value({text, <<"hi">>})).

encode_value_blob_test() ->
    ?assertEqual(<<4, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3>>,
                 pturso_bincode:encode_value({blob, <<1, 2, 3>>})).

decode_value_null_test() ->
    ?assertEqual({null, <<>>}, pturso_bincode:decode_value(<<0, 0, 0, 0>>)).

decode_value_integer_test() ->
    ?assertEqual({{integer, 42}, <<>>},
                 pturso_bincode:decode_value(<<1, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0>>)).

decode_value_real_test() ->
    {{real, F}, <<>>} = pturso_bincode:decode_value(<<2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63>>),
    ?assert(abs(F - 1.0) < 0.0001).

decode_value_text_test() ->
    ?assertEqual({{text, <<"hi">>}, <<>>},
                 pturso_bincode:decode_value(<<3, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, $h, $i>>)).

decode_value_blob_test() ->
    ?assertEqual({{blob, <<1, 2, 3>>}, <<>>},
                 pturso_bincode:decode_value(<<4, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3>>)).

%%====================================================================
%% Roundtrip tests
%%====================================================================

roundtrip_value_test() ->
    Values = [null, {integer, 0}, {integer, -123}, {integer, 999999},
              {real, 0.0}, {real, 3.14159}, {real, -2.5},
              {text, <<>>}, {text, <<"hello world">>},
              {blob, <<>>}, {blob, <<0, 1, 255>>}],
    lists:foreach(fun(V) ->
        Encoded = pturso_bincode:encode_value(V),
        {Decoded, <<>>} = pturso_bincode:decode_value(Encoded),
        case V of
            {real, F} ->
                {real, F2} = Decoded,
                ?assert(abs(F - F2) < 0.00001);
            _ ->
                ?assertEqual(V, Decoded)
        end
    end, Values).

%%====================================================================
%% Request encoding tests
%%====================================================================

encode_request_crap_test() ->
    Encoded = pturso_bincode:encode_request({crap, <<"test error">>}),
    %% Tag 0 + string "test error"
    Expected = <<0, 0, 0, 0,                     % tag = 0 (Crap)
                 10, 0, 0, 0, 0, 0, 0, 0,        % string length = 10
                 "test error">>,
    ?assertEqual(Expected, Encoded).

encode_request_select_test() ->
    Encoded = pturso_bincode:encode_request({select, <<":memory:">>, <<"SELECT 1">>, []}),
    %% Tag 1 + db string + query string + empty params vec
    Expected = <<1, 0, 0, 0,                     % tag = 1 (Select)
                 8, 0, 0, 0, 0, 0, 0, 0,         % db length = 8
                 ":memory:",
                 8, 0, 0, 0, 0, 0, 0, 0,         % query length = 8
                 "SELECT 1",
                 0, 0, 0, 0, 0, 0, 0, 0>>,       % params length = 0
    ?assertEqual(Expected, Encoded).

encode_request_insert_test() ->
    Encoded = pturso_bincode:encode_request({insert, <<":memory:">>, <<"INSERT">>, [{integer, 1}]}),
    %% Tag 2 + db string + query string + params vec with one integer
    Expected = <<2, 0, 0, 0,                     % tag = 2 (Insert)
                 8, 0, 0, 0, 0, 0, 0, 0,         % db length = 8
                 ":memory:",
                 6, 0, 0, 0, 0, 0, 0, 0,         % query length = 6
                 "INSERT",
                 1, 0, 0, 0, 0, 0, 0, 0,         % params length = 1
                 1, 0, 0, 0,                     % value tag = 1 (Integer)
                 1, 0, 0, 0, 0, 0, 0, 0>>,       % i64 = 1
    ?assertEqual(Expected, Encoded).

%%====================================================================
%% Response decoding tests
%%====================================================================

decode_response_bad_request_test() ->
    Bin = <<0, 0, 0, 0,                     % tag = 0 (BadRequest)
            5, 0, 0, 0, 0, 0, 0, 0,         % string length = 5
            "error">>,
    ?assertEqual({bad_request, <<"error">>}, pturso_bincode:decode_response(Bin)).

decode_response_rows_result_ok_test() ->
    %% Empty result
    Bin = <<1, 0, 0, 0,                     % tag = 1 (RowsResult)
            0, 0, 0, 0,                     % variant = 0 (Ok)
            0, 0, 0, 0, 0, 0, 0, 0>>,       % vec length = 0
    ?assertEqual({rows_result, {ok, []}}, pturso_bincode:decode_response(Bin)).

decode_response_rows_result_with_data_test() ->
    %% One row with one integer value
    Bin = <<1, 0, 0, 0,                     % tag = 1 (RowsResult)
            0, 0, 0, 0,                     % variant = 0 (Ok)
            1, 0, 0, 0, 0, 0, 0, 0,         % outer vec length = 1
            1, 0, 0, 0, 0, 0, 0, 0,         % inner vec length = 1
            1, 0, 0, 0,                     % value tag = 1 (Integer)
            42, 0, 0, 0, 0, 0, 0, 0>>,      % i64 = 42
    ?assertEqual({rows_result, {ok, [[{integer, 42}]]}}, pturso_bincode:decode_response(Bin)).

decode_response_rows_result_error_test() ->
    Bin = <<1, 0, 0, 0,                     % tag = 1 (RowsResult)
            1, 0, 0, 0,                     % variant = 1 (Error)
            9, 0, 0, 0, 0, 0, 0, 0,         % string length = 9
            "not found">>,
    ?assertEqual({rows_result, {error, <<"not found">>}}, pturso_bincode:decode_response(Bin)).

decode_response_updated_ok_test() ->
    Bin = <<2, 0, 0, 0,                     % tag = 2 (Updated)
            0, 0, 0, 0,                     % variant = 0 (Ok)
            5, 0, 0, 0, 0, 0, 0, 0>>,       % u64 = 5
    ?assertEqual({updated, {ok, 5}}, pturso_bincode:decode_response(Bin)).

decode_response_updated_error_test() ->
    Bin = <<2, 0, 0, 0,                     % tag = 2 (Updated)
            1, 0, 0, 0,                     % variant = 1 (Error)
            6, 0, 0, 0, 0, 0, 0, 0,         % string length = 6
            "failed">>,
    ?assertEqual({updated, {error, <<"failed">>}}, pturso_bincode:decode_response(Bin)).
