%% @doc Bincode encoder/decoder for Rust interop.
%% Uses legacy FixedInt encoding (compatible with wincode defaults):
%% - Enum discriminant: u32 little-endian
%% - Lengths: u64 little-endian
%% - Integers: fixed-size little-endian
-module(pturso_bincode).

-export([
    %% Primitive encoding
    encode_u32/1,
    encode_u64/1,
    encode_i64/1,
    encode_f64/1,
    encode_string/1,
    encode_bytes/1,
    encode_vec/2,

    %% Primitive decoding
    decode_u32/1,
    decode_u64/1,
    decode_i64/1,
    decode_f64/1,
    decode_string/1,
    decode_bytes/1,
    decode_vec/2,

    %% Value encoding/decoding
    encode_value/1,
    decode_value/1,

    %% Request encoding
    encode_request/1,

    %% Response decoding
    decode_response/1
]).

%%====================================================================
%% Primitive Encoding (little-endian)
%%====================================================================

-spec encode_u32(non_neg_integer()) -> binary().
encode_u32(N) when N >= 0, N < 16#100000000 ->
    <<N:32/little-unsigned>>.

-spec encode_u64(non_neg_integer()) -> binary().
encode_u64(N) when N >= 0 ->
    <<N:64/little-unsigned>>.

-spec encode_i64(integer()) -> binary().
encode_i64(N) ->
    <<N:64/little-signed>>.

-spec encode_f64(float()) -> binary().
encode_f64(F) ->
    <<F:64/little-float>>.

-spec encode_string(binary()) -> binary().
encode_string(Str) when is_binary(Str) ->
    Len = byte_size(Str),
    <<Len:64/little-unsigned, Str/binary>>.

-spec encode_bytes(binary()) -> binary().
encode_bytes(Bytes) when is_binary(Bytes) ->
    Len = byte_size(Bytes),
    <<Len:64/little-unsigned, Bytes/binary>>.

-spec encode_vec(fun((T) -> binary()), [T]) -> binary().
encode_vec(EncodeFn, List) ->
    Len = length(List),
    Encoded = [EncodeFn(Item) || Item <- List],
    iolist_to_binary([<<Len:64/little-unsigned>> | Encoded]).

%%====================================================================
%% Primitive Decoding (little-endian)
%%====================================================================

-spec decode_u32(binary()) -> {non_neg_integer(), binary()}.
decode_u32(<<N:32/little-unsigned, Rest/binary>>) ->
    {N, Rest}.

-spec decode_u64(binary()) -> {non_neg_integer(), binary()}.
decode_u64(<<N:64/little-unsigned, Rest/binary>>) ->
    {N, Rest}.

-spec decode_i64(binary()) -> {integer(), binary()}.
decode_i64(<<N:64/little-signed, Rest/binary>>) ->
    {N, Rest}.

-spec decode_f64(binary()) -> {float(), binary()}.
decode_f64(<<F:64/little-float, Rest/binary>>) ->
    {F, Rest}.

-spec decode_string(binary()) -> {binary(), binary()}.
decode_string(<<Len:64/little-unsigned, Rest/binary>>) ->
    <<Str:Len/binary, Rest2/binary>> = Rest,
    {Str, Rest2}.

-spec decode_bytes(binary()) -> {binary(), binary()}.
decode_bytes(Bin) ->
    decode_string(Bin).  % Same format

-spec decode_vec(fun((binary()) -> {T, binary()}), binary()) -> {[T], binary()}.
decode_vec(DecodeFn, <<Len:64/little-unsigned, Rest/binary>>) ->
    decode_vec_items(DecodeFn, Len, Rest, []).

decode_vec_items(_DecodeFn, 0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_vec_items(DecodeFn, N, Bin, Acc) ->
    {Item, Rest} = DecodeFn(Bin),
    decode_vec_items(DecodeFn, N - 1, Rest, [Item | Acc]).

%%====================================================================
%% Value Encoding/Decoding
%% Rust: enum Value { Null=0, Integer(i64)=1, Real(f64)=2, Text(String)=3, Blob(Vec<u8>)=4 }
%%====================================================================

-type value() :: null
               | {integer, integer()}
               | {real, float()}
               | {text, binary()}
               | {blob, binary()}.

-spec encode_value(value()) -> binary().
encode_value(null) ->
    encode_u32(0);
encode_value({integer, N}) ->
    <<(encode_u32(1))/binary, (encode_i64(N))/binary>>;
encode_value({real, F}) ->
    <<(encode_u32(2))/binary, (encode_f64(F))/binary>>;
encode_value({text, S}) ->
    <<(encode_u32(3))/binary, (encode_string(S))/binary>>;
encode_value({blob, B}) ->
    <<(encode_u32(4))/binary, (encode_bytes(B))/binary>>.

-spec decode_value(binary()) -> {value(), binary()}.
decode_value(<<0:32/little-unsigned, Rest/binary>>) ->
    {null, Rest};
decode_value(<<1:32/little-unsigned, N:64/little-signed, Rest/binary>>) ->
    {{integer, N}, Rest};
decode_value(<<2:32/little-unsigned, F:64/little-float, Rest/binary>>) ->
    {{real, F}, Rest};
decode_value(<<3:32/little-unsigned, Rest/binary>>) ->
    {Str, Rest2} = decode_string(Rest),
    {{text, Str}, Rest2};
decode_value(<<4:32/little-unsigned, Rest/binary>>) ->
    {Bytes, Rest2} = decode_bytes(Rest),
    {{blob, Bytes}, Rest2}.

%%====================================================================
%% Request Encoding
%% Rust: enum Requests { Crap=0, Select=1, Execute=2, Run=3 }
%%====================================================================

-type crap_request() :: {crap, binary()}.
-type select_request() :: {select, binary(), binary(), [value()]}.
-type insert_request() :: {insert, binary(), binary(), [value()]}.
-type run_request() :: {run, binary(), binary()}.
-type request() :: crap_request() | select_request() | insert_request() | run_request().

-spec encode_request(request()) -> binary().
encode_request({crap, Reason}) ->
    %% Requests::Crap(Crap { reason: String })
    <<(encode_u32(0))/binary, (encode_string(Reason))/binary>>;

encode_request({select, Db, Query, Params}) ->
    %% Requests::Select(Select { db, query, params })
    ParamsBin = encode_vec(fun encode_value/1, Params),
    <<(encode_u32(1))/binary,
      (encode_string(Db))/binary,
      (encode_string(Query))/binary,
      ParamsBin/binary>>;

encode_request({insert, Db, Query, Params}) ->
    %% Requests::Execute(Execute { db, query, params })
    ParamsBin = encode_vec(fun encode_value/1, Params),
    <<(encode_u32(2))/binary,
      (encode_string(Db))/binary,
      (encode_string(Query))/binary,
      ParamsBin/binary>>;

encode_request({run, Db, Sql}) ->
    %% Requests::Run(Run { db, sql })
    <<(encode_u32(3))/binary,
      (encode_string(Db))/binary,
      (encode_string(Sql))/binary>>.

%%====================================================================
%% Response Decoding
%% Rust: enum Responses { BadRequest=0, RowsResult=1, Updated=2, RunResult=3 }
%%====================================================================

-type bad_request_response() :: {bad_request, binary()}.
-type rows_result_response() :: {rows_result, {ok, [[value()]]} | {error, binary()}}.
-type updated_response() :: {updated, {ok, non_neg_integer()} | {error, binary()}}.
-type run_result_response() :: {run_result, ok | {error, binary()}}.
-type response() :: bad_request_response() | rows_result_response() | updated_response() | run_result_response().

-spec decode_response(binary()) -> response().
decode_response(<<0:32/little-unsigned, Rest/binary>>) ->
    %% BadRequest { reason: String }
    {Reason, <<>>} = decode_string(Rest),
    {bad_request, Reason};

decode_response(<<1:32/little-unsigned, Rest/binary>>) ->
    %% RowsResult: enum { Ok(Vec<Vec<Value>>)=0, Error(String)=1 }
    decode_rows_result(Rest);

decode_response(<<2:32/little-unsigned, Rest/binary>>) ->
    %% Updated: enum { Ok(u64)=0, Error(String)=1 }
    decode_updated(Rest);

decode_response(<<3:32/little-unsigned, Rest/binary>>) ->
    %% RunResult: enum { Ok=0, Error(String)=1 }
    decode_run_result(Rest).

decode_rows_result(<<0:32/little-unsigned, Rest/binary>>) ->
    %% RowsResult::Ok(Vec<Vec<Value>>)
    {Rows, <<>>} = decode_vec(fun decode_row/1, Rest),
    {rows_result, {ok, Rows}};
decode_rows_result(<<1:32/little-unsigned, Rest/binary>>) ->
    %% RowsResult::Error(String)
    {Reason, <<>>} = decode_string(Rest),
    {rows_result, {error, Reason}}.

decode_row(Bin) ->
    decode_vec(fun decode_value/1, Bin).

decode_updated(<<0:32/little-unsigned, N:64/little-unsigned, _Rest/binary>>) ->
    %% Updated::Ok(u64)
    {updated, {ok, N}};
decode_updated(<<1:32/little-unsigned, Rest/binary>>) ->
    %% Updated::Error(String)
    {Reason, <<>>} = decode_string(Rest),
    {updated, {error, Reason}}.

decode_run_result(<<0:32/little-unsigned, _Rest/binary>>) ->
    %% RunResult::Ok
    {run_result, ok};
decode_run_result(<<1:32/little-unsigned, Rest/binary>>) ->
    %% RunResult::Error(String)
    {Reason, <<>>} = decode_string(Rest),
    {run_result, {error, Reason}}.
