import gleam/list
import gleam/result.{try}
import gleam/dynamic/decode
import gleam/dynamic.{type Dynamic}

/// Parameter values for queries (needed for bincode encoding)
pub type Param {
  Null
  Int(Int)
  Float(Float)
  String(String)
  Blob(BitArray)
}

pub type LogEntry {
  LogEntry(sql: String, duration_ms: Int)
}

pub type Port

pub type Connection {
  Connection(
    port: Port,
    db: String,
    logger_fn: fn(LogEntry) -> Nil,
  )
}

pub type Error {
  DatabaseError(message: String)
  DecodeError(errors: List(decode.DecodeError))
}

@external(erlang, "pturso_ffi", "now_ms")
fn now_ms() -> Int

@external(erlang, "pturso_ffi", "acquire_binary")
fn acquire_binary() -> Result(String, String)

@external(erlang, "pturso_ffi", "start")
pub fn start_with_binary(binary_path: String) -> Result(Port, String)

/// Start the erso binary, automatically acquiring it if needed.
/// First tries to build with cargo (if available), then downloads from GitHub releases.
pub fn start() -> Result(Port, String) {
  use binary_path <- try(acquire_binary())
  start_with_binary(binary_path)
}

@external(erlang, "pturso_ffi", "stop")
pub fn stop(conn: Port) -> Nil

/// Raw select function for queries that return results.
/// Considered "low level" as it takes in Port and DB separately
/// and is implemented in Erlang.
@external(erlang, "pturso_ffi", "select")
pub fn select(
  conn: Port,
  db: String,
  query: String,
  params: List(Param),
) -> Result(List(Dynamic), String)

/// Raw execute function for queries that do not return anything.
/// Considered "low level" as it takes in Port and DB separately
/// and is implemented in Erlang.
@external(erlang, "pturso_ffi", "execute")
pub fn execute(
  conn: Port,
  db: String,
  query: String,
  params: List(Param),
) -> Result(Int, String)

/// Run one or more SQL statements without using prepared statements.
/// Supports multiple statements separated by semicolons.
/// Returns nothing on success.
@external(erlang, "pturso_ffi", "run")
pub fn run(conn: Port, db: String, sql: String) -> Result(Nil, String)

/// Creates a Connection object.
/// Note that this doesn't actually cause a real "connection" to
/// be created. Actual DB connections are created lazily as
/// queries come in.
/// This is useful if you want to use the sqlight-compatible
/// query and exec functions.
pub fn connect(port: Port, to db: String, log_with logger_fn: fn(LogEntry) -> Nil) -> Connection {
  Connection(port:, db:, logger_fn:)
}

/// sqlight-compatible query function.
pub fn query(
  sql: String,
  on conn: Connection,
  with params: List(Param),
  expecting decoder: decode.Decoder(a),
) -> Result(List(a), Error) {
  let before = now_ms()

  use rows <- try(select(
    conn.port,
    conn.db,
    sql,
    params,
  ) |> result.map_error(fn(error) {
    conn.logger_fn(LogEntry(sql:, duration_ms: now_ms() - before))
    DatabaseError(error)
  }))

  conn.logger_fn(LogEntry(sql:, duration_ms: now_ms() - before))

  let decoded = list.map(rows, decode.run(_, decoder))

  let successes = list.filter_map(decoded, fn(x) {
    case x {
      Ok(i) -> Ok(i)
      Error(_) -> Error(Nil)
    }
  })

  let errors = list.filter_map(decoded, fn(x) {
    case x {
      Ok(_) -> Error(Nil)
      Error(e) -> Ok(e)
    }
  })

  case successes, errors {
    s, [] -> Ok(s)
    _, [_, ..] as e -> Error(DecodeError(list.flatten(e)))
  }
}

/// sqlight-compatible exec function.
/// Runs one or more SQL statements without prepared statements.
pub fn exec(
  sql: String,
  on conn: Connection,
) -> Result(Nil, Error) {
  let before = now_ms()

  let return = run(conn.port, conn.db, sql)
  |> result.map_error(DatabaseError)

  conn.logger_fn(LogEntry(sql:, duration_ms: now_ms() - before))

  return
}
