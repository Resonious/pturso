import gleam/dynamic.{type Dynamic}

/// Parameter values for queries (needed for bincode encoding)
pub type Param {
  Null
  Int(Int)
  Float(Float)
  String(String)
  Blob(BitArray)
}

pub type Row =
  List(Dynamic)

pub type Connection

@external(erlang, "pturso_ffi", "start")
pub fn start(binary_path: String) -> Result(Connection, String)

@external(erlang, "pturso_ffi", "stop")
pub fn stop(conn: Connection) -> Nil

@external(erlang, "pturso_ffi", "select")
pub fn select(
  conn: Connection,
  db: String,
  query: String,
  params: List(Param),
) -> Result(List(Row), String)

@external(erlang, "pturso_ffi", "execute")
pub fn execute(
  conn: Connection,
  db: String,
  query: String,
  params: List(Param),
) -> Result(Int, String)
