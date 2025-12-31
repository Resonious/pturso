import gleam/dynamic/decode
import gleam/option
import gleeunit
import gleeunit/should
import pturso

pub fn main() -> Nil {
  gleeunit.main()
}

const binary_path = "rust/target/debug/erso"

pub fn select_literal_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let assert Ok([[col1, col2]]) =
    pturso.select(conn, ":memory:", "SELECT 1, 'hello'", [])
  should.equal(decode.run(col1, decode.int), Ok(1))
  should.equal(decode.run(col2, decode.string), Ok("hello"))
  pturso.stop(conn)
}

pub fn create_table_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let assert Ok(_) =
    pturso.execute(conn, ":memory:", "CREATE TABLE test (id INTEGER, name TEXT)", [])
  pturso.stop(conn)
}

pub fn insert_and_select_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let assert Ok(_) =
    pturso.execute(conn, ":memory:", "CREATE TABLE users (id INTEGER, name TEXT)", [])
  let assert Ok(_) =
    pturso.execute(conn, ":memory:", "INSERT INTO users VALUES (?, ?)", [
      pturso.Int(1),
      pturso.String("Alice"),
    ])
  let assert Ok([[id, name]]) =
    pturso.select(conn, ":memory:", "SELECT id, name FROM users", [])
  should.equal(decode.run(id, decode.int), Ok(1))
  should.equal(decode.run(name, decode.string), Ok("Alice"))
  pturso.stop(conn)
}

pub fn null_values_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let assert Ok(_) =
    pturso.execute(conn, ":memory:", "CREATE TABLE nullable (val TEXT)", [])
  let assert Ok(_) =
    pturso.execute(conn, ":memory:", "INSERT INTO nullable VALUES (?)", [pturso.Null])
  let assert Ok([[val]]) =
    pturso.select(conn, ":memory:", "SELECT val FROM nullable", [])
  // NULL comes back as nil - use optional decoder
  should.equal(decode.run(val, decode.optional(decode.string)), Ok(option.None))
  pturso.stop(conn)
}

pub fn multiple_types_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let assert Ok(_) =
    pturso.execute(
      conn,
      ":memory:",
      "CREATE TABLE types (i INTEGER, r REAL, t TEXT, b BLOB)",
      [],
    )
  let assert Ok(_) =
    pturso.execute(
      conn,
      ":memory:",
      "INSERT INTO types VALUES (?, ?, ?, ?)",
      [
        pturso.Int(42),
        pturso.Float(3.14),
        pturso.String("hello"),
        pturso.Blob(<<1, 2, 3>>),
      ],
    )
  let assert Ok([[i, r, t, b]]) =
    pturso.select(conn, ":memory:", "SELECT i, r, t, b FROM types", [])
  should.equal(decode.run(i, decode.int), Ok(42))
  let assert Ok(f) = decode.run(r, decode.float)
  should.be_true(f >. 3.13 && f <. 3.15)
  should.equal(decode.run(t, decode.string), Ok("hello"))
  should.equal(decode.run(b, decode.bit_array), Ok(<<1, 2, 3>>))
  pturso.stop(conn)
}

pub fn error_handling_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let result = pturso.select(conn, ":memory:", "SELECT * FROM nonexistent_table", [])
  should.be_error(result)
  pturso.stop(conn)
}
