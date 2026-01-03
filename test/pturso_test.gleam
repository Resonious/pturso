import gleam/dynamic/decode
import gleam/list
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
  let assert Ok(rows) = pturso.select(conn, ":memory:", "SELECT 1, 'hello'", [])

  let row_decoder = {
    use col1 <- decode.field(0, decode.int)
    use col2 <- decode.field(1, decode.string)
    decode.success(#(col1, col2))
  }

  let assert [row] = rows
  should.equal(decode.run(row, row_decoder), Ok(#(1, "hello")))
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
  let assert Ok(rows) =
    pturso.select(conn, ":memory:", "SELECT id, name FROM users", [])

  let user_decoder = {
    use id <- decode.field(0, decode.int)
    use name <- decode.field(1, decode.string)
    decode.success(#(id, name))
  }

  let assert [row] = rows
  should.equal(decode.run(row, user_decoder), Ok(#(1, "Alice")))
  pturso.stop(conn)
}

pub fn null_values_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let assert Ok(_) =
    pturso.execute(conn, ":memory:", "CREATE TABLE nullable (val TEXT)", [])
  let assert Ok(_) =
    pturso.execute(conn, ":memory:", "INSERT INTO nullable VALUES (?)", [pturso.Null])
  let assert Ok(rows) =
    pturso.select(conn, ":memory:", "SELECT val FROM nullable", [])

  let nullable_decoder = {
    use val <- decode.field(0, decode.optional(decode.string))
    decode.success(val)
  }

  let assert [row] = rows
  should.equal(decode.run(row, nullable_decoder), Ok(option.None))
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
  let assert Ok(rows) =
    pturso.select(conn, ":memory:", "SELECT i, r, t, b FROM types", [])

  let types_decoder = {
    use i <- decode.field(0, decode.int)
    use r <- decode.field(1, decode.float)
    use t <- decode.field(2, decode.string)
    use b <- decode.field(3, decode.bit_array)
    decode.success(#(i, r, t, b))
  }

  let assert [row] = rows
  let assert Ok(#(i, r, t, b)) = decode.run(row, types_decoder)
  should.equal(i, 42)
  should.be_true(r >. 3.13 && r <. 3.15)
  should.equal(t, "hello")
  should.equal(b, <<1, 2, 3>>)
  pturso.stop(conn)
}

pub fn multiple_rows_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let assert Ok(_) =
    pturso.execute(
      conn,
      ":memory:",
      "CREATE TABLE cats (name TEXT, age INTEGER)",
      [],
    )
  let assert Ok(_) =
    pturso.execute(
      conn,
      ":memory:",
      "INSERT INTO cats (name, age) VALUES (?, ?), (?, ?), (?, ?)",
      [
        pturso.String("Nubi"),
        pturso.Int(4),
        pturso.String("Biffy"),
        pturso.Int(10),
        pturso.String("Ginny"),
        pturso.Int(6),
      ],
    )
  let assert Ok(rows) =
    pturso.select(conn, ":memory:", "SELECT name, age FROM cats ORDER BY age", [])

  let cat_decoder = {
    use name <- decode.field(0, decode.string)
    use age <- decode.field(1, decode.int)
    decode.success(#(name, age))
  }

  let cats = list.map(rows, fn(row) {
    let assert Ok(cat) = decode.run(row, cat_decoder)
    cat
  })

  should.equal(cats, [#("Nubi", 4), #("Ginny", 6), #("Biffy", 10)])
  pturso.stop(conn)
}

pub fn error_handling_test() {
  let assert Ok(conn) = pturso.start(binary_path)
  let result = pturso.select(conn, ":memory:", "SELECT * FROM nonexistent_table", [])
  should.be_error(result)
  pturso.stop(conn)
}

pub fn run_multiple_statements_test() {
  let assert Ok(port) = pturso.start(binary_path)

  // Run multiple statements in a single call
  let assert Ok(Nil) =
    pturso.run(
      port,
      ":memory:",
      "
      CREATE TABLE cats (name TEXT, age INTEGER);
      INSERT INTO cats (name, age) VALUES ('Nubi', 4);
      INSERT INTO cats (name, age) VALUES ('Biffy', 10);
      INSERT INTO cats (name, age) VALUES ('Ginny', 6);
      ",
    )

  // Verify the data was inserted
  let assert Ok(rows) =
    pturso.select(port, ":memory:", "SELECT name, age FROM cats ORDER BY age", [])

  let cat_decoder = {
    use name <- decode.field(0, decode.string)
    use age <- decode.field(1, decode.int)
    decode.success(#(name, age))
  }

  let cats = list.map(rows, fn(row) {
    let assert Ok(cat) = decode.run(row, cat_decoder)
    cat
  })

  should.equal(cats, [#("Nubi", 4), #("Ginny", 6), #("Biffy", 10)])
  pturso.stop(port)
}

pub fn exec_with_connection_test() {
  let assert Ok(port) = pturso.start(binary_path)
  let conn = pturso.connect(port, to: ":memory:", log_with: fn(_) { Nil })

  // Use exec to run multiple statements
  let assert Ok(Nil) =
    pturso.exec(
      "
      CREATE TABLE dogs (name TEXT, age INTEGER);
      INSERT INTO dogs (name, age) VALUES ('Rex', 5);
      INSERT INTO dogs (name, age) VALUES ('Max', 3);
      ",
      on: conn,
    )

  // Use query to fetch results
  let dog_decoder = {
    use name <- decode.field(0, decode.string)
    use age <- decode.field(1, decode.int)
    decode.success(#(name, age))
  }

  let assert Ok(dogs) =
    pturso.query("SELECT name, age FROM dogs ORDER BY age", on: conn, with: [], expecting: dog_decoder)

  should.equal(dogs, [#("Max", 3), #("Rex", 5)])
  pturso.stop(port)
}
