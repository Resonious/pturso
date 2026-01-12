# pturso

[![Package Version](https://img.shields.io/hexpm/v/pturso)](https://hex.pm/packages/pturso)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/pturso/)

[Turso](https://github.com/tursodatabase/turso) client for Gleam, implemented as an external Rust process (source in this repo, a binary called `erso`) communicating via Erlang ports.

Since this uses ports, it works with [erlang-linux-builds](https://github.com/gleam-community/erlang-linux-builds).

The API is similar to that of [sqlight](https://github.com/lpil/sqlight) which means it also plays nicely with [parrot](github.com/daniellionel01/parrot).

Currently this only works for the Erlang target. Turso technically does run in wasm, so we could support JS theoretically but I haven't had the need yet!

This project is architected in a "very async" way. No benchmarks yet though, so no clue if that pays off or not.
* The `erso` binary can handle multiple in-flight messages at the same time; only one `erso` can power many DBs and many queries at once.
* Turso itself is async IO all the way down

Communication between BEAM and `erso` uses the [bincode/wincode](https://github.com/anza-xyz/wincode) format, which is quick and Rust-native.

```sh
gleam add pturso
```

## Quick Start

```gleam
import gleam/dynamic/decode
import pturso

pub fn main() {
  // Start the erso binary (auto-downloads if needed)
  let assert Ok(port) = pturso.start()

  // Connect to a database (local files only for now; no Turso Cloud support yet)
  let conn = pturso.connect(port, to: "my_app.db", log_with: fn(_) { Nil })

  // Create a table
  let assert Ok(Nil) = pturso.exec("
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT
    )
  ", on: conn)

  // Insert data with parameters
  let assert Ok(_) = pturso.query(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    on: conn,
    with: [pturso.String("Alice"), pturso.String("alice@example.com")],
    expecting: decode.success(Nil),
  )

  // Query data
  let user_decoder = {
    use id <- decode.field(0, decode.int)
    use name <- decode.field(1, decode.string)
    use email <- decode.field(2, decode.optional(decode.string))
    decode.success(#(id, name, email))
  }

  let assert Ok(users) = pturso.query(
    "SELECT id, name, email FROM users",
    on: conn,
    with: [],
    expecting: user_decoder,
  )

  echo users
  // [#(1, "Alice", Some("alice@example.com"))]

  pturso.stop(port)
}
```

## Starting the Client

pturso requires a native binary (`erso`) to perform database queries.
The source code for that binary is in this repo in the `./rust` directory.

In Gleam, this binary can be managed in a few ways.

### Automatic (easiest)

```gleam
// Auto-detects the best method:
// 1. Uses ERSO env var if set
// 2. Uses cargo install if cargo is available
// 3. Downloads pre-built binary from GitHub releases
let assert Ok(port) = pturso.start()
```

### Build from source via crates.io

Requires [Cargo](https://doc.rust-lang.org/cargo/).

```gleam
// Uses ~/.cargo/bin/erso, runs `cargo install erso` if not present
let assert Ok(port) = pturso.start_from_crates_io()
```

### Pre-built from GitHub Releases

Downloads artifacts built via the GH Actions on this repo.

```gleam
// Downloads pre-built binary to ~/.cache/pturso/erso
let assert Ok(port) = pturso.start_from_github_release()
```

### Custom Binary Path

For if you want to manage the `erso` binary yourself.

```gleam
// Use your own binary
let assert Ok(port) = pturso.start_with_binary("/path/to/erso")
```

## Connecting to Databases

```gleam
// Local SQLite file
let conn = pturso.connect(port, to: "local.db", log_with: fn(_) { Nil })

// In-memory database
let conn = pturso.connect(port, to: ":memory:", log_with: fn(_) { Nil })

// With query logging
let conn = pturso.connect(port, to: "app.db", log_with: fn(entry) {
  io.println("SQL: " <> entry.sql <> " (" <> int.to_string(entry.duration_ms) <> "ms)")
})
```

> [!NOTE]
> Actual database connections are created lazily. `connect` doesn't interact with the filesystem, and so it never fails. You'll have to run an actual query before the DB is created/read.

## Parameter Types

```gleam
// Supported parameter types
pturso.Null
pturso.Int(42)
pturso.Float(3.14)
pturso.String("hello")
pturso.Blob(<<1, 2, 3>>)
```

## Running Multiple Statements

Use `exec` to run multiple SQL statements (e.g., migrations):

```gleam
let assert Ok(Nil) = pturso.exec("
  CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);
  CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER, body TEXT);
  CREATE INDEX idx_comments_post ON comments(post_id);
", on: conn)
```

## Error Handling

Unfortunately we just use Strings for errors right now.

The only reason is I haven't needed specific error handling for different error codes yet.

```gleam
case pturso.query("SELECT * FROM nonexistent", on: conn, with: [], expecting: decoder) {
  Ok(rows) -> // handle rows
  Error(pturso.DatabaseError(message)) -> io.println("DB error: " <> message)
  Error(pturso.DecodeError(errors)) -> io.println("Decode error")
}
```

## Development

Most of the code is Erlang files in `src/*.erl`. I did it this way since Gleam doesn't seem to have good stdlib ports support yet.

```sh
gleam test             # Run Gleam/Erlang tests
cd rust && cargo test  # Run Rust tests
```

Further documentation can be found at <https://hexdocs.pm/pturso>.
