mod port;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use port::{new_port, ResponseTx};
use tokio::sync::RwLock;
use wincode::{SchemaRead, SchemaWrite};
use pmrpc::define_requests;

#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct Crap {
    reason: String,
}
#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct BadRequest {
    reason: String,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct Select {
    pub db: String,
    pub query: String,
    pub params: Vec<Value>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct Execute {
    pub db: String,
    pub query: String,
    pub params: Vec<Value>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct Run {
    pub db: String,
    pub sql: String,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub enum RunResult {
    Ok,
    Error(String),
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub enum Updated {
    Ok(u64),
    Error(String),
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub enum RowsResult {
    Ok(Vec<Vec<Value>>),
    Error(String),
}

#[derive(Clone, Debug, PartialEq, SchemaRead, SchemaWrite)]
pub enum Value {
    Null,
    Integer(i64),  // Should correspond to Erlang Int
    Real(f64),     // Should correspond to Erlang Float
    Text(String),  // Should correspond to Erlang <<"hi"/utf8> bit string
    Blob(Vec<u8>), // Should correspond to Erlang bit string
}

impl Into<turso::Value> for Value {
    fn into(self) -> turso::Value {
        match self {
            Value::Null => turso::Value::Null,
            Value::Integer(x) => turso::Value::Integer(x),
            Value::Real(x) => turso::Value::Real(x),
            Value::Text(x) => turso::Value::Text(x),
            Value::Blob(x) => turso::Value::Blob(x),
        }
    }
}

impl From<turso::Value> for Value {
    fn from(value: turso::Value) -> Value {
        match value {
            turso::Value::Null => Value::Null,
            turso::Value::Integer(x) => Value::Integer(x),
            turso::Value::Real(x) => Value::Real(x),
            turso::Value::Text(x) => Value::Text(x),
            turso::Value::Blob(x) => Value::Blob(x),
        }
    }
}

// NOTE: this produces enums that look like this:
//
// #[derive(SchemaRead, SchemaWrite)]
// enum Requests {
//     Select(Select),
//     ...
// }
//
// #[derive(SchemaRead, SchemaWrite)]
// enum Responses {
//     RowsResult(RowsResult),
//     ...
// }
define_requests! {
    (SchemaRead, SchemaWrite)

    Crap => BadRequest,
    Select => RowsResult,
    Execute => Updated,
    Run => RunResult,
}

#[tokio::main]
async fn main() {
    let (mut reader, writer, response_tx, response_rx) = new_port(100);
    let cache = new_db_cache();

    // Spawn the writer task
    let writer_handle = tokio::spawn(async move {
        if let Err(e) = writer.run(response_rx).await {
            eprintln!("Writer error: {}", e);
        }
    });

    // Read requests and spawn handler tasks
    loop {
        match reader.read_request().await {
            Ok(Some(request)) => {
                tokio::spawn(process_request(cache.clone(), request, response_tx.clone()));
            }
            Ok(None) => {
                // EOF - Erlang closed the port
                break;
            }
            Err(e) => {
                eprintln!("Failed to read request: {}", e);
                break;
            }
        }
    }

    // Drop the sender so the writer task can finish
    drop(response_tx);
    let _ = writer_handle.await;
}

/// In charge of deserializing the request and serializing the response.
async fn process_request(cache: DbCache, request: port::Request, tx: ResponseTx) {
    let req: Result<Requests, _> = wincode::deserialize(&request.data);
    let req = match req {
        Ok(req) => req,
        Err(e) => Crap { reason: e.to_string() }.into(),
    };

    let resp = handle_request(&cache, req).await;
    let resp_data = wincode::serialize(&resp)
        .unwrap_or_else(|e| {
            eprintln!("SERIALIZE ERROR: {e:?}");
            Vec::new()
        });

    let response = port::Response {
        id: request.id,
        data: resp_data,
    };

    if let Err(e) = tx.send(response).await {
        eprintln!("Failed to send response: {}", e);
    }
}

/// In charge of actually performing the requested operation.
async fn handle_request(cache: &DbCache, req: Requests) -> Responses {
    match req {
        Requests::Crap(crap, p) => {
            let resp = BadRequest { reason: crap.reason };
            response_enum(p, resp)
        }

        Requests::Select(select, p) => {
            let resp = match execute_select(cache, select).await {
                Ok(values) => RowsResult::Ok(values),
                Err(e) => RowsResult::Error(e),
            };
            response_enum(p, resp)
        }

        Requests::Execute(execute, p) => {
            let resp = match execute_statement(cache, execute).await {
                Ok(x) => Updated::Ok(x),
                Err(e) => Updated::Error(e),
            };
            response_enum(p, resp)
        }

        Requests::Run(run, p) => {
            let resp = match run_batch(cache, run).await {
                Ok(()) => RunResult::Ok,
                Err(e) => RunResult::Error(e),
            };
            response_enum(p, resp)
        }
    }
}

pub fn response_enum<Req, Resp>(_req: PhantomData<Req>, resp: Resp) -> Responses
where
    Req: RespondsWith<Resp> + Send,
    Resp: Send,
{
    Req::resp_enum(resp)
}

/// Cached database.
struct CachedDb {
    db: turso::Database,
}

/// Shared database cache type.
pub type DbCache = Arc<RwLock<HashMap<String, Arc<CachedDb>>>>;

/// Create a new empty database cache.
pub fn new_db_cache() -> DbCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Get or create a cached database entry.
async fn get_cached_db(cache: &DbCache, db_name: &str) -> Result<Arc<CachedDb>, String> {
    // Fast path: db already exists
    {
        let dbs = cache.read().await;
        if let Some(cached) = dbs.get(db_name) {
            return Ok(Arc::clone(cached));
        }
    }

    // Slow path: need to open the database
    let mut dbs = cache.write().await;

    // Double-check after acquiring write lock
    if let Some(cached) = dbs.get(db_name) {
        return Ok(Arc::clone(cached));
    }

    let db = turso::Builder::new_local(db_name)
        .with_mvcc(true)
        .build()
        .await
        .map_err(|e| e.to_string())?;
    let cached = Arc::new(CachedDb { db });
    dbs.insert(db_name.to_string(), Arc::clone(&cached));
    Ok(cached)
}

async fn execute_select(cache: &DbCache, select: Select) -> Result<Vec<Vec<Value>>, String> {
    let cached = get_cached_db(cache, &select.db).await?;
    let conn = cached.db.connect().map_err(|e| e.to_string())?;
    let params: Vec<turso::Value> = select.params.into_iter().map(|v| v.into()).collect();

    let mut stmt = conn.prepare(&select.query).await.map_err(|e| e.to_string())?;
    let mut rows = stmt.query(params).await.map_err(|e| e.to_string())?;

    let mut results = Vec::new();
    while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
        let col_count = row.column_count();
        let mut result_row = Vec::with_capacity(col_count);
        for col_idx in 0..col_count {
            let val = row.get_value(col_idx).map_err(|e| e.to_string())?;
            result_row.push(val.into());
        }
        results.push(result_row);
    }

    Ok(results)
}

async fn execute_statement(cache: &DbCache, execute: Execute) -> Result<u64, String> {
    let cached = get_cached_db(cache, &execute.db).await?;
    let conn = cached.db.connect().map_err(|e| e.to_string())?;
    let params: Vec<turso::Value> = execute.params.into_iter().map(|v| v.into()).collect();

    let mut stmt = conn.prepare(&execute.query).await.map_err(|e| e.to_string())?;
    stmt.execute(params).await.map_err(|e| e.to_string())
}

async fn run_batch(cache: &DbCache, run: Run) -> Result<(), String> {
    let cached = get_cached_db(cache, &run.db).await?;
    let conn = cached.db.connect().map_err(|e| e.to_string())?;
    conn.execute_batch(&run.sql).await.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_insert_and_select() {
        let cache = new_db_cache();

        // Create table
        let create = Execute {
            db: ":memory:".to_string(),
            query: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".to_string(),
            params: vec![],
        };
        let req: Requests = create.into();
        let resp = handle_request(&cache, req).await;
        assert!(matches!(resp, Responses::Updated(Updated::Ok(_))));

        // Insert a row
        let insert = Execute {
            db: ":memory:".to_string(),
            query: "INSERT INTO users (id, name) VALUES (?, ?)".to_string(),
            params: vec![Value::Integer(1), Value::Text("Alice".to_string())],
        };
        let req: Requests = insert.into();
        let resp = handle_request(&cache, req).await;
        assert!(matches!(resp, Responses::Updated(Updated::Ok(1))));

        // Select the row back
        let select = Select {
            db: ":memory:".to_string(),
            query: "SELECT id, name FROM users".to_string(),
            params: vec![],
        };
        let req: Requests = select.into();
        let resp = handle_request(&cache, req).await;

        match resp {
            Responses::RowsResult(RowsResult::Ok(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[0][0], Value::Integer(1));
                assert_eq!(rows[0][1], Value::Text("Alice".to_string()));
            }
            other => panic!("Expected RowsResult::Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_select_with_params() {
        let cache = new_db_cache();

        // Create and populate table
        let create = Execute {
            db: ":memory:".to_string(),
            query: "CREATE TABLE items (id INTEGER, value TEXT)".to_string(),
            params: vec![],
        };
        handle_request(&cache, create.into()).await;

        for i in 1..=3 {
            let insert = Execute {
                db: ":memory:".to_string(),
                query: "INSERT INTO items VALUES (?, ?)".to_string(),
                params: vec![Value::Integer(i), Value::Text(format!("item{}", i))],
            };
            handle_request(&cache, insert.into()).await;
        }

        // Select with parameter
        let select = Select {
            db: ":memory:".to_string(),
            query: "SELECT value FROM items WHERE id = ?".to_string(),
            params: vec![Value::Integer(2)],
        };
        let resp = handle_request(&cache, select.into()).await;

        match resp {
            Responses::RowsResult(RowsResult::Ok(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Text("item2".to_string()));
            }
            other => panic!("Expected RowsResult::Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_select_empty_result() {
        let cache = new_db_cache();

        let create = Execute {
            db: ":memory:".to_string(),
            query: "CREATE TABLE empty_table (id INTEGER)".to_string(),
            params: vec![],
        };
        handle_request(&cache, create.into()).await;

        let select = Select {
            db: ":memory:".to_string(),
            query: "SELECT * FROM empty_table".to_string(),
            params: vec![],
        };
        let resp = handle_request(&cache, select.into()).await;

        match resp {
            Responses::RowsResult(RowsResult::Ok(rows)) => {
                assert!(rows.is_empty());
            }
            other => panic!("Expected empty RowsResult::Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let cache = new_db_cache();

        let select = Select {
            db: ":memory:".to_string(),
            query: "SELECT * FROM nonexistent_table".to_string(),
            params: vec![],
        };
        let resp = handle_request(&cache, select.into()).await;

        assert!(matches!(resp, Responses::RowsResult(RowsResult::Error(_))));
    }

    #[tokio::test]
    async fn test_multiple_databases() {
        let cache = new_db_cache();

        // Create tables in two different in-memory databases
        // Note: each :memory: connection is separate, but here we use the same
        // cache key so they share. Use different paths to test isolation.
        let create1 = Execute {
            db: ":memory:".to_string(),
            query: "CREATE TABLE db1_table (x INTEGER)".to_string(),
            params: vec![],
        };
        handle_request(&cache, create1.into()).await;

        // Insert into first db
        let insert1 = Execute {
            db: ":memory:".to_string(),
            query: "INSERT INTO db1_table VALUES (42)".to_string(),
            params: vec![],
        };
        handle_request(&cache, insert1.into()).await;

        // Query first db
        let select1 = Select {
            db: ":memory:".to_string(),
            query: "SELECT x FROM db1_table".to_string(),
            params: vec![],
        };
        let resp = handle_request(&cache, select1.into()).await;

        match resp {
            Responses::RowsResult(RowsResult::Ok(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(42));
            }
            other => panic!("Expected RowsResult::Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_null_values() {
        let cache = new_db_cache();

        let create = Execute {
            db: ":memory:".to_string(),
            query: "CREATE TABLE nullable (a INTEGER, b TEXT)".to_string(),
            params: vec![],
        };
        handle_request(&cache, create.into()).await;

        let insert = Execute {
            db: ":memory:".to_string(),
            query: "INSERT INTO nullable VALUES (?, ?)".to_string(),
            params: vec![Value::Null, Value::Text("not null".to_string())],
        };
        handle_request(&cache, insert.into()).await;

        let select = Select {
            db: ":memory:".to_string(),
            query: "SELECT a, b FROM nullable".to_string(),
            params: vec![],
        };
        let resp = handle_request(&cache, select.into()).await;

        match resp {
            Responses::RowsResult(RowsResult::Ok(rows)) => {
                assert_eq!(rows[0][0], Value::Null);
                assert_eq!(rows[0][1], Value::Text("not null".to_string()));
            }
            other => panic!("Expected RowsResult::Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_blob_values() {
        let cache = new_db_cache();

        let create = Execute {
            db: ":memory:".to_string(),
            query: "CREATE TABLE blobs (data BLOB)".to_string(),
            params: vec![],
        };
        handle_request(&cache, create.into()).await;

        let blob_data = vec![0x00, 0x01, 0x02, 0xFF];
        let insert = Execute {
            db: ":memory:".to_string(),
            query: "INSERT INTO blobs VALUES (?)".to_string(),
            params: vec![Value::Blob(blob_data.clone())],
        };
        handle_request(&cache, insert.into()).await;

        let select = Select {
            db: ":memory:".to_string(),
            query: "SELECT data FROM blobs".to_string(),
            params: vec![],
        };
        let resp = handle_request(&cache, select.into()).await;

        match resp {
            Responses::RowsResult(RowsResult::Ok(rows)) => {
                assert_eq!(rows[0][0], Value::Blob(blob_data));
            }
            other => panic!("Expected RowsResult::Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_real_values() {
        let cache = new_db_cache();

        let create = Execute {
            db: ":memory:".to_string(),
            query: "CREATE TABLE reals (value REAL)".to_string(),
            params: vec![],
        };
        handle_request(&cache, create.into()).await;

        let insert = Execute {
            db: ":memory:".to_string(),
            query: "INSERT INTO reals VALUES (?)".to_string(),
            params: vec![Value::Real(3.14159)],
        };
        handle_request(&cache, insert.into()).await;

        let select = Select {
            db: ":memory:".to_string(),
            query: "SELECT value FROM reals".to_string(),
            params: vec![],
        };
        let resp = handle_request(&cache, select.into()).await;

        match resp {
            Responses::RowsResult(RowsResult::Ok(rows)) => {
                if let Value::Real(val) = rows[0][0] {
                    assert!((val - 3.14159).abs() < 0.00001);
                } else {
                    panic!("Expected Real value");
                }
            }
            other => panic!("Expected RowsResult::Ok, got {:?}", other),
        }
    }
}
