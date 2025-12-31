mod port;

use std::collections::{HashMap, hash_map};
use std::marker::PhantomData;
use std::sync::OnceLock;

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
pub struct Insert {
    pub db: String,
    pub query: String,
    pub params: Vec<Value>,
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
    Insert => Updated,
}

#[tokio::main]
async fn main() {
    let (mut reader, writer, response_tx, response_rx) = new_port(100);

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
                tokio::spawn(process_request(request, response_tx.clone()));
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
async fn process_request(request: port::Request, tx: ResponseTx) {
    let req: Result<Requests, _> = wincode::deserialize(&request.data);
    let req = match req {
        Ok(req) => req,
        Err(e) => Crap { reason: e.to_string() }.into(),
    };

    let resp = handle_request(req).await;
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
async fn handle_request(req: Requests) -> Responses {
    match req {
        Requests::Crap(crap, p) => {
            let resp = BadRequest { reason: crap.reason };
            response_enum(p, resp)
        }

        Requests::Select(select, p) => {
            let resp = match execute_select(select).await {
                Ok(values) => RowsResult::Ok(values),
                Err(e) => RowsResult::Error(e),
            };
            response_enum(p, resp)
        }

        Requests::Insert(insert, p) => {
            let resp = match execute_insert(insert).await {
                Ok(x) => Updated::Ok(x),
                Err(e) => Updated::Error(e),
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

/// Global map of open databases, keyed by filename.
static DBS: OnceLock<RwLock<HashMap<String, turso::Database>>> = OnceLock::new();

fn get_dbs() -> &'static RwLock<HashMap<String, turso::Database>> {
    DBS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Get a connection to the database, opening it if necessary.
async fn get_connection(db_name: &str) -> Result<turso::Connection, String> {
    {
        let dbs = get_dbs().read().await;
        if let Some(db) = dbs.get(db_name) {
            return db.connect().map_err(|e| e.to_string());
        }
    }

    let mut dbs = get_dbs().write().await;

    let db = match dbs.entry(db_name.to_string()) {
        hash_map::Entry::Occupied(e) => e.into_mut(),
        hash_map::Entry::Vacant(e) => {
            let db = turso::Builder::new_local(db_name)
                .build()
                .await
                .map_err(|e| e.to_string())?;
            e.insert(db)
        }
    };
    db.connect().map_err(|e| e.to_string())
}

async fn execute_select(select: Select) -> Result<Vec<Vec<Value>>, String> {
    let conn = get_connection(&select.db).await?;
    let params: Vec<turso::Value> = select.params.into_iter().map(|v| v.into()).collect();
    let mut rows = conn.query(&select.query, params).await.map_err(|e| e.to_string())?;

    let mut results = Vec::new();
    while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
        let mut col_idx = 0;
        let mut result_row = Vec::with_capacity(16);
        loop {
            match row.get_value(col_idx) {
                Ok(val) => {
                    result_row.push(val.into());
                    col_idx += 1;
                }
                Err(_) => break,
            }
        }
        results.push(result_row);
    }

    Ok(results)
}

async fn execute_insert(insert: Insert) -> Result<u64, String> {
    let conn = get_connection(&insert.db).await?;
    let params: Vec<turso::Value> = insert.params.into_iter().map(|v| v.into()).collect();
    let rows_affected = conn.execute(&insert.query, params).await.map_err(|e| e.to_string())?;
    Ok(rows_affected)
}
