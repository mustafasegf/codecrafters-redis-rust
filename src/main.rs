use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::BytesMut;
// Uncomment this block to pass the first stage
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use anyhow::Result;
use tokio::sync::Mutex;

#[derive(Debug)]
pub enum Command {
    Echo(String),
    Ping(Option<String>),
    Set(String, String, Option<u64>),
    Get(String),
    Error(String),
    Unknown(String),
}

#[derive(Debug)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
    Null,
}

impl Value {
    pub fn to_command(&self) -> Command {
        println!("self: {:?}", self);
        match self {
            Value::Array(items) => match items.get(0) {
                Some(Value::BulkString(cmd)) => match cmd.to_ascii_lowercase().as_str() {
                    "ping" => match items.get(1) {
                        Some(Value::BulkString(msg)) => Command::Ping(Some(msg.to_string())),
                        None => Command::Ping(None),
                        _ => Command::Error("invalid command".to_string()),
                    },
                    "echo" => match items.get(1) {
                        Some(Value::BulkString(msg)) => Command::Echo(msg.to_string()),
                        _ => Command::Error("invalid command, need message".to_string()),
                    },
                    "set" => match (items.get(1), items.get(2), items.get(3), items.get(4)) {
                        (
                            Some(Value::BulkString(key)),
                            Some(Value::BulkString(value)),
                            Some(Value::BulkString(cmd)),
                            Some(Value::BulkString(expire)),
                        ) if cmd.to_ascii_lowercase() == "px" => Command::Set(
                            key.to_string(),
                            value.to_string(),
                            expire.parse::<u64>().ok(),
                        ),
                        (
                            Some(Value::BulkString(key)),
                            Some(Value::BulkString(value)),
                            None,
                            None,
                        ) => Command::Set(key.to_string(), value.to_string(), None),
                        (Some(Value::BulkString(_)), None, None, None) => {
                            Command::Error("invalid command, need value".to_string())
                        }
                        _ => Command::Error("invalid command, need key and value".to_string()),
                    },
                    "get" => match items.get(1) {
                        Some(Value::BulkString(key)) => Command::Get(key.to_string()),
                        _ => Command::Error("invalid command, need key".to_string()),
                    },

                    cmd => Command::Unknown(format!("unknown command: {}", cmd)),
                },
                _ => Command::Error("invalid command".to_string()),
            },
            _ => Command::Error("Error: Not Array".to_string()),
        }
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::Error(s) => format!("-{}\r\n", s),
            Value::Integer(i) => format!(":{}\r\n", i),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::Null => "$-1\r\n".to_string(),
            Value::Array(v) => {
                let mut s = String::new();
                s.push_str(&format!("*{}\r\n", v.len()));
                for value in v {
                    s.push_str(&value.to_string());
                }
                s
            }
        }
    }
}

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(1024),
        }
    }

    pub async fn read(&mut self) -> Result<Option<Value>> {
        loop {
            let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                return Ok(None);
            }

            match parse_command(self.buffer.split())? {
                Some((value, _)) => return Ok(Some(value)),
                None => {}
            }
        }
    }

    pub async fn write(&mut self, value: Value) -> Result<()> {
        self.stream.write_all(value.to_string().as_bytes()).await?;
        Ok(())
    }
}

pub fn parse_command(buf: BytesMut) -> Result<Option<(Value, usize)>> {
    match buf[0] as char {
        '*' => {
            let (arr_len, mut consumed) = match readline(&buf[1..]) {
                Some((line, len)) => {
                    let line = std::str::from_utf8(line)?;
                    let arr_len = line.parse::<usize>()?;

                    (arr_len, len + 1)
                }
                None => return Ok(None),
            };

            let mut arr = Vec::with_capacity(arr_len);

            for _ in 0..arr_len {
                match parse_command(BytesMut::from(&buf[consumed..]))? {
                    Some((value, len)) => {
                        arr.push(value);
                        consumed += len;
                    }
                    None => return Ok(None),
                }
            }

            Ok(Some((Value::Array(arr), consumed)))
        }
        '$' => {
            let (str_len, consumed) = match readline(&buf[1..]) {
                Some((line, len)) => {
                    let line = std::str::from_utf8(line)?;
                    let str_len = line.parse::<usize>()?;

                    (str_len, len + 1)
                }
                None => return Ok(None),
            };

            match consumed + str_len + 2 <= buf.len() {
                true => {
                    let str_buf = &buf[consumed..consumed + str_len];
                    let str_buf = std::str::from_utf8(str_buf)?;
                    let str_buf = str_buf.to_string();

                    Ok(Some((Value::BulkString(str_buf), consumed + str_len + 2)))
                }
                false => Ok(None),
            }
        }
        '+' => match readline(&buf[1..]) {
            Some((line, len)) => {
                let line = std::str::from_utf8(line)?;
                let line = line.to_string();

                Ok(Some((Value::SimpleString(line), len + 1)))
            }
            None => Ok(None),
        },
        ':' => match readline(&buf[1..]) {
            Some((line, len)) => {
                let line = std::str::from_utf8(line)?;
                let line = line.parse::<i64>()?;

                Ok(Some((Value::Integer(line), len + 1)))
            }
            None => Ok(None),
        },
        _ => Err(anyhow::anyhow!("unknown command")),
    }
}

fn readline(buf: &[u8]) -> Option<(&[u8], usize)> {
    for (i, b) in buf.windows(2).enumerate() {
        if b == b"\r\n".as_slice() {
            return Some((&buf[..i], i + 2));
        }
    }
    None
}

pub struct ExpiringValue {
    value: String,
    expires_at: Option<SystemTime>,
}

#[tokio::main]
async fn main() {
    println!("Starting server...");

    let listener = match TcpListener::bind("127.0.0.1:6379").await {
        Ok(listener) => listener,
        Err(e) => {
            println!("error binding socket: {}", e);
            return;
        }
    };

    let db: Arc<Mutex<HashMap<String, ExpiringValue>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let conn = listener.accept().await;
        tokio::spawn({
            let db = db.clone();
            async move {
                match conn {
                    Ok((stream, _)) => {
                        println!("new client!");
                        if let Err(e) = handle_client(stream, db).await {
                            println!("error: {}", e);
                        }
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }
            }
        });
    }
}

pub async fn handle_client(
    stream: TcpStream,
    db: Arc<Mutex<HashMap<String, ExpiringValue>>>,
) -> Result<()> {
    let mut conn = Connection::new(stream);

    loop {
        match conn.read().await {
            Err(e) => {
                println!("error: {}", e);
                return Ok(());
            }
            Ok(None) => {
                println!("client disconnected");
                return Ok(());
            }
            Ok(Some(value)) => {
                let resp = match value.to_command() {
                    Command::Echo(s) => Value::SimpleString(s),
                    Command::Ping(s) => {
                        Value::SimpleString(s.or_else(|| Some("PONG".to_string())).unwrap())
                    }
                    Command::Set(k, v, Some(exp)) => {
                        db.lock().await.insert(
                            k,
                            ExpiringValue {
                                value: v,
                                expires_at: Some(SystemTime::now() + Duration::from_millis(exp)),
                            },
                        );
                        Value::SimpleString("OK".to_string())
                    }
                    Command::Set(k, v, None) => {
                        db.lock().await.insert(
                            k,
                            ExpiringValue {
                                value: v,
                                expires_at: None,
                            },
                        );
                        Value::SimpleString("OK".to_string())
                    }
                    Command::Get(k) => match db.lock().await.get(&k) {
                        Some(ExpiringValue {
                            value: val,
                            expires_at: exp,
                        }) => exp
                            .and_then(|exp| match exp.duration_since(SystemTime::now()) {
                                Ok(time) if time.as_millis() > 0 => {
                                    Some(Value::BulkString(val.to_string()))
                                }
                                _ => Some(Value::Null),
                            })
                            .unwrap_or(Value::BulkString(val.to_string())),
                        None => Value::Null,
                    },
                    Command::Unknown(s) => Value::Error(s),
                    Command::Error(s) => Value::Error(s),
                };
                conn.write(resp).await?;
            }
        }
    }
}
