// Uncomment this block to pass the first stage
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use anyhow::Result;

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

    loop {
        let conn = listener.accept().await;
        tokio::spawn(async move {
            match conn {
                Ok((stream, _)) => {
                    println!("new client!");
                    if let Err(e) = handle_client(stream).await {
                        println!("error: {}", e);
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}

pub async fn handle_client(mut stream: TcpStream) -> Result<()> {
    let mut buf = vec![0; 512];

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                println!("client disconnected");
                return Ok(());
            }
            Ok(n) => {
                println!("read {} bytes", n);
                stream.write(b"+PONG\r\n").await?;
            }
            Err(e) => {
                println!("error: {}", e);
                return Ok(());
            }
        }
    }
}
