use tokio::runtime::Builder;
use udp_stream::buf_with_bytesmut::*;

use std::{net::SocketAddr, str::FromStr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
    time::timeout,
};

use criterion::{criterion_group, criterion_main, Criterion};

const MAX_TEST_CONN:usize = 100;
const SEND_TIMES:usize = 2000;
const UDP_TIMEOUT: u64 = 100; // 10sec
const LISTENER_ADDR: &str = "127.0.0.1:8090";

async fn do_many_packets() -> std::io::Result<()> {
    const UDP_BUFFER_SIZE: usize = 17480; // 17kb
    // println!("listener start");
    let listener = UdpListener::bind(SocketAddr::from_str(LISTENER_ADDR).unwrap()).await.unwrap();
    let mut handles = Vec::with_capacity(MAX_TEST_CONN);
    for _ in 0..MAX_TEST_CONN {
        let (mut stream, _addr) = listener.accept().await.unwrap();

        // println!("accpet from {}", _addr);

        handles.push(tokio::spawn(async move {
            let mut buf = vec![0u8; UDP_BUFFER_SIZE];
            // loop {
            for _ in 0..SEND_TIMES{
                let n = match timeout(Duration::from_millis(UDP_TIMEOUT), stream.read(&mut buf))
                    .await
                {
                    Ok(recv_data)=> match recv_data{
                        Ok(len) => len,
                        Err(_) => {
                            println!("shutdown {}", _addr);
                            stream.shutdown();
                            return;
                        }
                    },
                    Err(_) =>{
                        // println!("timeout, no more data");
                        return ;
                    }
                };

                stream.write_all(&buf[0..n.min(1)]).await.unwrap();
            }
        }));
    }

    for h in handles{
        h.await.unwrap();
    }

    Ok(())
}

async fn clients() -> std::io::Result<()> {
    // wait for the listener to start
    // println!("client start");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut handles = Vec::with_capacity(MAX_TEST_CONN);

    for _ in 0..MAX_TEST_CONN {
        handles.push(tokio::spawn(async {
            let mut buffer = [0u8; 256];
            let socket = UdpSocket::bind(SocketAddr::from_str("127.0.0.1:0").unwrap()).await.unwrap();
            socket.connect(SocketAddr::from_str(LISTENER_ADDR).unwrap()).await.unwrap();

            for _ in 0..SEND_TIMES {
                socket.send(&buffer).await.unwrap();
                // socket.recv(&mut buffer).await.unwrap();
                timeout(Duration::from_millis(UDP_TIMEOUT), socket.recv(&mut buffer))
                    .await.unwrap().unwrap();
                // let n = match timeout(Duration::from_millis(UDP_TIMEOUT), socket.recv(&mut buffer))
                    // .await
                // {
                //     Ok(recv_data)=> match recv_data{
                //         Ok(len) => len,
                //         Err(_) => {
                //             println!("shutdown {}", _addr);
                //             stream.shutdown();
                //             return;
                //         }
                //     },
                //     Err(_) =>{
                //         // println!("timeout, no more data");
                //         return ;
                //     }
                // };
            }

            Ok::<(), std::io::Error>(())
        }));
    }

    for h in handles{
        h.await?.unwrap();
    }

    Ok(())
}

fn many_packets() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let handle = runtime.spawn(do_many_packets());
    let handle_client = runtime.spawn(clients());

    runtime.block_on(handle).unwrap().unwrap();
    runtime.block_on(handle_client).unwrap().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sample-size-example");
    group.sample_size(10);
    group.bench_function("optimize buf", |b| b.iter(|| many_packets()));
    // c.bench_function("fib 40", |b| b.iter(|| fibonacci(black_box(40))));
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
