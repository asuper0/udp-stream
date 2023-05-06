use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use std::future::Future;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::UdpSocket,
    sync::mpsc,
    sync::Mutex,
};
use bytes::{Bytes, BytesMut, Buf};

const UDP_BUFFER_SIZE: usize = 17480; // 17kb
                                      // const UDP_TIMEOUT: u64 = 10 * 1000; // 10sec
const CHANNEL_LEN: usize = 100;

pub struct UdpListener {
    handler: tokio::task::JoinHandle<()>,
    receiver: Arc<Mutex<mpsc::Receiver<(UdpStream, SocketAddr)>>>,
    local_addr: SocketAddr,
}

impl Drop for UdpListener {
    fn drop(&mut self) {
        self.handler.abort();
    }
}

/// An I/O object representing a UDP socket listening for incoming connections.
///
/// This object can be converted into a stream of incoming connections for
/// various forms of processing.
///
/// # Examples
///
/// ```no_run
/// use udp_stream::UdpListener;
///
/// use std::{io, net::SocketAddr};
/// # async fn process_socket<T>(_socket: T) {}
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn Error>> {
///     let mut listener = UdpListener::bind(SocketAddr::from_str("127.0.0.1:8080")?).await?;
///
///     loop {
///         let (socket, _) = listener.accept().await?;
///         process_socket(socket).await;
///     }
/// }
/// ```
impl UdpListener {
    #[allow(unreachable_code)]
    pub async fn bind(local_addr: SocketAddr) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel(CHANNEL_LEN);
        let udp_socket = UdpSocket::bind(local_addr).await?;
        let local_addr = udp_socket.local_addr()?;

        let handler = tokio::spawn(async move {
            let mut streams: HashMap<
                SocketAddr,
                mpsc::Sender<Bytes>,
            > = HashMap::new();
            let socket = Arc::new(udp_socket);
            let (drop_tx,mut drop_rx) = mpsc::channel(1);

            let mut buf = BytesMut::with_capacity(UDP_BUFFER_SIZE*3);
            loop {
                if buf.capacity() < UDP_BUFFER_SIZE{
                    buf.reserve(UDP_BUFFER_SIZE * 3);
                }
                tokio::select! {
                    some_drop = drop_rx.recv() => {
                        let peer_addr = some_drop.unwrap();
                        streams.remove(&peer_addr);
                    }
                    received = socket.recv_buf_from(&mut buf) => {

                        if let Ok((len, addr)) = received{
                            match streams.get_mut(&addr) {
                                Some(child_tx) => {
                                    if let Err(_) = child_tx.send(buf.copy_to_bytes(len)).await {
                                        child_tx.closed().await;
                                        streams.remove(&addr);
                                        continue;
                                    }
                                }
                                None => {
                                    let (child_tx, child_rx) = mpsc::channel(CHANNEL_LEN);
                                    if child_tx.send(buf.copy_to_bytes(len)).await.is_err()
                                    || tx
                                        .send((
                                            UdpStream {
                                                local_addr: local_addr,
                                                peer_addr: addr,
                                                receiver: Arc::new(Mutex::new(child_rx)),
                                                socket: socket.clone(),
                                                handler: None,
                                                drop: Some(drop_tx.clone()),
                                            },
                                            addr,
                                        ))
                                        .await
                                        .is_err()
                                    {
                                        println!("some err with new conn");
                                        continue;
                                    };
                                    streams.insert(addr, child_tx.clone());
                                }
                            }
                        }        else{
                            println!("error while recv {:?}", received);
                        }
                        
                        //error?
                    }
                }
            }

            panic!("listener exit");
        });
        Ok(Self {
            handler,
            receiver: Arc::new(Mutex::new(rx)),
            local_addr,
        })
    }
    ///Returns the local address that this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr.clone())
    }
    pub async fn accept(&self) -> io::Result<(UdpStream, SocketAddr)> {
        (&self.receiver)
            .lock()
            .await
            .recv()
            .await
            .ok_or(io::Error::new(io::ErrorKind::BrokenPipe, "Broken Pipe"))
    }
}
/// An I/O object representing a UDP stream connected to a remote endpoint.
///
/// A UDP stream can either be created by connecting to an endpoint, via the
/// [`connect`] method, or by [accepting] a connection from a [listener].
///
/// [`connect`]: struct.UdpStream.html#method.connect
/// [accepting]: struct.UdpListener.html#method.accept
/// [listener]: struct.UdpListener.html
#[derive(Debug)]
pub struct UdpStream {
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    receiver: Arc<Mutex<mpsc::Receiver<Bytes>>>,
    socket: Arc<tokio::net::UdpSocket>,
    handler: Option<tokio::task::JoinHandle<()>>,
    drop: Option<mpsc::Sender<SocketAddr>>,
}

impl Drop for UdpStream {
    fn drop(&mut self) {
        if let Some(handler) = &self.handler {
            handler.abort()
        }

        if let Some(drop) = &self.drop{
            let _ = drop.try_send(self.peer_addr.clone());
        };
    }
}

impl UdpStream {
    /// Create a new UDP stream connected to the specified address.
    ///
    /// This function will create a new UDP socket and attempt to connect it to
    /// the `addr` provided. The returned future will be resolved once the
    /// stream has successfully connected, or it will return an error if one
    /// occurs.
    #[allow(unused)]
    pub async fn connect(addr: SocketAddr) -> Result<Self, tokio::io::Error> {
        let local_addr: SocketAddr = if addr.is_ipv4() {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        } else {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
        };

        let socket = Arc::new(UdpSocket::bind(local_addr).await?);
        let local_addr = socket.local_addr()?;
        socket.connect(&addr);
        let (child_tx, child_rx) = mpsc::channel(CHANNEL_LEN);

        let drop = Arc::new(Mutex::new(false));
        let socket_inner = socket.clone();
        let handler = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(UDP_BUFFER_SIZE);
            while let Ok((len, addr)) = socket_inner.clone().recv_buf_from(&mut buf).await {
                if child_tx.send(buf.copy_to_bytes(len)).await.is_err() {
                    child_tx.closed();
                    break;
                }

                if buf.capacity() < UDP_BUFFER_SIZE{
                    buf.reserve(UDP_BUFFER_SIZE * 3);
                }
            }
        });
        Ok(UdpStream {
            local_addr: local_addr,
            peer_addr: addr,
            receiver: Arc::new(Mutex::new(child_rx)),
            socket: socket.clone(),
            handler: Some(handler),
            drop: None,
        })
    }
    #[allow(unused)]
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.peer_addr)
    }
    #[allow(unused)]
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
    #[allow(unused)]
    pub fn shutdown(&self) {
        if let Some(drop) = &self.drop{
            let _ = drop.try_send(self.peer_addr.clone());
        };
    }
}

impl AsyncRead for UdpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        let mut socket = match Pin::new(&mut Box::pin(self.receiver.lock())).poll(cx) {
            Poll::Ready(socket) => socket,
            Poll::Pending => return Poll::Pending,
        };

        match socket.poll_recv(cx) {
            Poll::Ready(Some(inner_buf)) => {
                return {
                    buf.put_slice(&inner_buf[..]);
                    Poll::Ready(Ok(()))
                }
            }
            Poll::Ready(None) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Broken Pipe",
                )))
            }
            Poll::Pending => return Poll::Pending,
        };
    }
}

impl AsyncWrite for UdpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.socket.poll_send_to(cx, &buf, self.peer_addr) {
            Poll::Ready(Ok(r)) => Poll::Ready(Ok(r)),
            Poll::Ready(Err(e)) => {
                if let Some(drop) = &self.drop{
                    let _ = drop.try_send(self.peer_addr.clone());
                };
                Poll::Ready(Err(e))
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn test_buf(){
    use tokio::runtime::Builder;

    use std::{net::SocketAddr, str::FromStr, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UdpSocket,
        time::timeout,
    };

    const LISTENER_ADDR: &str = "127.0.0.1:8095";

    const MAX_TEST_CONN:usize = 10;
    const SEND_TIMES:usize = 20;
    const UDP_TIMEOUT: u64 = 1000; // 10sec
    const UDP_BUFFER_SIZE: usize = 17480; // 17kb

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let handle1 = runtime.spawn(async {

        let listener = UdpListener::bind(SocketAddr::from_str(LISTENER_ADDR).unwrap()).await.unwrap();
        let mut handles = Vec::with_capacity(MAX_TEST_CONN);
        println!("listener start");
        for _ in 0..MAX_TEST_CONN {
            let (mut stream, _addr) = listener.accept().await.unwrap();

            println!("accpet from {}", _addr);

            handles.push(tokio::spawn(async move {
                let mut buf = vec![0u8; UDP_BUFFER_SIZE];
                // loop {
                for i in 0..SEND_TIMES{
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
                            println!("timeout, no more data");
                            return ;
                        }
                    };

                    // println!("recv {} bytes from {}", n, _addr);
                    for b in &buf[..n]{
                        assert_eq!(*b, i as u8);
                    }

                    stream.write_all(&buf[0..n]).await.unwrap();
                }
            }));
        }

        for h in handles{
            h.await.unwrap();
        }
    });

    let handle2 = runtime.spawn(async{
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut handles = Vec::with_capacity(MAX_TEST_CONN);
        for _ in 0..MAX_TEST_CONN{
            handles.push(tokio::spawn(async {
                let socket = UdpSocket::bind(SocketAddr::from_str("127.0.0.1:0").unwrap()).await.unwrap();
                socket.connect(SocketAddr::from_str(LISTENER_ADDR).unwrap()).await.unwrap();

                for i in 0..SEND_TIMES {
                    let mut buffer: Vec<u8> = (0..256).into_iter().map(|_| i as u8).collect();
                    socket.send(&buffer).await.unwrap();
                    // socket.recv(&mut buffer).await.unwrap();
                    let n =timeout(Duration::from_millis(UDP_TIMEOUT), socket.recv(&mut buffer))
                        .await.unwrap().unwrap();
                    for b in &buffer[..n]{
                        assert_eq!(*b, i as u8);
                    }

                }
            }));
        }

        for h in handles{
            h.await.unwrap();
        }
    });

    runtime.block_on(handle2).unwrap();
    runtime.block_on(handle1).unwrap();
}
