use std::{io, net::SocketAddr, num::NonZeroUsize, sync::Arc};
use tokio_util::sync::CancellationToken;
use utp::{SocketOpts, UtpSocketUdp};

pub struct Socket {
    pub socket: Arc<UtpSocketUdp>,
    cancel: CancellationToken,
}

impl Socket {
    pub async fn bind(address: SocketAddr, capacity: usize) -> io::Result<Self> {
        let options = SocketOpts {
            disable_nagle: true,
            max_live_vsocks: NonZeroUsize::new(capacity),
            ..Default::default()
        };
        let cancel = options.cancellation_token.clone();
        let socket = UtpSocketUdp::new_udp_with_opts(address, options, Default::default())
            .await
            .map_err(io::Error::other)?;
        Ok(Self { socket, cancel })
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
