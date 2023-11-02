use crate::{bindings::http::types, types::FieldMap};
use anyhow::anyhow;
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use std::future::Future;
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use wasmtime_wasi::preview2::{
    self, AbortOnDropJoinHandle, HostInputStream, HostOutputStream, StreamError, Subscribe,
};

pub type HyperIncomingBody = BoxBody<Bytes, anyhow::Error>;

/// Holds onto the things needed to construct a [`HostIncomingBody`] until we are ready to build
/// one. The HostIncomingBody spawns a task that starts consuming the incoming body, and we don't
/// want to do that unless the user asks to consume the body.
pub struct HostIncomingBodyBuilder {
    pub body: HyperIncomingBody,
    pub between_bytes_timeout: Duration,
}

impl HostIncomingBodyBuilder {
    /// Consume the state held in the [`HostIncomingBodyBuilder`] to spawn a task that will drive the
    /// streaming body to completion. Data segments will be communicated out over the
    /// [`HostIncomingBodyStream`], and a [`HostFutureTrailers`] gives a way to block on/retrieve
    /// the trailers.
    pub fn build(mut self) -> HostIncomingBody {
        let (body_writer, body_receiver) = mpsc::channel(1);
        let (trailer_writer, trailers) = oneshot::channel();

        let worker = preview2::spawn(async move {
            loop {
                let frame = match tokio::time::timeout(
                    self.between_bytes_timeout,
                    http_body_util::BodyExt::frame(&mut self.body),
                )
                .await
                {
                    Ok(None) => break,

                    Ok(Some(Ok(frame))) => frame,

                    Ok(Some(Err(e))) => {
                        match body_writer.send(Err(e)).await {
                            Ok(_) => {}
                            // If the body read end has dropped, then we report this error with the
                            // trailers. unwrap and rewrap Err because the Ok side of these two Results
                            // are different.
                            Err(e) => {
                                let _ = trailer_writer.send(Err(e.0.unwrap_err()));
                            }
                        }
                        break;
                    }

                    Err(_) => {
                        match body_writer
                            .send(Err(types::Error::TimeoutError(
                                "data frame timed out".to_string(),
                            )
                            .into()))
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = trailer_writer.send(Err(e.0.unwrap_err()));
                            }
                        }
                        break;
                    }
                };

                if frame.is_trailers() {
                    // We know we're not going to write any more data frames at this point, so we
                    // explicitly drop the body_writer so that anything waiting on the read end returns
                    // immediately.
                    drop(body_writer);

                    let trailers = frame.into_trailers().unwrap();

                    // TODO: this will fail in two cases:
                    // 1. we've already used the channel once, which should be imposible,
                    // 2. the read end is closed.
                    // I'm not sure how to differentiate between these two cases, or really
                    // if we need to do anything to handle either.
                    let _ = trailer_writer.send(Ok(trailers));

                    break;
                }

                assert!(frame.is_data(), "frame wasn't data");

                let data = frame.into_data().unwrap();

                // If the receiver no longer exists, thats ok - in that case we want to keep the
                // loop running to relieve backpressure, so we get to the trailers.
                let _ = body_writer.send(Ok(data)).await;
            }
        });

        HostIncomingBody {
            worker,
            stream: Some(HostIncomingBodyStream::new(body_receiver)),
            trailers,
        }
    }
}

pub struct HostIncomingBody {
    pub worker: AbortOnDropJoinHandle<()>,
    pub stream: Option<HostIncomingBodyStream>,
    pub trailers: oneshot::Receiver<Result<hyper::HeaderMap, anyhow::Error>>,
}

impl HostIncomingBody {
    pub fn into_future_trailers(self) -> HostFutureTrailers {
        HostFutureTrailers {
            _worker: self.worker,
            state: HostFutureTrailersState::Waiting(self.trailers),
        }
    }
}

pub struct HostIncomingBodyStream {
    pub open: bool,
    pub receiver: mpsc::Receiver<Result<Bytes, anyhow::Error>>,
    pub buffer: Bytes,
    pub error: Option<anyhow::Error>,
}

impl HostIncomingBodyStream {
    fn new(receiver: mpsc::Receiver<Result<Bytes, anyhow::Error>>) -> Self {
        Self {
            open: true,
            receiver,
            buffer: Bytes::new(),
            error: None,
        }
    }
}

#[async_trait::async_trait]
impl HostInputStream for HostIncomingBodyStream {
    fn read(&mut self, size: usize) -> Result<Bytes, StreamError> {
        use mpsc::error::TryRecvError;

        if !self.buffer.is_empty() {
            let len = size.min(self.buffer.len());
            let chunk = self.buffer.split_to(len);
            return Ok(chunk);
        }

        if let Some(e) = self.error.take() {
            return Err(StreamError::LastOperationFailed(e));
        }

        if !self.open {
            return Err(StreamError::Closed);
        }

        match self.receiver.try_recv() {
            Ok(Ok(mut bytes)) => {
                let len = bytes.len().min(size);
                let chunk = bytes.split_to(len);
                if !bytes.is_empty() {
                    self.buffer = bytes;
                }

                return Ok(chunk);
            }

            Ok(Err(e)) => {
                self.open = false;
                return Err(StreamError::LastOperationFailed(e));
            }

            Err(TryRecvError::Empty) => {
                return Ok(Bytes::new());
            }

            Err(TryRecvError::Disconnected) => {
                self.open = false;
                return Err(StreamError::Closed);
            }
        }
    }
}

#[async_trait::async_trait]
impl Subscribe for HostIncomingBodyStream {
    async fn ready(&mut self) {
        if !self.buffer.is_empty() {
            return;
        }

        if !self.open {
            return;
        }

        match self.receiver.recv().await {
            Some(Ok(bytes)) => self.buffer = bytes,

            Some(Err(e)) => {
                self.error = Some(e);
                self.open = false;
            }

            None => self.open = false,
        }
    }
}

pub struct HostFutureTrailers {
    _worker: AbortOnDropJoinHandle<()>,
    pub state: HostFutureTrailersState,
}

pub enum HostFutureTrailersState {
    Waiting(oneshot::Receiver<Result<hyper::HeaderMap, anyhow::Error>>),
    Done(Result<FieldMap, types::Error>),
}

#[async_trait::async_trait]
impl Subscribe for HostFutureTrailers {
    async fn ready(&mut self) {
        if let HostFutureTrailersState::Waiting(rx) = &mut self.state {
            let result = match rx.await {
                Ok(Ok(headers)) => Ok(FieldMap::from(headers)),
                Ok(Err(e)) => Err(types::Error::ProtocolError(format!("hyper error: {e:?}"))),
                Err(_) => Err(types::Error::ProtocolError(
                    "stream hung up before trailers were received".to_string(),
                )),
            };
            self.state = HostFutureTrailersState::Done(result);
        }
    }
}

pub type HyperOutgoingBody = BoxBody<Bytes, anyhow::Error>;

pub enum FinishMessage {
    Finished,
    Trailers(hyper::HeaderMap),
    Abort,
}

pub struct HostOutgoingBody {
    pub body_output_stream: Option<Box<dyn HostOutputStream>>,
    pub finish_sender: Option<tokio::sync::oneshot::Sender<FinishMessage>>,
}

impl HostOutgoingBody {
    pub fn new() -> (Self, HyperOutgoingBody) {
        use http_body_util::BodyExt;
        use hyper::body::{Body, Frame};
        use std::task::{Context, Poll};
        use tokio::sync::oneshot::error::RecvError;
        struct BodyImpl {
            body_receiver: mpsc::Receiver<Bytes>,
            finish_receiver: Option<oneshot::Receiver<FinishMessage>>,
        }
        impl Body for BodyImpl {
            type Data = Bytes;
            type Error = anyhow::Error;
            fn poll_frame(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                match self.as_mut().body_receiver.poll_recv(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Some(frame)) => Poll::Ready(Some(Ok(Frame::data(frame)))),

                    // This means that the `body_sender` end of the channel has been dropped.
                    Poll::Ready(None) => {
                        if let Some(mut finish_receiver) = self.as_mut().finish_receiver.take() {
                            match Pin::new(&mut finish_receiver).poll(cx) {
                                Poll::Pending => {
                                    self.as_mut().finish_receiver = Some(finish_receiver);
                                    Poll::Pending
                                }
                                Poll::Ready(Ok(message)) => match message {
                                    FinishMessage::Finished => Poll::Ready(None),
                                    FinishMessage::Trailers(trailers) => {
                                        Poll::Ready(Some(Ok(Frame::trailers(trailers))))
                                    }
                                    FinishMessage::Abort => Poll::Ready(Some(Err(
                                        anyhow::anyhow!("response corrupted"),
                                    ))),
                                },
                                Poll::Ready(Err(RecvError { .. })) => Poll::Ready(None),
                            }
                        } else {
                            Poll::Ready(None)
                        }
                    }
                }
            }
        }

        let (body_sender, body_receiver) = mpsc::channel(1);
        let (finish_sender, finish_receiver) = oneshot::channel();
        let body_impl = BodyImpl {
            body_receiver,
            finish_receiver: Some(finish_receiver),
        }
        .boxed();
        (
            Self {
                // TODO: this capacity constant is arbitrary, and should be configurable
                body_output_stream: Some(Box::new(BodyWriteStream::new(1024 * 1024, body_sender))),
                finish_sender: Some(finish_sender),
            },
            body_impl,
        )
    }
}

/// Provides a [`HostOutputStream`] impl from a [`tokio::sync::mpsc::Sender`].
pub struct BodyWriteStream {
    writer: mpsc::Sender<Bytes>,
    pending: Option<Bytes>,
    write_budget: usize,
    closed: bool,
}

impl BodyWriteStream {
    /// Create a [`BodyWriteStream`].
    pub fn new(write_budget: usize, writer: mpsc::Sender<Bytes>) -> Self {
        BodyWriteStream {
            writer,
            write_budget,
            pending: None,
            closed: false,
        }
    }
}

#[async_trait::async_trait]
impl HostOutputStream for BodyWriteStream {
    fn write(&mut self, bytes: Bytes) -> Result<(), StreamError> {
        // If there are pending bytes to send then `check_write` wouldn't have
        // said that bytes could be written.
        if self.pending.is_some() {
            return Err(StreamError::Trap(anyhow!("write exceeded budget")));
        }

        // Delegate to `flush` which will attempt to write out the bytes. This
        // will attempt to send them immediately if possible. Note that this
        // will also handle the case that we're actually a closed channel.
        self.pending = Some(bytes);
        self.flush()
    }

    fn flush(&mut self) -> Result<(), StreamError> {
        // Try sending out the bytes now if we can, otherwise delay this to
        // later.
        if let Some(bytes) = self.pending.take() {
            match self.writer.try_send(bytes) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(bytes)) => self.pending = Some(bytes),
                // Note that the bytes are lost here but there's nothing we can
                // do about that because the channel is closed.
                Err(mpsc::error::TrySendError::Closed(_)) => self.closed = true,
            }
        }
        if self.closed {
            return Err(StreamError::Closed);
        }
        Ok(())
    }

    fn check_write(&mut self) -> Result<usize, StreamError> {
        // Try flushing out any pending bytes if there are any to clear the
        // `pending` field if possible. This will also check to see if we're a
        // closed channel.
        self.flush()?;

        if self.pending.is_some() {
            Ok(0)
        } else {
            Ok(self.write_budget)
        }
    }
}

#[async_trait::async_trait]
impl Subscribe for BodyWriteStream {
    async fn ready(&mut self) {
        if self.pending.is_none() || self.closed {
            return;
        }
        // If there's pending data and this isn't a closed channel yet then wait
        // for a permit that allows us to send, and on acquisition send the
        // buffered bytes.
        match self.writer.reserve().await {
            Ok(permit) => permit.send(self.pending.take().unwrap()),
            Err(_) => self.closed = true,
        }
    }
}
