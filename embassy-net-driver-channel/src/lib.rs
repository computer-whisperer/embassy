#![no_std]
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

// must go first!
mod fmt;

use core::cell::RefCell;
use core::mem::MaybeUninit;
use core::ops::{Deref, DerefMut};
use core::task::{Context, Poll};

pub use embassy_net_driver as driver;
use embassy_net_driver::{Capabilities, LinkState};
use embassy_sync::blocking_mutex::raw::{NoopRawMutex, RawMutex};
use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::waitqueue::WakerRegistration;
use embassy_sync::zerocopy_channel;

/// Channel state.
///
/// Holds a buffer of packets with size MTU, for both TX and RX.
///
/// `M` guards the queues and the shared link state. The default
/// `NoopRawMutex` is right when the [`Runner`] and the [`Device`] are polled
/// from the same executor; pick `CriticalSectionRawMutex` when the driver
/// runner lives on a higher-priority (interrupt) executor than the network
/// stack, so the two ends may interleave.
pub struct State<const MTU: usize, const N_RX: usize, const N_TX: usize, M: RawMutex = NoopRawMutex> {
    rx: [PacketBuf<MTU>; N_RX],
    tx: [PacketBuf<MTU>; N_TX],
    inner: MaybeUninit<StateInner<'static, MTU, M>>,
}

impl<const MTU: usize, const N_RX: usize, const N_TX: usize, M: RawMutex> State<MTU, N_RX, N_TX, M> {
    /// Create a new channel state.
    pub const fn new() -> Self {
        Self {
            rx: [const { PacketBuf::new() }; N_RX],
            tx: [const { PacketBuf::new() }; N_TX],
            inner: MaybeUninit::uninit(),
        }
    }
}

struct StateInner<'d, const MTU: usize, M: RawMutex> {
    rx: zerocopy_channel::Channel<'d, M, PacketBuf<MTU>>,
    tx: zerocopy_channel::Channel<'d, M, PacketBuf<MTU>>,
    shared: Mutex<M, RefCell<Shared>>,
}

struct Shared {
    link_state: LinkState,
    waker: WakerRegistration,
    hardware_address: driver::HardwareAddress,
}

/// Channel runner.
///
/// Holds the shared state and the lower end of channels for inbound and outbound packets.
pub struct Runner<'d, const MTU: usize, M: RawMutex = NoopRawMutex> {
    tx_chan: zerocopy_channel::Receiver<'d, M, PacketBuf<MTU>>,
    rx_chan: zerocopy_channel::Sender<'d, M, PacketBuf<MTU>>,
    shared: &'d Mutex<M, RefCell<Shared>>,
}

/// State runner.
///
/// Holds the shared state of the channel such as link state.
#[derive(Clone, Copy)]
pub struct StateRunner<'d, M: RawMutex = NoopRawMutex> {
    shared: &'d Mutex<M, RefCell<Shared>>,
}

/// RX runner.
///
/// Holds the lower end of the channel for passing inbound packets up the stack.
pub struct RxRunner<'d, const MTU: usize, M: RawMutex = NoopRawMutex> {
    rx_chan: zerocopy_channel::Sender<'d, M, PacketBuf<MTU>>,
}

/// TX runner.
///
/// Holds the lower end of the channel for passing outbound packets down the stack.
pub struct TxRunner<'d, const MTU: usize, M: RawMutex = NoopRawMutex> {
    tx_chan: zerocopy_channel::Receiver<'d, M, PacketBuf<MTU>>,
}

/// A slot for an inbound packet.
pub struct RxSlot<'a, const MTU: usize, M: RawMutex = NoopRawMutex>(zerocopy_channel::SendSlot<'a, M, PacketBuf<MTU>>);

impl<'a, const MTU: usize, M: RawMutex> From<zerocopy_channel::SendSlot<'a, M, PacketBuf<MTU>>> for RxSlot<'a, MTU, M> {
    fn from(value: zerocopy_channel::SendSlot<'a, M, PacketBuf<MTU>>) -> Self {
        Self(value)
    }
}

impl<const MTU: usize, M: RawMutex> Deref for RxSlot<'_, MTU, M> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0.buf
    }
}

impl<const MTU: usize, M: RawMutex> DerefMut for RxSlot<'_, MTU, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.buf
    }
}

impl<const MTU: usize, M: RawMutex> RxSlot<'_, MTU, M> {
    /// Mark packet of `len` bytes as pushed to the inbound channel.
    pub fn rx_done(mut self, len: usize) {
        self.0.len = len;
        self.0.send_done();
    }
}

/// A slot for an outbound packet.
pub struct TxSlot<'a, const MTU: usize, M: RawMutex = NoopRawMutex>(
    zerocopy_channel::ReceiveSlot<'a, M, PacketBuf<MTU>>,
);

impl<const MTU: usize, M: RawMutex> Deref for TxSlot<'_, MTU, M> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let len = self.0.len;
        &self.0.buf[..len]
    }
}

impl<const MTU: usize, M: RawMutex> DerefMut for TxSlot<'_, MTU, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let len = self.0.len;
        &mut self.0.buf[..len]
    }
}

impl<const MTU: usize, M: RawMutex> TxSlot<'_, MTU, M> {
    /// Mark outbound packet as processed.
    pub fn tx_done(self) {
        self.0.receive_done();
    }
}

impl<'a, const MTU: usize, M: RawMutex> From<zerocopy_channel::ReceiveSlot<'a, M, PacketBuf<MTU>>>
    for TxSlot<'a, MTU, M>
{
    fn from(value: zerocopy_channel::ReceiveSlot<'a, M, PacketBuf<MTU>>) -> Self {
        Self(value)
    }
}

impl<'d, const MTU: usize, M: RawMutex> Runner<'d, MTU, M> {
    /// Split the runner into separate runners for controlling state, rx and tx.
    pub fn split(self) -> (StateRunner<'d, M>, RxRunner<'d, MTU, M>, TxRunner<'d, MTU, M>) {
        (
            StateRunner { shared: self.shared },
            RxRunner { rx_chan: self.rx_chan },
            TxRunner { tx_chan: self.tx_chan },
        )
    }

    /// Split the runner into separate runners for controlling state, rx and tx borrowing the underlying state.
    pub fn borrow_split(&mut self) -> (StateRunner<'_, M>, RxRunner<'_, MTU, M>, TxRunner<'_, MTU, M>) {
        (
            StateRunner { shared: self.shared },
            RxRunner {
                rx_chan: self.rx_chan.borrow(),
            },
            TxRunner {
                tx_chan: self.tx_chan.borrow(),
            },
        )
    }

    /// Create a state runner sharing the state channel.
    pub fn state_runner(&self) -> StateRunner<'d, M> {
        StateRunner { shared: self.shared }
    }

    /// Set the link state.
    pub fn set_link_state(&mut self, state: LinkState) {
        self.shared.lock(|s| {
            let s = &mut *s.borrow_mut();
            s.link_state = state;
            s.waker.wake();
        });
    }

    /// Set the hardware address.
    pub fn set_hardware_address(&mut self, address: driver::HardwareAddress) {
        self.shared.lock(|s| {
            let s = &mut *s.borrow_mut();
            s.hardware_address = address;
            s.waker.wake();
        });
    }

    /// Wait until there is space for more inbound packets and return a slot.
    pub async fn rx_buf(&mut self) -> RxSlot<'_, MTU, M> {
        self.rx_chan.send().await.into()
    }

    /// Check if there is space for more inbound packets right now.
    pub fn try_rx_buf(&mut self) -> Option<RxSlot<'_, MTU, M>> {
        self.rx_chan.try_send().map(Into::into)
    }

    /// Polling the inbound channel if there is space for packets.
    pub fn poll_rx_buf(&'_ mut self, cx: &mut Context) -> Poll<RxSlot<'_, MTU, M>> {
        match self.rx_chan.poll_send(cx) {
            Poll::Ready(slot) => Poll::Ready(slot.into()),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Wait until there is space for more outbound packets and return a slot.
    pub async fn tx_buf(&mut self) -> TxSlot<'_, MTU, M> {
        self.tx_chan.receive().await.into()
    }

    /// Check if there is space for more outbound packets right now.
    pub fn try_tx_buf(&mut self) -> Option<TxSlot<'_, MTU, M>> {
        self.tx_chan.try_receive().map(Into::into)
    }

    /// Polling the outbound channel if there is space for packets.
    pub fn poll_tx_buf(&mut self, cx: &mut Context) -> Poll<TxSlot<'_, MTU, M>> {
        match self.tx_chan.poll_receive(cx) {
            Poll::Ready(slot) => Poll::Ready(slot.into()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'d, M: RawMutex> StateRunner<'d, M> {
    /// Set link state.
    pub fn set_link_state(&self, state: LinkState) {
        self.shared.lock(|s| {
            let s = &mut *s.borrow_mut();
            s.link_state = state;
            s.waker.wake();
        });
    }

    /// Set the hardware address.
    pub fn set_hardware_address(&self, address: driver::HardwareAddress) {
        self.shared.lock(|s| {
            let s = &mut *s.borrow_mut();
            s.hardware_address = address;
            s.waker.wake();
        });
    }
}

impl<'d, const MTU: usize, M: RawMutex> RxRunner<'d, MTU, M> {
    /// Wait until there is space for more inbound packets and return a slot.
    pub async fn rx_buf(&mut self) -> RxSlot<'_, MTU, M> {
        self.rx_chan.send().await.into()
    }

    /// Check if there is space for more inbound packets right now.
    pub fn try_rx_buf(&mut self) -> Option<RxSlot<'_, MTU, M>> {
        self.rx_chan.try_send().map(Into::into)
    }

    /// Polling the inbound channel if there is space for packets.
    pub fn poll_rx_buf(&mut self, cx: &mut Context) -> Poll<RxSlot<'_, MTU, M>> {
        match self.rx_chan.poll_send(cx) {
            Poll::Ready(slot) => Poll::Ready(slot.into()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'d, const MTU: usize, M: RawMutex> TxRunner<'d, MTU, M> {
    /// Wait until there is space for more outbound packets and return a slot.
    pub async fn tx_buf(&mut self) -> TxSlot<'_, MTU, M> {
        self.tx_chan.receive().await.into()
    }

    /// Check if there is space for more outbound packets right now.
    pub fn try_tx_buf(&mut self) -> Option<TxSlot<'_, MTU, M>> {
        self.tx_chan.try_receive().map(Into::into)
    }

    /// Polling the outbound channel if there is space for packets.
    pub fn poll_tx_buf(&mut self, cx: &mut Context) -> Poll<TxSlot<'_, MTU, M>> {
        match self.tx_chan.poll_receive(cx) {
            Poll::Ready(slot) => Poll::Ready(slot.into()),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Create a channel.
///
/// Returns a pair of handles for interfacing with the peripheral and the networking stack.
///
/// The runner is interfacing with the peripheral at the lower part of the stack.
/// The device is interfacing with the networking stack on the layer above.
pub fn new<'d, const MTU: usize, const N_RX: usize, const N_TX: usize, M: RawMutex>(
    state: &'d mut State<MTU, N_RX, N_TX, M>,
    hardware_address: driver::HardwareAddress,
) -> (Runner<'d, MTU, M>, Device<'d, MTU, M>) {
    let mut caps = Capabilities::default();
    caps.max_transmission_unit = MTU;

    // safety: this is a self-referential struct, however:
    // - it can't move while the `'d` borrow is active.
    // - when the borrow ends, the dangling references inside the MaybeUninit will never be used again.
    let state_uninit: *mut MaybeUninit<StateInner<'d, MTU, M>> =
        (&mut state.inner as *mut MaybeUninit<StateInner<'static, MTU, M>>).cast();
    let state = unsafe { &mut *state_uninit }.write(StateInner {
        rx: zerocopy_channel::Channel::new(&mut state.rx[..]),
        tx: zerocopy_channel::Channel::new(&mut state.tx[..]),
        shared: Mutex::new(RefCell::new(Shared {
            link_state: LinkState::Down,
            hardware_address,
            waker: WakerRegistration::new(),
        })),
    });

    let (rx_sender, rx_receiver) = state.rx.split();
    let (tx_sender, tx_receiver) = state.tx.split();

    (
        Runner {
            tx_chan: tx_receiver,
            rx_chan: rx_sender,
            shared: &state.shared,
        },
        Device {
            caps,
            shared: &state.shared,
            rx: rx_receiver,
            tx: tx_sender,
        },
    )
}

/// Represents a packet of size MTU.
pub struct PacketBuf<const MTU: usize> {
    len: usize,
    buf: [u8; MTU],
}

impl<const MTU: usize> PacketBuf<MTU> {
    /// Create a new packet buffer.
    pub const fn new() -> Self {
        Self { len: 0, buf: [0; MTU] }
    }
}

/// Channel device.
///
/// Holds the shared state and upper end of channels for inbound and outbound packets.
pub struct Device<'d, const MTU: usize, M: RawMutex = NoopRawMutex> {
    rx: zerocopy_channel::Receiver<'d, M, PacketBuf<MTU>>,
    tx: zerocopy_channel::Sender<'d, M, PacketBuf<MTU>>,
    shared: &'d Mutex<M, RefCell<Shared>>,
    caps: Capabilities,
}

impl<'d, const MTU: usize, M: RawMutex> embassy_net_driver::Driver for Device<'d, MTU, M> {
    type RxToken<'a>
        = RxToken<'a, MTU, M>
    where
        Self: 'a;
    type TxToken<'a>
        = TxToken<'a, MTU, M>
    where
        Self: 'a;

    fn receive(&mut self, cx: &mut Context) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.rx.poll_receive(cx).is_ready() && self.tx.poll_send(cx).is_ready() {
            Some((RxToken { rx: self.rx.borrow() }, TxToken { tx: self.tx.borrow() }))
        } else {
            None
        }
    }

    /// Construct a transmit token.
    fn transmit(&mut self, cx: &mut Context) -> Option<Self::TxToken<'_>> {
        if self.tx.poll_send(cx).is_ready() {
            Some(TxToken { tx: self.tx.borrow() })
        } else {
            None
        }
    }

    /// Get a description of device capabilities.
    fn capabilities(&self) -> Capabilities {
        self.caps.clone()
    }

    fn hardware_address(&self) -> driver::HardwareAddress {
        self.shared.lock(|s| s.borrow().hardware_address)
    }

    fn link_state(&mut self, cx: &mut Context) -> LinkState {
        self.shared.lock(|s| {
            let s = &mut *s.borrow_mut();
            s.waker.register(cx.waker());
            s.link_state
        })
    }
}

/// A rx token.
///
/// Holds inbound receive channel and interfaces with embassy-net-driver.
pub struct RxToken<'a, const MTU: usize, M: RawMutex = NoopRawMutex> {
    rx: zerocopy_channel::Receiver<'a, M, PacketBuf<MTU>>,
}

impl<'a, const MTU: usize, M: RawMutex> embassy_net_driver::RxToken for RxToken<'a, MTU, M> {
    fn consume<R, F>(mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        // NOTE(unwrap): we checked the queue wasn't full when creating the token.
        let mut pkt = unwrap!(self.rx.try_receive());
        let len = pkt.len;
        let r = f(&mut pkt.buf[..len]);
        pkt.receive_done();
        r
    }
}

/// A tx token.
///
/// Holds outbound transmit channel and interfaces with embassy-net-driver.
pub struct TxToken<'a, const MTU: usize, M: RawMutex = NoopRawMutex> {
    tx: zerocopy_channel::Sender<'a, M, PacketBuf<MTU>>,
}

impl<'a, const MTU: usize, M: RawMutex> embassy_net_driver::TxToken for TxToken<'a, MTU, M> {
    fn consume<R, F>(mut self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        // NOTE(unwrap): we checked the queue wasn't full when creating the token.
        let mut pkt = unwrap!(self.tx.try_send());
        let r = f(&mut pkt.buf[..len]);
        pkt.len = len;
        pkt.send_done();
        r
    }
}
