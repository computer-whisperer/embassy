use core::future::Future;
use core::mem::MaybeUninit;
#[cfg(feature = "bt-tx-repro-diag")]
use core::sync::atomic::{AtomicU32, Ordering};

use bt_hci::transport::WithIndicator;
use bt_hci::{ControllerToHostPacket, FromHciBytes, FromHciBytesError, HostToControllerPacket, PacketKind, WriteHci};
use embassy_futures::yield_now;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::mutex::Mutex;
use embassy_sync::zerocopy_channel;
use embassy_time::{Duration, Timer};
use embedded_io_async::ErrorKind;

use crate::consts::*;
use crate::runner::Bus;
pub use crate::spi::SpiBusCyw43;
use crate::util::round_up;
use crate::{CHIP, util};

pub(crate) struct BtState {
    rx: [BtPacketBuf; 4],
    tx: [BtPacketBuf; 4],
    inner: MaybeUninit<BtStateInnre<'static>>,
}

impl BtState {
    pub const fn new() -> Self {
        Self {
            rx: [const { BtPacketBuf::new() }; 4],
            tx: [const { BtPacketBuf::new() }; 4],
            inner: MaybeUninit::uninit(),
        }
    }
}

struct BtStateInnre<'d> {
    rx: zerocopy_channel::Channel<'d, NoopRawMutex, BtPacketBuf>,
    tx: zerocopy_channel::Channel<'d, NoopRawMutex, BtPacketBuf>,
}

/// Bluetooth driver.
pub struct BtDriver<'d> {
    rx: Mutex<NoopRawMutex, zerocopy_channel::Receiver<'d, NoopRawMutex, BtPacketBuf>>,
    tx: Mutex<NoopRawMutex, zerocopy_channel::Sender<'d, NoopRawMutex, BtPacketBuf>>,
}

pub(crate) struct BtRunner<'d> {
    pub(crate) tx_chan: zerocopy_channel::Receiver<'d, NoopRawMutex, BtPacketBuf>,
    rx_chan: zerocopy_channel::Sender<'d, NoopRawMutex, BtPacketBuf>,

    // Bluetooth circular buffers
    addr: u32,
    h2b_write_pointer: u32,
    b2h_read_pointer: u32,
    host_ctrl_cache_reg: u32,
    #[cfg(feature = "bt-hostctrl-cache-sample")]
    host_ctrl_sample_ops: u32,
    #[cfg(feature = "bt-hostctrl-cache-sample")]
    host_ctrl_sample_mismatch: u32,

    #[cfg(feature = "bt-rx-byte-probe")]
    probe_packets: u32,
    #[cfg(feature = "bt-rx-byte-probe")]
    probe_checked_o4: u32,
    #[cfg(feature = "bt-rx-byte-probe")]
    probe_mismatch_o4: u32,
    #[cfg(feature = "bt-rx-byte-probe")]
    probe_checked_o9: u32,
    #[cfg(feature = "bt-rx-byte-probe")]
    probe_mismatch_o9: u32,

    #[cfg(feature = "bt-rx-readback-probe")]
    rx_readback_packets: u32,
    #[cfg(feature = "bt-rx-readback-probe")]
    rx_readback_sampled: u32,
    #[cfg(feature = "bt-rx-readback-probe")]
    rx_readback_mismatch_packets: u32,
    #[cfg(feature = "bt-rx-readback-probe")]
    rx_readback_mismatch_bytes: u32,

    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_packets: u32,
    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_blocks: u32,
    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_mismatch: u32,
    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_carry: [u8; 15],
    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_carry_ring_off: [u32; 15],
    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_carry_len: u8,
    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_carry_handle: u16,
    #[cfg(feature = "bt-rx-sentinel-probe")]
    sentinel_carry_active: bool,
}

const BT_HCI_MTU: usize = 1024;

#[cfg(feature = "bt-rx-sentinel-probe")]
const SENTINEL_HEAD: [u8; 8] = *b"R4D_PING";
#[cfg(feature = "bt-rx-sentinel-probe")]
const SENTINEL_TAIL: [u8; 8] = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
#[cfg(any(
    feature = "bt-rx-sentinel-probe",
    feature = "bt-tx-repro-diag",
    feature = "bt-rx-readback-probe"
))]
const HCI_PACKET_TYPE_ACL: u8 = 0x02;
#[cfg(feature = "bt-rx-sentinel-probe")]
const ACL_PB_CONTINUING: u16 = 0x1;

#[cfg(any(feature = "bt-tx-repro-diag", feature = "bt-rx-readback-probe"))]
const REPRO_MAGIC: [u8; 8] = *b"R4D_SPI!";
#[cfg(any(feature = "bt-tx-repro-diag", feature = "bt-rx-readback-probe"))]
const REPRO_TEST_PACKET_LEN: usize = 220;
#[cfg(feature = "bt-tx-repro-diag")]
const REPRO_HEADER_LEN: usize = 16;

#[cfg(feature = "bt-ring-tx-publish-delay-10us")]
const BT_RING_TX_PUBLISH_DELAY_US: u64 = 10;
#[cfg(feature = "bt-ring-tx-intr-delay-10us")]
const BT_RING_TX_INTR_DELAY_US: u64 = 10;
#[cfg(feature = "bt-ring-rx-out-delay-10us")]
const BT_RING_RX_OUT_DELAY_US: u64 = 10;
#[cfg(feature = "bt-ring-rx-intr-delay-10us")]
const BT_RING_RX_INTR_DELAY_US: u64 = 10;
#[cfg(feature = "bt-hostctrl-cache-sample")]
const HOST_CTRL_SAMPLE_EVERY_OPS: u32 = 1024;

#[cfg(feature = "bt-tx-repro-diag")]
static TX_DIAG_DRIVER_CHECKED: AtomicU32 = AtomicU32::new(0);
#[cfg(feature = "bt-tx-repro-diag")]
static TX_DIAG_DRIVER_BAD: AtomicU32 = AtomicU32::new(0);
#[cfg(feature = "bt-tx-repro-diag")]
static TX_DIAG_RUNNER_CHECKED: AtomicU32 = AtomicU32::new(0);
#[cfg(feature = "bt-tx-repro-diag")]
static TX_DIAG_RUNNER_BAD: AtomicU32 = AtomicU32::new(0);
#[cfg(feature = "bt-tx-readback-probe")]
static TX_DIAG_READBACK_CHECKED: AtomicU32 = AtomicU32::new(0);
#[cfg(feature = "bt-tx-readback-probe")]
static TX_DIAG_READBACK_BAD: AtomicU32 = AtomicU32::new(0);

/// Represents a packet of size MTU.
pub(crate) struct BtPacketBuf {
    pub(crate) len: usize,
    pub(crate) buf: [u8; BT_HCI_MTU],
}

impl BtPacketBuf {
    /// Create a new packet buffer.
    pub const fn new() -> Self {
        Self {
            len: 0,
            buf: [0; BT_HCI_MTU],
        }
    }
}

pub(crate) fn new<'d>(state: &'d mut BtState) -> (BtRunner<'d>, BtDriver<'d>) {
    // safety: this is a self-referential struct, however:
    // - it can't move while the `'d` borrow is active.
    // - when the borrow ends, the dangling references inside the MaybeUninit will never be used again.
    let state_uninit: *mut MaybeUninit<BtStateInnre<'d>> =
        (&mut state.inner as *mut MaybeUninit<BtStateInnre<'static>>).cast();
    let state = unsafe { &mut *state_uninit }.write(BtStateInnre {
        rx: zerocopy_channel::Channel::new(&mut state.rx[..]),
        tx: zerocopy_channel::Channel::new(&mut state.tx[..]),
    });

    let (rx_sender, rx_receiver) = state.rx.split();
    let (tx_sender, tx_receiver) = state.tx.split();

    (
        BtRunner {
            tx_chan: tx_receiver,
            rx_chan: rx_sender,

            addr: 0,
            h2b_write_pointer: 0,
            b2h_read_pointer: 0,
            host_ctrl_cache_reg: 0,
            #[cfg(feature = "bt-hostctrl-cache-sample")]
            host_ctrl_sample_ops: 0,
            #[cfg(feature = "bt-hostctrl-cache-sample")]
            host_ctrl_sample_mismatch: 0,

            #[cfg(feature = "bt-rx-byte-probe")]
            probe_packets: 0,
            #[cfg(feature = "bt-rx-byte-probe")]
            probe_checked_o4: 0,
            #[cfg(feature = "bt-rx-byte-probe")]
            probe_mismatch_o4: 0,
            #[cfg(feature = "bt-rx-byte-probe")]
            probe_checked_o9: 0,
            #[cfg(feature = "bt-rx-byte-probe")]
            probe_mismatch_o9: 0,

            #[cfg(feature = "bt-rx-readback-probe")]
            rx_readback_packets: 0,
            #[cfg(feature = "bt-rx-readback-probe")]
            rx_readback_sampled: 0,
            #[cfg(feature = "bt-rx-readback-probe")]
            rx_readback_mismatch_packets: 0,
            #[cfg(feature = "bt-rx-readback-probe")]
            rx_readback_mismatch_bytes: 0,

            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_packets: 0,
            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_blocks: 0,
            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_mismatch: 0,
            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_carry: [0; 15],
            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_carry_ring_off: [0; 15],
            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_carry_len: 0,
            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_carry_handle: 0,
            #[cfg(feature = "bt-rx-sentinel-probe")]
            sentinel_carry_active: false,
        },
        BtDriver {
            rx: Mutex::new(rx_receiver),
            tx: Mutex::new(tx_sender),
        },
    )
}

pub(crate) struct CybtFwCb<'a> {
    pub p_next_line_start: &'a [u8],
}

pub(crate) struct HexFileData<'a> {
    pub addr_mode: i32,
    pub hi_addr: u16,
    pub dest_addr: u32,
    pub p_ds: &'a mut [u8],
}

pub(crate) fn read_firmware_patch_line(p_btfw_cb: &mut CybtFwCb, hfd: &mut HexFileData) -> u32 {
    let mut abs_base_addr32 = 0;

    loop {
        let num_bytes = p_btfw_cb.p_next_line_start[0];
        p_btfw_cb.p_next_line_start = &p_btfw_cb.p_next_line_start[1..];

        let addr = (p_btfw_cb.p_next_line_start[0] as u16) << 8 | p_btfw_cb.p_next_line_start[1] as u16;
        p_btfw_cb.p_next_line_start = &p_btfw_cb.p_next_line_start[2..];

        let line_type = p_btfw_cb.p_next_line_start[0];
        p_btfw_cb.p_next_line_start = &p_btfw_cb.p_next_line_start[1..];

        if num_bytes == 0 {
            break;
        }

        hfd.p_ds[..num_bytes as usize].copy_from_slice(&p_btfw_cb.p_next_line_start[..num_bytes as usize]);
        p_btfw_cb.p_next_line_start = &p_btfw_cb.p_next_line_start[num_bytes as usize..];

        match line_type {
            BTFW_HEX_LINE_TYPE_EXTENDED_ADDRESS => {
                hfd.hi_addr = (hfd.p_ds[0] as u16) << 8 | hfd.p_ds[1] as u16;
                hfd.addr_mode = BTFW_ADDR_MODE_EXTENDED;
            }
            BTFW_HEX_LINE_TYPE_EXTENDED_SEGMENT_ADDRESS => {
                hfd.hi_addr = (hfd.p_ds[0] as u16) << 8 | hfd.p_ds[1] as u16;
                hfd.addr_mode = BTFW_ADDR_MODE_SEGMENT;
            }
            BTFW_HEX_LINE_TYPE_ABSOLUTE_32BIT_ADDRESS => {
                abs_base_addr32 = (hfd.p_ds[0] as u32) << 24
                    | (hfd.p_ds[1] as u32) << 16
                    | (hfd.p_ds[2] as u32) << 8
                    | hfd.p_ds[3] as u32;
                hfd.addr_mode = BTFW_ADDR_MODE_LINEAR32;
            }
            BTFW_HEX_LINE_TYPE_DATA => {
                hfd.dest_addr = addr as u32;
                match hfd.addr_mode {
                    BTFW_ADDR_MODE_EXTENDED => hfd.dest_addr += (hfd.hi_addr as u32) << 16,
                    BTFW_ADDR_MODE_SEGMENT => hfd.dest_addr += (hfd.hi_addr as u32) << 4,
                    BTFW_ADDR_MODE_LINEAR32 => hfd.dest_addr += abs_base_addr32,
                    _ => {}
                }
                return num_bytes as u32;
            }
            _ => {}
        }
    }
    0
}

async fn bt_toggle_intr(bus: &mut impl Bus) {
    trace!("bt_toggle_intr");
    let old_val = bus.bp_read32(HOST_CTRL_REG_ADDR).await;
    // TODO: do we need to swap endianness on this read?
    let new_val = old_val ^ BTSDIO_REG_DATA_VALID_BITMASK;
    bus.bp_write32(HOST_CTRL_REG_ADDR, new_val).await;
}

#[cfg(feature = "bt-tx-repro-diag")]
#[derive(Clone, Copy)]
struct ReproDiag {
    seq: u16,
    mismatches: u16,
    first_offset: u16,
    first_expected: u8,
    first_actual: u8,
}

#[cfg(feature = "bt-tx-repro-diag")]
fn inspect_repro_hci_packet(packet: &[u8]) -> Option<ReproDiag> {
    if packet.len() < 5 || packet[0] != HCI_PACKET_TYPE_ACL {
        return None;
    }

    let acl_len = u16::from_le_bytes([packet[3], packet[4]]) as usize;
    if packet.len() < 5 + acl_len {
        return None;
    }

    inspect_repro_acl_payload(&packet[5..5 + acl_len])
}

#[cfg(feature = "bt-tx-repro-diag")]
fn inspect_repro_acl_payload(acl_payload: &[u8]) -> Option<ReproDiag> {
    if acl_payload.len() < 4 + 2 + REPRO_TEST_PACKET_LEN {
        return None;
    }

    let l2cap_len = u16::from_le_bytes([acl_payload[0], acl_payload[1]]) as usize;
    if l2cap_len < 2 + REPRO_TEST_PACKET_LEN || acl_payload.len() < 4 + l2cap_len {
        return None;
    }

    let l2_payload = &acl_payload[4..4 + l2cap_len];
    let sdu_len = u16::from_le_bytes([l2_payload[0], l2_payload[1]]) as usize;
    if sdu_len != REPRO_TEST_PACKET_LEN || l2_payload.len() < 2 + sdu_len {
        return None;
    }

    inspect_repro_payload(&l2_payload[2..2 + sdu_len])
}

#[cfg(feature = "bt-tx-repro-diag")]
fn inspect_repro_payload(payload: &[u8]) -> Option<ReproDiag> {
    if payload.len() != REPRO_TEST_PACKET_LEN || payload[..8] != REPRO_MAGIC {
        return None;
    }

    let seq = u16::from_le_bytes([payload[8], payload[9]]);
    let mut mismatches = 0u16;
    let mut first_offset = 0u16;
    let mut first_expected = 0u8;
    let mut first_actual = 0u8;
    let mut first_set = false;

    for (offset, actual) in payload.iter().copied().enumerate() {
        let expected = repro_expected_byte(offset, seq);
        if actual != expected {
            mismatches = mismatches.wrapping_add(1);
            if !first_set {
                first_set = true;
                first_offset = offset as u16;
                first_expected = expected;
                first_actual = actual;
            }
        }
    }

    Some(ReproDiag {
        seq,
        mismatches,
        first_offset,
        first_expected,
        first_actual,
    })
}

#[cfg(feature = "bt-tx-repro-diag")]
fn repro_expected_byte(offset: usize, seq: u16) -> u8 {
    match offset {
        0..=7 => REPRO_MAGIC[offset],
        8..=9 => seq.to_le_bytes()[offset - 8],
        10..=11 => (REPRO_TEST_PACKET_LEN as u16).to_le_bytes()[offset - 10],
        12 => 0xA5 ^ (seq as u8),
        13 => 0x5A ^ ((seq >> 8) as u8),
        14 => 0x3C,
        15 => 0xC3,
        _ => (((offset - REPRO_HEADER_LEN) as u16).wrapping_add(seq) & 0xff) as u8,
    }
}

#[cfg(feature = "bt-tx-repro-diag")]
fn tx_diag_record_driver(diag: ReproDiag) {
    let checked = TX_DIAG_DRIVER_CHECKED.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    if diag.mismatches != 0 {
        let bad = TX_DIAG_DRIVER_BAD.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        warn!(
            "[txdiag bt-driver] seq={} off={} got={:02x} exp={:02x} xor={:02x} mismatches={} bad={}/{}",
            diag.seq,
            diag.first_offset,
            diag.first_actual,
            diag.first_expected,
            diag.first_actual ^ diag.first_expected,
            diag.mismatches,
            bad,
            checked
        );
    } else if checked % 1024 == 0 {
        info!(
            "[txdiag bt-driver] checked={} bad={}",
            checked,
            TX_DIAG_DRIVER_BAD.load(Ordering::Relaxed)
        );
    }
}

#[cfg(feature = "bt-tx-repro-diag")]
fn tx_diag_record_runner(diag: ReproDiag) {
    let checked = TX_DIAG_RUNNER_CHECKED.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    if diag.mismatches != 0 {
        let bad = TX_DIAG_RUNNER_BAD.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        warn!(
            "[txdiag bt-runner] seq={} off={} got={:02x} exp={:02x} xor={:02x} mismatches={} bad={}/{}",
            diag.seq,
            diag.first_offset,
            diag.first_actual,
            diag.first_expected,
            diag.first_actual ^ diag.first_expected,
            diag.mismatches,
            bad,
            checked
        );
    } else if checked % 1024 == 0 {
        info!(
            "[txdiag bt-runner] checked={} bad={}",
            checked,
            TX_DIAG_RUNNER_BAD.load(Ordering::Relaxed)
        );
    }
}

impl<'a> BtRunner<'a> {
    #[cfg(feature = "bt-sharedbus-strict-checks")]
    fn assert_host_ctrl_bits(value: u32) {
        assert!(value & !BTSDIO_REG_HOST_CTRL_ALLOWED_BITMASK == 0);
    }

    #[cfg(feature = "bt-sharedbus-strict-checks")]
    fn assert_ring_pointer_value(value: u32) {
        assert!(value < BTSDIO_FWBUF_SIZE);
        assert!(value % 4 == 0);
    }

    #[cfg(feature = "bt-sharedbus-strict-checks")]
    fn assert_ring_span(offset: u32, len: usize) {
        assert!(offset < BTSDIO_FWBUF_SIZE);
        assert!(offset as usize + len <= BTSDIO_FWBUF_SIZE as usize);
    }

    async fn host_ctrl_sync_from_hw(&mut self, bus: &mut impl Bus) {
        let value = bus.bp_read32(HOST_CTRL_REG_ADDR).await;
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        Self::assert_host_ctrl_bits(value);
        self.host_ctrl_cache_reg = value;
    }

    #[cfg(feature = "bt-sharedbus-strict-checks")]
    async fn host_ctrl_assert_hw_matches_cache(&mut self, bus: &mut impl Bus) {
        let value = bus.bp_read32(HOST_CTRL_REG_ADDR).await;
        Self::assert_host_ctrl_bits(value);
        assert!(value == self.host_ctrl_cache_reg);
    }

    #[cfg(feature = "bt-hostctrl-cache-sample")]
    async fn host_ctrl_sample_hw(&mut self, bus: &mut impl Bus, phase: &str) {
        self.host_ctrl_sample_ops = self.host_ctrl_sample_ops.wrapping_add(1);
        if self.host_ctrl_sample_ops % HOST_CTRL_SAMPLE_EVERY_OPS != 0 {
            return;
        }

        let value = bus.bp_read32(HOST_CTRL_REG_ADDR).await;
        if value & !BTSDIO_REG_HOST_CTRL_ALLOWED_BITMASK != 0 {
            warn!(
                "host_ctrl sample invalid bits phase={} op={} hw=0x{:08x} cache=0x{:08x}",
                phase, self.host_ctrl_sample_ops, value, self.host_ctrl_cache_reg
            );
        }

        if value != self.host_ctrl_cache_reg {
            self.host_ctrl_sample_mismatch = self.host_ctrl_sample_mismatch.wrapping_add(1);
            warn!(
                "host_ctrl sample mismatch phase={} op={} hw=0x{:08x} cache=0x{:08x} mismatches={}",
                phase,
                self.host_ctrl_sample_ops,
                value,
                self.host_ctrl_cache_reg,
                self.host_ctrl_sample_mismatch
            );
        } else if (self.host_ctrl_sample_ops / HOST_CTRL_SAMPLE_EVERY_OPS) % 64 == 0 {
            info!(
                "host_ctrl sample ok samples={} mismatches={}",
                self.host_ctrl_sample_ops / HOST_CTRL_SAMPLE_EVERY_OPS,
                self.host_ctrl_sample_mismatch
            );
        }
    }

    async fn host_ctrl_read(&mut self, bus: &mut impl Bus) -> u32 {
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        Self::assert_host_ctrl_bits(self.host_ctrl_cache_reg);
        #[cfg(feature = "bt-hostctrl-cache-sample")]
        self.host_ctrl_sample_hw(bus, "read").await;
        #[cfg(not(feature = "bt-hostctrl-cache-sample"))]
        let _ = bus;

        self.host_ctrl_cache_reg
    }

    async fn host_ctrl_write(&mut self, bus: &mut impl Bus, value: u32) {
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        Self::assert_host_ctrl_bits(value);

        bus.bp_write32(HOST_CTRL_REG_ADDR, value).await;
        self.host_ctrl_cache_reg = value;
        #[cfg(feature = "bt-hostctrl-cache-sample")]
        self.host_ctrl_sample_hw(bus, "write").await;
    }

    async fn h2b_ring_write(bus: &mut impl Bus, bt_addr: u32, ring_off: u32, data: &[u8]) {
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        Self::assert_ring_span(ring_off, data.len());

        let addr = bt_addr + BTSDIO_OFFSET_HOST_WRITE_BUF + ring_off;
        bus.bp_write(addr, data).await;
    }

    #[cfg(feature = "bt-tx-readback-probe")]
    async fn h2b_ring_read(bus: &mut impl Bus, bt_addr: u32, ring_off: u32, data: &mut [u8]) {
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        Self::assert_ring_span(ring_off, data.len());

        let addr = bt_addr + BTSDIO_OFFSET_HOST_WRITE_BUF + ring_off;
        bus.bp_read(addr, data).await;
    }

    async fn b2h_ring_read(bus: &mut impl Bus, bt_addr: u32, ring_off: u32, data: &mut [u8]) {
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        Self::assert_ring_span(ring_off, data.len());

        let addr = bt_addr + BTSDIO_OFFSET_HOST_READ_BUF + ring_off;
        bus.bp_read(addr, data).await;
    }

    async fn read_ring_pointer(bus: &mut impl Bus, bt_addr: u32, pointer_off: u32) -> u32 {
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        {
            assert!(pointer_off == BTSDIO_OFFSET_HOST2BT_OUT || pointer_off == BTSDIO_OFFSET_BT2HOST_IN);
        }

        let value = bus.bp_read32(bt_addr + pointer_off).await;

        #[cfg(feature = "bt-sharedbus-strict-checks")]
        Self::assert_ring_pointer_value(value);

        value
    }

    async fn write_ring_pointer(bus: &mut impl Bus, bt_addr: u32, pointer_off: u32, value: u32) {
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        {
            assert!(pointer_off == BTSDIO_OFFSET_HOST2BT_IN || pointer_off == BTSDIO_OFFSET_BT2HOST_OUT);
            Self::assert_ring_pointer_value(value);
        }

        bus.bp_write32(bt_addr + pointer_off, value).await;
    }

    #[cfg(feature = "bt-ring-pointer-stable-read")]
    async fn read_ring_pointer_maybe_stable(bus: &mut impl Bus, bt_addr: u32, pointer_off: u32) -> u32 {
        let first = Self::read_ring_pointer(bus, bt_addr, pointer_off).await;
        let second = Self::read_ring_pointer(bus, bt_addr, pointer_off).await;
        if first != second {
            trace!(
                "bt ring pointer unstable off=0x{:x} first=0x{:x} second=0x{:x}",
                pointer_off,
                first,
                second
            );
        }
        second
    }

    #[cfg(not(feature = "bt-ring-pointer-stable-read"))]
    async fn read_ring_pointer_maybe_stable(bus: &mut impl Bus, bt_addr: u32, pointer_off: u32) -> u32 {
        Self::read_ring_pointer(bus, bt_addr, pointer_off).await
    }

    async fn write_ring_pointer_maybe_verify(bus: &mut impl Bus, bt_addr: u32, pointer_off: u32, value: u32) {
        Self::write_ring_pointer(bus, bt_addr, pointer_off, value).await;

        #[cfg(feature = "bt-ring-pointer-write-readback")]
        {
            let addr = bt_addr + pointer_off;
            for _ in 0..4 {
                let got = bus.bp_read32(addr).await;
                #[cfg(feature = "bt-sharedbus-strict-checks")]
                Self::assert_ring_pointer_value(got);
                if got == value {
                    return;
                }
                Timer::after(Duration::from_micros(5)).await;
            }
            warn!(
                "bt ring pointer write/readback mismatch off=0x{:x} value=0x{:x}",
                pointer_off,
                value
            );
        }
    }

    async fn bt_signal_intr(&mut self, bus: &mut impl Bus) {
        #[cfg(feature = "bt-ring-intr-set")]
        {
            self.bt_set_intr(bus).await;
        }

        #[cfg(not(feature = "bt-ring-intr-set"))]
        {
            self.bt_toggle_intr(bus).await;
        }
    }

    pub(crate) async fn init_bluetooth(&mut self, bus: &mut impl Bus, firmware: &[u8]) {
        trace!("init_bluetooth");
        bus.bp_write32(CHIP.bluetooth_base_address + BT2WLAN_PWRUP_ADDR, BT2WLAN_PWRUP_WAKE)
            .await;
        Timer::after(Duration::from_millis(2)).await;
        self.upload_bluetooth_firmware(bus, firmware).await;
        self.wait_bt_ready(bus).await;
        self.init_bt_buffers(bus).await;
        self.wait_bt_awake(bus).await;
        self.host_ctrl_sync_from_hw(bus).await;
        self.bt_set_host_ready(bus).await;
        self.bt_signal_intr(bus).await;
    }

    pub(crate) async fn upload_bluetooth_firmware(&mut self, bus: &mut impl Bus, firmware: &[u8]) {
        // read version
        let version_length = firmware[0];
        let _version = &firmware[1..=version_length as usize];
        // skip version + 1 extra byte as per cybt_shared_bus_driver.c
        let firmware = &firmware[version_length as usize + 2..];
        // buffers
        let mut data_buffer: [u8; 0x100] = [0; 0x100];
        let mut aligned_data_buffer: [u8; 0x100] = [0; 0x100];
        // structs
        let mut btfw_cb = CybtFwCb {
            p_next_line_start: firmware,
        };
        let mut hfd = HexFileData {
            addr_mode: BTFW_ADDR_MODE_EXTENDED,
            hi_addr: 0,
            dest_addr: 0,
            p_ds: &mut data_buffer,
        };
        loop {
            let num_fw_bytes = read_firmware_patch_line(&mut btfw_cb, &mut hfd);
            if num_fw_bytes == 0 {
                break;
            }
            let fw_bytes = &hfd.p_ds[0..num_fw_bytes as usize];
            let mut dest_start_addr = hfd.dest_addr + CHIP.bluetooth_base_address;
            let mut aligned_data_buffer_index: usize = 0;
            // pad start
            if !util::is_aligned(dest_start_addr, 4) {
                let num_pad_bytes = dest_start_addr % 4;
                let padded_dest_start_addr = util::round_down(dest_start_addr, 4);
                let memory_value = bus.bp_read32(padded_dest_start_addr).await;
                let memory_value_bytes = memory_value.to_le_bytes();
                // Copy the previous memory value's bytes to the start
                for i in 0..num_pad_bytes as usize {
                    aligned_data_buffer[aligned_data_buffer_index] = memory_value_bytes[i];
                    aligned_data_buffer_index += 1;
                }
                // Copy the firmware bytes after the padding bytes
                for i in 0..num_fw_bytes as usize {
                    aligned_data_buffer[aligned_data_buffer_index] = fw_bytes[i];
                    aligned_data_buffer_index += 1;
                }
                dest_start_addr = padded_dest_start_addr;
            } else {
                // Directly copy fw_bytes into aligned_data_buffer if no start padding is required
                for i in 0..num_fw_bytes as usize {
                    aligned_data_buffer[aligned_data_buffer_index] = fw_bytes[i];
                    aligned_data_buffer_index += 1;
                }
            }
            // pad end
            let mut dest_end_addr = dest_start_addr + aligned_data_buffer_index as u32;
            if !util::is_aligned(dest_end_addr, 4) {
                let offset = dest_end_addr % 4;
                let num_pad_bytes_end = 4 - offset;
                let padded_dest_end_addr = util::round_down(dest_end_addr, 4);
                let memory_value = bus.bp_read32(padded_dest_end_addr).await;
                let memory_value_bytes = memory_value.to_le_bytes();
                // Append the necessary memory bytes to pad the end of aligned_data_buffer
                for i in offset..4 {
                    aligned_data_buffer[aligned_data_buffer_index] = memory_value_bytes[i as usize];
                    aligned_data_buffer_index += 1;
                }
                dest_end_addr += num_pad_bytes_end;
            } else {
                // pad end alignment not needed
            }
            let buffer_to_write = &aligned_data_buffer[0..aligned_data_buffer_index as usize];
            assert!(dest_start_addr % 4 == 0);
            assert!(dest_end_addr % 4 == 0);
            assert!(aligned_data_buffer_index % 4 == 0);
            bus.bp_write(dest_start_addr, buffer_to_write).await;
        }
    }

    pub(crate) async fn wait_bt_ready(&mut self, bus: &mut impl Bus) {
        trace!("wait_bt_ready");
        let mut success = false;
        for _ in 0..300 {
            let val = bus.bp_read32(BT_CTRL_REG_ADDR).await;
            trace!("BT_CTRL_REG_ADDR = {:08x}", val);
            if val & BTSDIO_REG_FW_RDY_BITMASK != 0 {
                success = true;
                break;
            }
            Timer::after(Duration::from_millis(1)).await;
        }
        assert!(success == true);
    }

    pub(crate) async fn wait_bt_awake(&mut self, bus: &mut impl Bus) {
        trace!("wait_bt_awake");
        let mut success = false;
        for _ in 0..300 {
            let val = bus.bp_read32(BT_CTRL_REG_ADDR).await;
            trace!("BT_CTRL_REG_ADDR = {:08x}", val);
            if val & BTSDIO_REG_BT_AWAKE_BITMASK != 0 {
                success = true;
                break;
            }
            Timer::after(Duration::from_millis(1)).await;
        }
        assert!(success == true);
    }

    pub(crate) async fn bt_set_host_ready(&mut self, bus: &mut impl Bus) {
        trace!("bt_set_host_ready");
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
        let old_val = self.host_ctrl_read(bus).await;
        let new_val = old_val | BTSDIO_REG_SW_RDY_BITMASK;
        self.host_ctrl_write(bus, new_val).await;
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
    }

    // TODO: use this
    #[allow(dead_code)]
    pub(crate) async fn bt_set_awake(&mut self, bus: &mut impl Bus, awake: bool) {
        trace!("bt_set_awake");
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
        let old_val = self.host_ctrl_read(bus).await;
        let new_val = if awake {
            old_val | BTSDIO_REG_WAKE_BT_BITMASK
        } else {
            old_val & !BTSDIO_REG_WAKE_BT_BITMASK
        };
        self.host_ctrl_write(bus, new_val).await;
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
    }

    #[allow(dead_code)]
    pub(crate) async fn bt_toggle_intr(&mut self, bus: &mut impl Bus) {
        trace!("bt_toggle_intr");
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
        let old_val = self.host_ctrl_read(bus).await;
        let new_val = old_val ^ BTSDIO_REG_DATA_VALID_BITMASK;
        self.host_ctrl_write(bus, new_val).await;
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
    }

    // TODO: use this
    #[allow(dead_code)]
    pub(crate) async fn bt_set_intr(&mut self, bus: &mut impl Bus) {
        trace!("bt_set_intr");
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
        let old_val = self.host_ctrl_read(bus).await;
        let new_val = old_val | BTSDIO_REG_DATA_VALID_BITMASK;
        self.host_ctrl_write(bus, new_val).await;
        #[cfg(feature = "bt-sharedbus-strict-checks")]
        self.host_ctrl_assert_hw_matches_cache(bus).await;
    }

    pub(crate) async fn init_bt_buffers(&mut self, bus: &mut impl Bus) {
        trace!("init_bt_buffers");
        self.addr = bus.bp_read32(WLAN_RAM_BASE_REG_ADDR).await;
        assert!(self.addr != 0);
        trace!("wlan_ram_base_addr = {:08x}", self.addr);
        bus.bp_write32(self.addr + BTSDIO_OFFSET_HOST2BT_IN, 0).await;
        bus.bp_write32(self.addr + BTSDIO_OFFSET_HOST2BT_OUT, 0).await;
        bus.bp_write32(self.addr + BTSDIO_OFFSET_BT2HOST_IN, 0).await;
        bus.bp_write32(self.addr + BTSDIO_OFFSET_BT2HOST_OUT, 0).await;
    }

    async fn bt_bus_request(&mut self, bus: &mut impl Bus) {
        // TODO: CYW43_THREAD_ENTER mutex?
        self.bt_set_awake(bus, true).await;
        self.wait_bt_awake(bus).await;
    }

    pub(crate) async fn hci_write(&mut self, bus: &mut impl Bus) {
        self.bt_bus_request(bus).await;

        // NOTE(unwrap): we only call this when we do have a packet in the queue.
        let buf = self.tx_chan.try_receive().unwrap();
        #[cfg(feature = "bt-tx-repro-diag")]
        let repro_diag = inspect_repro_hci_packet(&buf.buf[..buf.len]);
        #[cfg(feature = "bt-tx-repro-diag")]
        if let Some(diag) = repro_diag {
            tx_diag_record_runner(diag);
        }
        debug!("HCI tx: {:02x}", crate::fmt::Bytes(&buf.buf[..buf.len]));

        let len = buf.len as u32 - 1; // len doesn't include hci type byte
        let rounded_len = round_up(len, 4);
        let total_len = 4 + rounded_len;
        #[cfg(feature = "bt-tx-readback-probe")]
        let ring_write_start = self.h2b_write_pointer;

        let read_pointer = Self::read_ring_pointer_maybe_stable(bus, self.addr, BTSDIO_OFFSET_HOST2BT_OUT).await;
        let available = read_pointer.wrapping_sub(self.h2b_write_pointer + 4) % BTSDIO_FWBUF_SIZE;
        if available < total_len {
            warn!(
                "bluetooth tx queue full, retrying. len {} available {}",
                total_len, available
            );
            yield_now().await;
            return;
        }

        // Build header
        let mut header = [0u8; 4];
        header[0] = len as u8;
        header[1] = (len >> 8) as u8;
        header[2] = (len >> 16) as u8;
        header[3] = buf.buf[0]; // HCI type byte

        // Write header
        Self::h2b_ring_write(bus, self.addr, self.h2b_write_pointer, &header).await;
        self.h2b_write_pointer = (self.h2b_write_pointer + 4) % BTSDIO_FWBUF_SIZE;

        // Write payload.
        let payload = &buf.buf[1..][..rounded_len as usize];
        if self.h2b_write_pointer as usize + payload.len() > BTSDIO_FWBUF_SIZE as usize {
            // wraparound
            let n = BTSDIO_FWBUF_SIZE - self.h2b_write_pointer;
            Self::h2b_ring_write(bus, self.addr, self.h2b_write_pointer, &payload[..n as usize])
                .await;
            Self::h2b_ring_write(bus, self.addr, 0, &payload[n as usize..]).await;
        } else {
            // no wraparound
            Self::h2b_ring_write(bus, self.addr, self.h2b_write_pointer, payload).await;
        }
        self.h2b_write_pointer = (self.h2b_write_pointer + payload.len() as u32) % BTSDIO_FWBUF_SIZE;

        #[cfg(feature = "bt-tx-readback-probe")]
        if let Some(diag) = repro_diag {
            Self::tx_readback_probe(bus, self.addr, ring_write_start, &header, payload, diag).await;
        }

        // Update pointer.
        #[cfg(feature = "bt-ring-tx-publish-delay-10us")]
        Timer::after(Duration::from_micros(BT_RING_TX_PUBLISH_DELAY_US)).await;
        Self::write_ring_pointer_maybe_verify(bus, self.addr, BTSDIO_OFFSET_HOST2BT_IN, self.h2b_write_pointer).await;

        #[cfg(feature = "bt-ring-tx-intr-delay-10us")]
        Timer::after(Duration::from_micros(BT_RING_TX_INTR_DELAY_US)).await;
        // Free-fn form (not self.bt_signal_intr): `buf` holds a &mut borrow of
        // self via tx_chan that lives until receive_done() below, so we can't
        // take a second &mut self here. The free fn toggles DATA_VALID through
        // `bus` only. (Diag commit used self.bt_signal_intr against the 0.6.0
        // API where this borrow didn't exist; ported to 0.7.0 accordingly.)
        bt_toggle_intr(bus).await;

        buf.receive_done();
    }

    #[cfg(feature = "bt-tx-readback-probe")]
    async fn tx_readback_probe(
        bus: &mut impl Bus,
        bt_addr: u32,
        ring_write_start: u32,
        header: &[u8; 4],
        payload: &[u8],
        diag: ReproDiag,
    ) {
        let total_len = 4 + payload.len();
        if total_len > BT_HCI_MTU + 4 {
            return;
        }

        let mut expected = [0u8; BT_HCI_MTU + 4];
        expected[..4].copy_from_slice(header);
        expected[4..total_len].copy_from_slice(payload);

        let mut actual = [0u8; BT_HCI_MTU + 4];
        let mut ptr = ring_write_start;
        let mut copied = 0usize;
        while copied < total_len {
            let to_wrap = (BTSDIO_FWBUF_SIZE - ptr) as usize;
            let n = core::cmp::min(total_len - copied, to_wrap);
            Self::h2b_ring_read(bus, bt_addr, ptr, &mut actual[copied..copied + n]).await;
            copied += n;
            ptr = (ptr + n as u32) % BTSDIO_FWBUF_SIZE;
        }

        let checked = TX_DIAG_READBACK_CHECKED
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);

        let mut mismatches = 0u16;
        let mut first_offset = 0u16;
        let mut first_expected = 0u8;
        let mut first_actual = 0u8;
        let mut first_set = false;
        for offset in 0..total_len {
            let exp = expected[offset];
            let got = actual[offset];
            if exp != got {
                mismatches = mismatches.wrapping_add(1);
                if !first_set {
                    first_set = true;
                    first_offset = offset as u16;
                    first_expected = exp;
                    first_actual = got;
                }
            }
        }

        if mismatches != 0 {
            let bad = TX_DIAG_READBACK_BAD.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
            let in_header = first_offset < 4;
            let payload_off = first_offset.saturating_sub(4);
            warn!(
                "[txdiag bt-write-readback] seq={} off={} payload_off={} header={} got={:02x} exp={:02x} mismatches={} bad={}/{}",
                diag.seq,
                first_offset,
                payload_off,
                in_header,
                first_actual,
                first_expected,
                mismatches,
                bad,
                checked
            );
        } else if checked % 1024 == 0 {
            info!(
                "[txdiag bt-write-readback] checked={} bad={}",
                checked,
                TX_DIAG_READBACK_BAD.load(Ordering::Relaxed)
            );
        }
    }

    async fn bt_has_work(&mut self, bus: &mut impl Bus) -> bool {
        let int_status = bus.bp_read32(CHIP.sdiod_core_base_address + SDIO_INT_STATUS).await;
        if int_status & I_HMB_FC_CHANGE != 0 {
            bus.bp_write32(
                CHIP.sdiod_core_base_address + SDIO_INT_STATUS,
                int_status & I_HMB_FC_CHANGE,
            )
            .await;
            return true;
        }
        return false;
    }

    #[cfg(feature = "bt-rx-sentinel-probe")]
    fn scan_sentinel_blocks(payload: &[u8]) -> (u32, u32, Option<(usize, [u8; 8])>) {
        const BLOCK_LEN: usize = 16;
        if payload.len() < BLOCK_LEN {
            return (0, 0, None);
        }

        let mut hits = 0u32;
        let mut mismatches = 0u32;
        let mut first_mismatch: Option<(usize, [u8; 8])> = None;

        for i in 0..=payload.len() - BLOCK_LEN {
            if payload[i..i + 8] == SENTINEL_HEAD {
                hits = hits.wrapping_add(1);
                if payload[i + 8..i + 16] != SENTINEL_TAIL {
                    mismatches = mismatches.wrapping_add(1);
                    if first_mismatch.is_none() {
                        let mut got_tail = [0u8; 8];
                        got_tail.copy_from_slice(&payload[i + 8..i + 16]);
                        first_mismatch = Some((i, got_tail));
                    }
                }
            }
        }

        (hits, mismatches, first_mismatch)
    }

    #[cfg(feature = "bt-rx-byte-probe")]
    async fn probe_bulk_vs_single_byte(
        bus: &mut impl Bus,
        bt_read_buf_addr: u32,
        payload_start_ptr: u32,
        payload: &[u8],
        probe_offset: usize,
    ) -> Option<bool> {
        if payload.len() <= probe_offset {
            return None;
        }

        let ring_off = (payload_start_ptr + probe_offset as u32) % BTSDIO_FWBUF_SIZE;
        let addr = bt_read_buf_addr + ring_off;

        let bulk = payload[probe_offset];
        let single = bus.bp_read8(addr).await;

        if bulk != single {
            warn!(
                "[bt-rx-byte-probe] bulk[{}]={:02x} single={:02x} ring_off=0x{:x}",
                probe_offset,
                bulk,
                single,
                ring_off
            );
            return Some(true);
        }
        Some(false)
    }

    #[cfg(feature = "bt-rx-readback-probe")]
    async fn probe_rx_payload_readback(
        bus: &mut impl Bus,
        bt_addr: u32,
        payload_start_ptr: u32,
        payload: &[u8],
    ) -> Option<(u16, u8, u8, u16, u32)> {
        let mut ptr = payload_start_ptr;
        let mut compared = 0usize;
        let mut scratch = [0u8; 64];

        let mut mismatches = 0u16;
        let mut first_offset = 0u16;
        let mut first_expected = 0u8;
        let mut first_actual = 0u8;
        let mut first_ring_off = 0u32;
        let mut first_set = false;

        while compared < payload.len() {
            let to_wrap = (BTSDIO_FWBUF_SIZE - ptr) as usize;
            let n = core::cmp::min(core::cmp::min(payload.len() - compared, to_wrap), scratch.len());
            Self::b2h_ring_read(bus, bt_addr, ptr, &mut scratch[..n]).await;

            for i in 0..n {
                let actual = scratch[i];
                let expected = payload[compared + i];
                if actual != expected {
                    mismatches = mismatches.wrapping_add(1);
                    if !first_set {
                        first_set = true;
                        first_offset = (compared + i) as u16;
                        first_expected = expected;
                        first_actual = actual;
                        first_ring_off = (ptr + i as u32) % BTSDIO_FWBUF_SIZE;
                    }
                }
            }

            compared += n;
            ptr = (ptr + n as u32) % BTSDIO_FWBUF_SIZE;
        }

        if mismatches == 0 {
            None
        } else {
            Some((first_offset, first_expected, first_actual, mismatches, first_ring_off))
        }
    }

    #[cfg(feature = "bt-rx-readback-probe")]
    fn probe_rx_packet_meta(hci_packet_type: u8, payload_valid: &[u8]) -> (u16, u8, u16, u32) {
        let mut acl_handle = 0xffff;
        let mut acl_pb = 0xff;
        let mut l2cap_cid = 0xffff;
        let mut repro_seq = u32::MAX;

        if hci_packet_type != HCI_PACKET_TYPE_ACL || payload_valid.len() < 4 {
            return (acl_handle, acl_pb, l2cap_cid, repro_seq);
        }

        let acl_flags = u16::from_le_bytes([payload_valid[0], payload_valid[1]]);
        acl_handle = acl_flags & 0x0fff;
        acl_pb = ((acl_flags >> 12) & 0x3) as u8;

        let acl_len_decl = u16::from_le_bytes([payload_valid[2], payload_valid[3]]) as usize;
        let acl_len_avail = payload_valid.len().saturating_sub(4);
        let acl_len = acl_len_decl.min(acl_len_avail);
        if acl_len < 4 {
            return (acl_handle, acl_pb, l2cap_cid, repro_seq);
        }

        let acl_payload = &payload_valid[4..4 + acl_len];
        let l2cap_len = u16::from_le_bytes([acl_payload[0], acl_payload[1]]) as usize;
        l2cap_cid = u16::from_le_bytes([acl_payload[2], acl_payload[3]]);
        if acl_payload.len() < 4 + l2cap_len || l2cap_len < 2 {
            return (acl_handle, acl_pb, l2cap_cid, repro_seq);
        }

        let l2cap_payload = &acl_payload[4..4 + l2cap_len];
        let sdu_len = u16::from_le_bytes([l2cap_payload[0], l2cap_payload[1]]) as usize;
        if sdu_len != REPRO_TEST_PACKET_LEN || l2cap_payload.len() < 2 + sdu_len {
            return (acl_handle, acl_pb, l2cap_cid, repro_seq);
        }

        let sdu = &l2cap_payload[2..2 + sdu_len];
        if sdu.len() == REPRO_TEST_PACKET_LEN && sdu[..8] == REPRO_MAGIC {
            repro_seq = u16::from_le_bytes([sdu[8], sdu[9]]) as u32;
        }

        (acl_handle, acl_pb, l2cap_cid, repro_seq)
    }

    pub(crate) async fn handle_irq(&mut self, bus: &mut impl Bus) {
        if self.bt_has_work(bus).await {
            loop {
                // Check if we have data.
                let write_pointer = Self::read_ring_pointer_maybe_stable(bus, self.addr, BTSDIO_OFFSET_BT2HOST_IN).await;
                let available = write_pointer.wrapping_sub(self.b2h_read_pointer) % BTSDIO_FWBUF_SIZE;
                if available == 0 {
                    break;
                }

                // read header
                let mut header = [0u8; 4];
                Self::b2h_ring_read(bus, self.addr, self.b2h_read_pointer, &mut header).await;

                // calc length
                let len = header[0] as u32 | ((header[1]) as u32) << 8 | ((header[2]) as u32) << 16;
                let rounded_len = round_up(len, 4);
                if available < 4 + rounded_len {
                    warn!("ringbuf data not enough for a full packet?");
                    break;
                }
                self.b2h_read_pointer = (self.b2h_read_pointer + 4) % BTSDIO_FWBUF_SIZE;

                // Obtain a buf from the channel.
                let mut buf = self.rx_chan.send().await;
                #[cfg(any(
                    feature = "bt-rx-byte-probe",
                    feature = "bt-rx-sentinel-probe",
                    feature = "bt-rx-readback-probe"
                ))]
                let bt_read_buf_addr = self.addr + BTSDIO_OFFSET_HOST_READ_BUF;

                let hci_packet_type = header[3];
                buf.buf[0] = hci_packet_type; // hci packet type
                let payload = &mut buf.buf[1..][..rounded_len as usize];
                let payload_start_ptr = self.b2h_read_pointer;
                if payload_start_ptr as usize + payload.len() > BTSDIO_FWBUF_SIZE as usize {
                    // wraparound
                    let n = BTSDIO_FWBUF_SIZE - payload_start_ptr;
                    Self::b2h_ring_read(bus, self.addr, payload_start_ptr, &mut payload[..n as usize])
                        .await;
                    Self::b2h_ring_read(bus, self.addr, 0, &mut payload[n as usize..]).await;
                } else {
                    // no wraparound
                    Self::b2h_ring_read(bus, self.addr, payload_start_ptr, payload).await;
                }
                #[cfg(any(
                    feature = "bt-rx-byte-probe",
                    feature = "bt-rx-sentinel-probe",
                    feature = "bt-rx-readback-probe"
                ))]
                let payload_valid = &payload[..len as usize];

                #[cfg(feature = "bt-rx-readback-probe")]
                {
                    self.rx_readback_packets = self.rx_readback_packets.wrapping_add(1);
                    const PROBE_SAMPLE_MASK: u32 = 0x1f; // sample 1/32 packets

                    if (self.rx_readback_packets & PROBE_SAMPLE_MASK) == 0 {
                        self.rx_readback_sampled = self.rx_readback_sampled.wrapping_add(1);
                        if let Some((first_off, first_exp, first_got, mismatches, ring_off)) =
                            Self::probe_rx_payload_readback(bus, self.addr, payload_start_ptr, payload).await
                        {
                            let (acl_handle, acl_pb, l2cap_cid, repro_seq) =
                                Self::probe_rx_packet_meta(hci_packet_type, payload_valid);
                            self.rx_readback_mismatch_packets = self.rx_readback_mismatch_packets.wrapping_add(1);
                            self.rx_readback_mismatch_bytes =
                                self.rx_readback_mismatch_bytes.wrapping_add(mismatches as u32);
                            let single = bus.bp_read8(bt_read_buf_addr + ring_off).await;

                            warn!(
                                "[bt-rx-readback-probe] hci_type={} acl_handle={} pb={} cid=0x{:x} repro_seq={} len={} rounded={} off={} ring_off=0x{:x} got={:02x} exp={:02x} xor={:02x} single={:02x} mismatches={} bad={}/{}",
                                hci_packet_type,
                                acl_handle,
                                acl_pb,
                                l2cap_cid,
                                repro_seq,
                                len,
                                rounded_len,
                                first_off,
                                ring_off,
                                first_got,
                                first_exp,
                                first_got ^ first_exp,
                                single,
                                mismatches,
                                self.rx_readback_mismatch_packets,
                                self.rx_readback_sampled
                            );
                        }
                    }

                    if self.rx_readback_packets % 2048 == 0 {
                        info!(
                            "[bt-rx-readback-probe] pkts={} sample=1/32 sampled={} bad={} bad_bytes={}",
                            self.rx_readback_packets,
                            self.rx_readback_sampled,
                            self.rx_readback_mismatch_packets,
                            self.rx_readback_mismatch_bytes
                        );
                    }
                }

                #[cfg(feature = "bt-rx-sentinel-probe")]
                {
                    self.sentinel_packets = self.sentinel_packets.wrapping_add(1);
                    // Only stitch across true ACL continuation fragments on the same handle.
                    // This avoids false matches that include the next packet's ACL header bytes.
                    if hci_packet_type == HCI_PACKET_TYPE_ACL && payload_valid.len() >= 4 {
                        let acl_flags = u16::from_le_bytes([payload_valid[0], payload_valid[1]]);
                        let handle = acl_flags & 0x0fff;
                        let pb = (acl_flags >> 12) & 0x3;
                        let is_cont = pb == ACL_PB_CONTINUING;
                        let acl_len = u16::from_le_bytes([payload_valid[2], payload_valid[3]]) as usize;
                        let acl_avail = payload_valid.len().saturating_sub(4);
                        let acl_len = acl_len.min(acl_avail);
                        let acl_data = &payload_valid[4..4 + acl_len];

                        if !is_cont || !self.sentinel_carry_active || self.sentinel_carry_handle != handle {
                            self.sentinel_carry_len = 0;
                        }

                        let carry_len = self.sentinel_carry_len as usize;
                        let mut scan_buf = [0u8; BT_HCI_MTU + 15];
                        scan_buf[..carry_len].copy_from_slice(&self.sentinel_carry[..carry_len]);
                        let total_len = carry_len + acl_data.len();
                        scan_buf[carry_len..total_len].copy_from_slice(acl_data);

                        let scan_slice = &scan_buf[..total_len];
                        let (hits, mismatches, first_mismatch) = Self::scan_sentinel_blocks(scan_slice);
                        self.sentinel_blocks = self.sentinel_blocks.wrapping_add(hits);
                        self.sentinel_mismatch = self.sentinel_mismatch.wrapping_add(mismatches);

                        if let Some((offset, got_tail)) = first_mismatch {
                            let cross_boundary = offset < carry_len;
                            let acl_off = offset.saturating_sub(carry_len);
                            let mut tail_idx = 0usize;
                            while tail_idx < SENTINEL_TAIL.len()
                                && got_tail[tail_idx] == SENTINEL_TAIL[tail_idx]
                            {
                                tail_idx += 1;
                            }
                            if tail_idx < SENTINEL_TAIL.len() {
                                let scan_idx = offset + SENTINEL_HEAD.len() + tail_idx;
                                let ring_off = if scan_idx < carry_len {
                                    self.sentinel_carry_ring_off[scan_idx]
                                } else {
                                    let acl_idx = scan_idx - carry_len;
                                    (payload_start_ptr + 4 + acl_idx as u32) % BTSDIO_FWBUF_SIZE
                                };
                                let bulk = got_tail[tail_idx];
                                let single = bus.bp_read8(bt_read_buf_addr + ring_off).await;
                                warn!(
                                    "[bt-rx-sentinel-probe] acl_off={} cross={} tail_idx={} bulk={:02x} single={:02x} ring_off=0x{:x} got_tail={:02x} expected_tail={:02x}",
                                    acl_off,
                                    cross_boundary,
                                    tail_idx,
                                    bulk,
                                    single,
                                    ring_off,
                                    crate::fmt::Bytes(&got_tail),
                                    crate::fmt::Bytes(&SENTINEL_TAIL)
                                );
                            } else {
                                warn!(
                                    "[bt-rx-sentinel-probe] acl_off={} cross={} got_tail={:02x} expected_tail={:02x}",
                                    acl_off,
                                    cross_boundary,
                                    crate::fmt::Bytes(&got_tail),
                                    crate::fmt::Bytes(&SENTINEL_TAIL)
                                );
                            }
                        }

                        let new_carry_len = total_len.min(self.sentinel_carry.len());
                        let carry_start = total_len - new_carry_len;
                        for i in 0..new_carry_len {
                            let src_idx = carry_start + i;
                            self.sentinel_carry[i] = scan_slice[src_idx];
                            self.sentinel_carry_ring_off[i] = if src_idx < carry_len {
                                self.sentinel_carry_ring_off[src_idx]
                            } else {
                                let acl_idx = src_idx - carry_len;
                                (payload_start_ptr + 4 + acl_idx as u32) % BTSDIO_FWBUF_SIZE
                            };
                        }
                        self.sentinel_carry_len = new_carry_len as u8;
                        self.sentinel_carry_handle = handle;
                        self.sentinel_carry_active = true;
                    } else {
                        self.sentinel_carry_len = 0;
                        self.sentinel_carry_active = false;
                    }

                    if self.sentinel_packets % 2048 == 0 {
                        info!(
                            "[bt-rx-sentinel-probe] pkts={} blocks={} mismatches={}",
                            self.sentinel_packets,
                            self.sentinel_blocks,
                            self.sentinel_mismatch
                        );
                    }
                }

                #[cfg(feature = "bt-rx-byte-probe")]
                {
                    self.probe_packets = self.probe_packets.wrapping_add(1);
                    const PROBE_SAMPLE_MASK: u32 = 0x3f; // sample 1/64 packets

                    if (self.probe_packets & PROBE_SAMPLE_MASK) == 0 {
                        // Candidate offsets for the dominant corruption position:
                        // - 4: continuation byte 0 (if IPHC header is fully elided)
                        // - 9: continuation byte 5 (if IPHC header is ~35 bytes)
                        match Self::probe_bulk_vs_single_byte(
                            bus,
                            bt_read_buf_addr,
                            payload_start_ptr,
                            payload_valid,
                            4,
                        )
                        .await
                        {
                            Some(true) => {
                                self.probe_checked_o4 = self.probe_checked_o4.wrapping_add(1);
                                self.probe_mismatch_o4 = self.probe_mismatch_o4.wrapping_add(1);
                            }
                            Some(false) => {
                                self.probe_checked_o4 = self.probe_checked_o4.wrapping_add(1);
                            }
                            None => {}
                        }

                        match Self::probe_bulk_vs_single_byte(
                            bus,
                            bt_read_buf_addr,
                            payload_start_ptr,
                            payload_valid,
                            9,
                        )
                        .await
                        {
                            Some(true) => {
                                self.probe_checked_o9 = self.probe_checked_o9.wrapping_add(1);
                                self.probe_mismatch_o9 = self.probe_mismatch_o9.wrapping_add(1);
                            }
                            Some(false) => {
                                self.probe_checked_o9 = self.probe_checked_o9.wrapping_add(1);
                            }
                            None => {}
                        }
                    }

                    if self.probe_packets % 2048 == 0 {
                        info!(
                            "[bt-rx-byte-probe] pkts={} sample=1/64 o4: checked={} mismatches={} o9: checked={} mismatches={}",
                            self.probe_packets,
                            self.probe_checked_o4,
                            self.probe_mismatch_o4,
                            self.probe_checked_o9,
                            self.probe_mismatch_o9
                        );
                    }
                }

                self.b2h_read_pointer = (self.b2h_read_pointer + payload.len() as u32) % BTSDIO_FWBUF_SIZE;
                #[cfg(feature = "bt-ring-rx-out-delay-10us")]
                Timer::after(Duration::from_micros(BT_RING_RX_OUT_DELAY_US)).await;
                Self::write_ring_pointer_maybe_verify(bus, self.addr, BTSDIO_OFFSET_BT2HOST_OUT, self.b2h_read_pointer)
                    .await;

                buf.len = 1 + len as usize;
                debug!("HCI rx: {:02x}", crate::fmt::Bytes(&buf.buf[..buf.len]));

                buf.send_done();

                #[cfg(feature = "bt-ring-rx-intr-delay-10us")]
                Timer::after(Duration::from_micros(BT_RING_RX_INTR_DELAY_US)).await;
                self.bt_signal_intr(bus).await;
            }
        }
    }
}

/// HCI transport error.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug)]
pub enum Error {
    /// I/O error.
    Io(ErrorKind),
}

impl From<FromHciBytesError> for Error {
    fn from(e: FromHciBytesError) -> Self {
        match e {
            FromHciBytesError::InvalidSize => Error::Io(ErrorKind::InvalidInput),
            FromHciBytesError::InvalidValue => Error::Io(ErrorKind::InvalidData),
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self, f)
    }
}

impl core::error::Error for Error {}

impl<'d> embedded_io_async::ErrorType for BtDriver<'d> {
    type Error = Error;
}

impl embedded_io_async::Error for Error {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Io(e) => *e,
        }
    }
}

impl<'d> bt_hci::transport::Transport for BtDriver<'d> {
    fn read<'a>(&self, rx: &'a mut [u8]) -> impl Future<Output = Result<ControllerToHostPacket<'a>, Self::Error>> {
        async {
            let mut ch = self.rx.lock().await;
            let buf = ch.receive().await;
            let n = buf.len;
            assert!(n < rx.len());
            rx[..n].copy_from_slice(&buf.buf[..n]);
            buf.receive_done();

            let kind = PacketKind::from_hci_bytes_complete(&rx[..1])?;
            let (pkt, _) = ControllerToHostPacket::from_hci_bytes_with_kind(kind, &rx[1..n])?;
            Ok(pkt)
        }
    }

    /// Write a complete HCI packet from the tx buffer
    fn write<T: HostToControllerPacket>(&self, val: &T) -> impl Future<Output = Result<(), Self::Error>> {
        async {
            let mut ch = self.tx.lock().await;
            let mut buf = ch.send().await;
            let buf_len = buf.buf.len();
            let mut slice = &mut buf.buf[..];
            WithIndicator::new(val)
                .write_hci(&mut slice)
                .map_err(|_| Error::Io(ErrorKind::Other))?;
            buf.len = buf_len - slice.len();
            #[cfg(feature = "bt-tx-repro-diag")]
            if let Some(diag) = inspect_repro_hci_packet(&buf.buf[..buf.len]) {
                tx_diag_record_driver(diag);
            }
            buf.send_done();
            Ok(())
        }
    }
}
