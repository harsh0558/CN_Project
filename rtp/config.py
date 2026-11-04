"""Protocol configuration parameters, constants, and default settings."""

# Packet and Header constants
HEADER_SIZE: int = 26
HEADER_FORMAT: str = "!IIIIHBBHI"
MAX_PAYLOAD_SIZE: int = 1024

# Packet Flags (Bitfield)
FLAG_SYN: int = 0x01
FLAG_ACK: int = 0x02
FLAG_FIN: int = 0x04
FLAG_DATA: int = 0x08
FLAG_RST: int = 0x10

# Accepted flag combinations explicitly allowed in the RTP protocol:
ACCEPTED_FLAG_COMBINATIONS: dict[int, str] = {
    FLAG_SYN: "SYN (Connection request)",
    FLAG_SYN | FLAG_ACK: "SYN|ACK (Connection response)",
    FLAG_ACK: "ACK (Pure acknowledgment)",
    FLAG_DATA: "DATA (Stream data packet or zero-window probe)",
    FLAG_FIN: "FIN (Connection teardown request)",
    FLAG_FIN | FLAG_ACK: "FIN|ACK (Teardown response with status)",
}

VALID_FLAG_COMBINATIONS: frozenset[int] = frozenset(ACCEPTED_FLAG_COMBINATIONS.keys())

# Window and buffer limits (in packets)
DEFAULT_RECEIVER_CAPACITY: int = 64  # R
DEFAULT_MAX_WINDOW: int = 64         # max_window
INITIAL_CWND: float = 1.0            # cwnd in packets
INITIAL_SSTHRESH: int = 64           # initial ssthresh

# RTO parameters (RFC 6298 adapted)
INITIAL_RTO: float = 1.0             # 1.0 s
MIN_RTO: float = float(__import__("os").environ.get("RTP_MIN_RTO", "0.2"))  # default 0.2 s (deviation from RFC 6298's 1.0 s); env override used only for the follow-up experiment
MAX_RTO: float = 60.0                # 60.0 s
RTO_ALPHA: float = 1.0 / 8.0         # 0.125
RTO_BETA: float = 1.0 / 4.0          # 0.25
RTO_K: int = 4
RTO_G: float = 0.001                 # 1 ms clock granularity

# Connection management limits
MAX_SYN_ATTEMPTS: int = 6
MAX_FIN_ATTEMPTS: int = 6
TIME_WAIT_CAP: float = 2.0           # Cap for min(2 * RTO, 2.0 s)
MAX_CONSECUTIVE_TIMEOUTS: int = 10   # Abort after 10 timeouts of the same packet
IDLE_TIMEOUT: float = 30.0           # Abort after 30 s without valid packets

# Flow control
PERSIST_TIMER_CAP: float = 2.0       # Zero-window probe persist timer cap (s)
PROBE_ECHO_SEQ: int = 0xFFFFFFFF     # Echo sequence number for probe replies and window refreshes

# Ports
DEFAULT_RTP_PORT: int = 9000
DEFAULT_TCP_PORT: int = 9001
DEFAULT_IPERF_PORT: int = 5201

# Application layer constants
DEFAULT_FILE_SIZE: int = 20_000_000  # 20,000,000 bytes (decimal 20 MB)
STATUS_OK: int = 0x00                # Hash matched, verified OK
STATUS_MISMATCH: int = 0x01          # Hash mismatch or incomplete
