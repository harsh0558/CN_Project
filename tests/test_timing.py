import json
import random
import socket
import threading
import time

from app.file_receiver import FileReceiver
from app.file_sender import FileSender


def test_duration_excludes_time_wait(tmp_path):
    src = tmp_path / "in.bin"
    src.write_bytes(random.Random(2).randbytes(100_000))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    rx = FileReceiver(("127.0.0.1", port), output_dir=tmp_path / "out", timeout=20)
    th = threading.Thread(target=rx.run_once)
    th.start()
    time.sleep(0.2)
    tx = FileSender(("127.0.0.1", port), timeout=20, run_dir=tmp_path / "run")
    t0 = time.monotonic()
    outcome, _ = tx.send_file(src)
    wall = time.monotonic() - t0
    th.join(30)
    d = json.loads((tmp_path / "run" / "sender_summary.json").read_text())
    assert outcome == "success"
    # TIME_WAIT is at least 2 * MIN_RTO = 0.4 s and must not be counted
    assert wall - d["duration_s"] > 0.3
