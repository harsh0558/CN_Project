import sys
P = "rtp/sender.py"
EDITS = [
('''DEFAULT_SR_WINDOW: Final[int] = 16''',
 '''DEFAULT_SR_WINDOW: Final[int] = 64'''),

('''        self.max_in_flight: int = 0
''',
 '''        self.max_in_flight: int = 0
        self._in_flight: int = 0   # packets currently IN_FLIGHT (O(1) bookkeeping)
'''),

('''    def _check_window_invariants(self) -> None:
        """Record any in-flight packet whose sequence number violates right_edge."""
        for p in self.packets.values():
            if p.state == PacketState.IN_FLIGHT:
                if p.seq >= self.right_edge:
                    self.window_violations.append((p.seq, self.send_base, self.right_edge))
''',
 '''    def _check_window_invariants(self, p: InFlightPacket) -> None:
        """Record a violation if p is transmitted beyond right_edge.

        right_edge never decreases, so checking at transmit time is sufficient.
        """
        if p.seq >= self.right_edge:
            self.window_violations.append((p.seq, self.send_base, self.right_edge))

    def _mark_acked(self, p: InFlightPacket) -> None:
        """Mark an IN_FLIGHT packet ACKED and keep the in-flight counter exact."""
        p.state = PacketState.ACKED
        p.deadline = 0.0
        self._in_flight -= 1
'''),

('''        t = now - self._start_time
        in_flight = sum(
            1 for p in self.packets.values() if p.state == PacketState.IN_FLIGHT
        )''',
 '''        t = now - self._start_time
        in_flight = self._in_flight'''),

('''        packet_info.deadline = now + self.current_rto
        packet_info.state = PacketState.IN_FLIGHT
        self._check_window_invariants()
''',
 '''        packet_info.deadline = now + self.current_rto
        if packet_info.state != PacketState.IN_FLIGHT:
            self._in_flight += 1
        packet_info.state = PacketState.IN_FLIGHT
        self._check_window_invariants(packet_info)
'''),

('''        in_flight = sum(
            1 for p in self.packets.values() if p.state == PacketState.IN_FLIGHT
        )
        if in_flight > self.max_in_flight:
            self.max_in_flight = in_flight''',
 '''        if self._in_flight > self.max_in_flight:
            self.max_in_flight = self._in_flight'''),

('''        newly_acked = 0
        saw_ss_exit = False''',
 '''        newly_acked = 0
        saw_ss_exit = False
        old_base = self.send_base'''),

('''            pkt_slot.state = PacketState.ACKED
            pkt_slot.deadline = 0.0
''',
 '''            self._mark_acked(pkt_slot)
'''),

('''                        self.packets[sack_seq].state = PacketState.ACKED
                        self.packets[sack_seq].deadline = 0.0
''',
 '''                        self._mark_acked(self.packets[sack_seq])
'''),

('''                    self.packets[s].state = PacketState.ACKED
                    self.packets[s].deadline = 0.0
''',
 '''                    self._mark_acked(self.packets[s])
'''),

('''        for s in list(self.packets.keys()):
            if s < self.send_base:
                del self.packets[s]
''',
 '''        for s in range(old_base, self.send_base):
            self.packets.pop(s, None)
'''),

('''        self._check_window_invariants()
''', ''''''),

('''                elif p.retry_count > self.loss_epoch_retry:
                    self.loss_epoch_end = self.next_seq_num - 1
                    self.loss_epoch_retry = p.retry_count
                    self.rto_estimator.on_timeout()
                    self.cc.on_timeout()
                    self._log_cwnd(event="timeout")
''',
 '''                elif p.retry_count > self.loss_epoch_retry:
                    # Same loss epoch, same packet lost again: back off RTO only.
                    # cwnd/ssthresh were already reduced once for this epoch.
                    self.loss_epoch_end = self.next_seq_num - 1
                    self.loss_epoch_retry = p.retry_count
                    self.rto_estimator.on_timeout()
'''),
]
raw = open(P, encoding="utf-8", newline="").read()
crlf = "\r\n" in raw
t = raw.replace("\r\n", "\n")
for i, (old, new) in enumerate(EDITS, 1):
    n = t.count(old)
    if n != 1:
        sys.exit(f"Edit {i} matched {n} times; nothing written. Tell me the number.")
    t = t.replace(old, new)
open(P, "w", encoding="utf-8", newline="").write(t.replace("\n", "\r\n") if crlf else t)
print("applied", len(EDITS), "edits")