-- Wireshark dissector for the CN Project 15 RTP header (26 bytes, big-endian)
local rtpu = Proto("rtpu", "Reliable Transport over UDP")
local F = rtpu.fields
F.conn_id  = ProtoField.uint32("rtpu.conn_id", "Connection ID", base.HEX)
F.seq      = ProtoField.uint32("rtpu.seq", "Sequence", base.DEC)
F.ack      = ProtoField.uint32("rtpu.ack", "Ack (next expected)", base.DEC)
F.sack     = ProtoField.uint32("rtpu.sack", "SACK bitmap", base.HEX)
F.rwnd     = ProtoField.uint16("rtpu.rwnd", "Receiver window (packets)", base.DEC)
F.flags    = ProtoField.uint8("rtpu.flags", "Flags", base.HEX)
F.f_syn    = ProtoField.bool("rtpu.flags.syn", "SYN", 8, nil, 0x01)
F.f_ack    = ProtoField.bool("rtpu.flags.ack", "ACK", 8, nil, 0x02)
F.f_fin    = ProtoField.bool("rtpu.flags.fin", "FIN", 8, nil, 0x04)
F.f_data   = ProtoField.bool("rtpu.flags.data", "DATA", 8, nil, 0x08)
F.f_rst    = ProtoField.bool("rtpu.flags.rst", "RST", 8, nil, 0x10)
F.reserved = ProtoField.uint8("rtpu.reserved", "Reserved", base.HEX)
F.length   = ProtoField.uint16("rtpu.length", "Payload length (bytes)", base.DEC)
F.crc32    = ProtoField.uint32("rtpu.crc32", "CRC-32", base.HEX)

local names = { [0x01]="SYN", [0x03]="SYN|ACK", [0x02]="ACK",
                [0x08]="DATA", [0x04]="FIN", [0x06]="FIN|ACK" }

function rtpu.dissector(buf, pinfo, tree)
  if buf:len() < 26 then return 0 end
  pinfo.cols.protocol = "RTPU"
  local t = tree:add(rtpu, buf(0, buf:len()), "RTP Header")
  t:add(F.conn_id,  buf(0, 4))
  t:add(F.seq,      buf(4, 4))
  t:add(F.ack,      buf(8, 4))
  t:add(F.sack,     buf(12, 4))
  t:add(F.rwnd,     buf(16, 2))
  local ft = t:add(F.flags, buf(18, 1))
  ft:add(F.f_syn, buf(18, 1)); ft:add(F.f_ack, buf(18, 1))
  ft:add(F.f_fin, buf(18, 1)); ft:add(F.f_data, buf(18, 1))
  ft:add(F.f_rst, buf(18, 1))
  t:add(F.reserved, buf(19, 1))
  t:add(F.length,   buf(20, 2))
  t:add(F.crc32,    buf(22, 4))
  local fl = buf(18, 1):uint()
  local nm = names[fl] or string.format("0x%02x", fl)
  pinfo.cols.info = string.format("%s seq=%d ack=%d rwnd=%d len=%d", nm,
    buf(4, 4):uint(), buf(8, 4):uint(), buf(16, 2):uint(), buf(20, 2):uint())
  if buf:len() > 26 then t:add(buf(26), "Payload (" .. (buf:len() - 26) .. " bytes)") end
  return buf:len()
end

DissectorTable.get("udp.port"):add(9000, rtpu)
