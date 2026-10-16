# Testing Guide

## Running Tests
Run the test suite with pytest:
```powershell
python -m pytest -v
```

To exclude slow tests:
```powershell
python -m pytest -m "not slow" -v
```

## Test Structure
- `tests/test_packet.py`: Packet encoding, decoding, field validation, bit-flip corruption detection, boundary conditions, and fuzz testing.
- Future phases will add handshake, selective repeat, RTO, flow control, and full transfer integration suites.
