# dummy-edge

Test-harness **edge target** for the [proxy](../proxy): a fake device exposing all
three transports with one channel per message type, auto-generating the paired confirm.

| Channel (this device)        | Receives          | Pushes back (to the proxy)            |
| ---------------------------- | ----------------- | ------------------------------------- |
| HTTP `POST /pick-tasks`      | WAVE_RELEASE      | PICK_CONFIRM → `POST {proxy}/pick-confirm` |
| TCP port `9001`              | CONTAINER_PUTAWAY | PUTAWAY_CONFIRM → proxy TCP `6001`    |
| FTP folder `cycle-count` (server on `2222`) | CYCLE_COUNT_REQ | CYCLE_COUNT_CONFIRM → proxy FTP folder `cycle-count-confirm` (`2221`) |

`GET /received` lists everything the device received. Confirms are pushed after a small
simulated delay and retried a few times if the proxy nacks — like a real device.

Run from the repo root: `just run-dummy-edge` (port **8092**). Targets overridable via
`SERVER_PORT`, `EDGE_PROXY_HTTPBASE`, etc.
