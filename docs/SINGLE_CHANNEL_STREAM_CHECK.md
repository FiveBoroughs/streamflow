# Single Channel Stream Check

Use this API call to run an immediate stream check for one Dispatcharr channel:

```bash
curl -X POST http://localhost:5000/api/stream-checker/check-single-channel \
  -H 'Content-Type: application/json' \
  -d '{"channel_id":176}'
```

Replace `176` with the real Dispatcharr channel ID.

Notes:
- This is different from the visible channel number. For example, the UFC channel had channel number `0`, but its real channel ID was `176`.
- The call can take several minutes because it refreshes relevant data and probes the channel streams.
- While it runs, progress is available at:

```bash
curl http://localhost:5000/api/stream-checker/progress
```

You can also check overall status with:

```bash
curl http://localhost:5000/api/stream-checker/status
```
