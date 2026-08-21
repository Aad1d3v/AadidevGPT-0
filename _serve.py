import os
from app import app

# Use an explicit port for local testing. The ambient PORT=0 env var would
# otherwise make Flask bind to a random port.
port = int(os.environ.get("PORT") or 0)
if port <= 0:
    port = 5000

app.run(
    host="0.0.0.0",
    port=port,
    debug=False,
    use_reloader=False,
)
