import time
import urllib.error
import urllib.request

deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen("http://localhost:8080/ready", timeout=2) as response:
            if response.status == 200:
                print("Ingestion readiness confirmed.")
                break
    except (urllib.error.URLError, TimeoutError):
        pass
    time.sleep(0.5)
else:
    raise SystemExit("Ingestion not ready within 120s")
