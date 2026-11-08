"""Download the two core families (nbastats, shotdetail), seasons 1996-2024, into dataset/.

Source: https://github.com/shufinskiy/nba_data (Apache-2.0). The Kaggle dataset used in the
first analysis is a mirror of the same files. Each season is a .tar.xz archive holding one CSV.

Usage (from the project root):
    python src/download_data.py
Files already present in dataset/ are skipped, so the script can be re-run after an interruption.
"""
import os
import re
import tarfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEST = os.path.join(ROOT, "dataset")
TMP = os.path.join(DEST, "_tmp")
LIST_URL = "https://raw.githubusercontent.com/shufinskiy/nba_data/main/list_data.txt"
PATTERN = re.compile(r"^(nbastats|shotdetail)(_po)?_(\d{4})$")


def main():
    os.makedirs(TMP, exist_ok=True)
    listing = urllib.request.urlopen(LIST_URL).read().decode("utf8")
    todo = []
    for line in listing.splitlines():
        name, _, url = line.strip().partition("=")
        m = PATTERN.match(name)
        if m and 1996 <= int(m.group(3)) <= 2024:
            todo.append((name, url.strip()))
    print("files to fetch:", len(todo), flush=True)

    failed = []
    t0 = time.time()
    for i, (name, url) in enumerate(todo, 1):
        csv_path = os.path.join(DEST, name + ".csv")
        if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
            print(f"[{i}/{len(todo)}] skip {name}", flush=True)
            continue
        try:
            arc = os.path.join(TMP, name + ".tar.xz")
            urllib.request.urlretrieve(url, arc)
            with tarfile.open(arc, "r:xz") as tf:
                members = [m for m in tf.getmembers() if m.isfile() and m.name.endswith(".csv")]
                assert len(members) == 1, (name, [m.name for m in tf.getmembers()])
                src = tf.extractfile(members[0])
                with open(csv_path, "wb") as out:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        out.write(chunk)
            os.remove(arc)
            print(f"[{i}/{len(todo)}] ok {name} {os.path.getsize(csv_path) / 1e6:.1f} MB ({time.time() - t0:.0f}s)", flush=True)
        except Exception as e:  # keep going, report at the end
            failed.append(name)
            print(f"[{i}/{len(todo)}] FAILED {name}: {e}", flush=True)
    print("done. failed:", failed, flush=True)


if __name__ == "__main__":
    main()
