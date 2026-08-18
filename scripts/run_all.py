"""Execute every notebook headlessly, in order. `make run-all`.

Each notebook ends in its own `assert` block over its pass criteria, so a
non-zero exit here means a criterion actually failed — this is the same gate
the instructor runs before grading.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"


def main() -> int:
    notebooks = sorted(p for p in NB_DIR.glob("*.py") if not p.name.startswith("_"))
    if not notebooks:
        print("No notebooks found.")
        return 1

    print(f"Running {len(notebooks)} notebooks with {sys.executable}\n")
    failures, total = [], 0.0
    for nb in notebooks:
        t0 = time.perf_counter()
        proc = subprocess.run([sys.executable, str(nb)], capture_output=True, text=True)
        dt = time.perf_counter() - t0
        total += dt
        if proc.returncode == 0:
            print(f"  PASS  {nb.name:<32} {dt:6.1f}s")
        else:
            print(f"  FAIL  {nb.name:<32} {dt:6.1f}s")
            failures.append((nb.name, proc.stdout[-1500:], proc.stderr[-1500:]))

    print(f"\n{len(notebooks) - len(failures)}/{len(notebooks)} passed in {total:.1f}s")
    for name, out, err in failures:
        print(f"\n{'=' * 70}\n{name}\n{'=' * 70}")
        print(out)
        print(err)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
