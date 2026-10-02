#!/usr/bin/env python3
"""Generate a Workspace-like test tree with about N entries (files, dirs, symlinks).

Usage: gen_tree.py DIR N SEED
Writes DIR/../<name>.manifest.json listing public single-link files, public
hard-link pairs and directories for the driver.
"""
import json
import os
import random
import sys

root, total, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rng = random.Random(seed)
os.umask(0o022)
os.makedirs(root)
entries = 0
dirs = []
single = []
pairs = []


def mkdir(p, mode=0o755):
    global entries
    os.mkdir(os.path.join(root, p))
    os.chmod(os.path.join(root, p), mode)
    entries += 1


def mkfile(p, mode=0o644, size=None):
    global entries
    size = rng.randint(32, 512) if size is None else size
    full = os.path.join(root, p)
    with open(full, "wb") as f:
        f.write(rng.randbytes(size))
    os.chmod(full, mode)
    entries += 1


# Private state at several depths, the hidden .venv, NFS silly-rename names.
for d in [".forge", ".forge/sessions", ".plori-trash", ".venv", ".venv/lib"]:
    mkdir(d, 0o700 if d.startswith(".plori-trash") else 0o755)
for i in range(200):
    mkfile(f".forge/sessions/s{i:04d}.json")
for i in range(50):
    mkfile(f".plori-trash/old-{i:03d}")
for i in range(1000):
    mkfile(f".venv/lib/m{i:04d}.py")
mkfile(".config")
# Private-only hard-link groups (never a public name).
for i in range(20):
    mkfile(f".forge/sessions/p{i:03d}")
    os.link(os.path.join(root, f".forge/sessions/p{i:03d}"), os.path.join(root, f".forge/q{i:03d}"))
    entries += 1

# Public tree: fan-out 8 directories, about 11 files per directory.
frontier = [""]
target_dirs = max(8, total // 12)
while len(dirs) < target_dirs:
    parent = frontier.pop(0)
    for k in range(8):
        if len(dirs) >= target_dirs:
            break
        p = os.path.join(parent, f"d{len(dirs):05d}") if parent else f"d{len(dirs):05d}"
        mkdir(p)
        dirs.append(p)
        frontier.append(p)
    if not frontier:
        frontier = list(dirs)
# Nested private names inside public directories (excluded at every depth).
for d in rng.sample(dirs, 10):
    mkdir(f"{d}/.trash")
    mkfile(f"{d}/.trash/t")
    mkfile(f"{d}/.nfs000{rng.randint(1000, 9999)}")
i = 0
while entries < total - 400:
    d = dirs[i % len(dirs)]
    p = f"{d}/f{i:06d}.txt"
    mode = rng.choice([0o644, 0o644, 0o644, 0o600, 0o755])
    mkfile(p, mode)
    single.append(p)
    i += 1
# Public hard-link pairs across directories, symlinks, xattrs.
for k in range(100):
    a, b = rng.sample(dirs, 2)
    pa, pb = f"{a}/pair{k:03d}a", f"{b}/pair{k:03d}b"
    mkfile(pa)
    os.link(os.path.join(root, pa), os.path.join(root, pb))
    entries += 1
    pairs.append([pa, pb])
for k in range(150):
    d = rng.choice(dirs)
    os.symlink(f"../{os.path.basename(d)}", os.path.join(root, d, f"link{k:03d}"))
    entries += 1
for p in rng.sample(single, 50):
    os.setxattr(os.path.join(root, p), "user.plori", p.encode())
    single.remove(p)
manifest = {"entries": entries, "dirs": dirs, "single": single, "pairs": pairs}
with open(root.rstrip("/") + ".manifest.json", "w") as f:
    json.dump(manifest, f)
print(json.dumps({"entries": entries, "dirs": len(dirs), "single": len(single), "pairs": len(pairs)}))
