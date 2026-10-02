#!/usr/bin/env python3
"""Compare two restored trees: names, type, mode, bytes, symlink targets,
user xattrs and hard-link groups (names sharing an inode)."""
import hashlib, json, os, stat, sys

def scan(root):
    out, inodes = {}, {}
    for dp, dns, fns in os.walk(root, followlinks=False):
        for name in dns + fns:
            full = os.path.join(dp, name)
            rel = os.path.relpath(full, root)
            st = os.lstat(full)
            kind = "dir" if stat.S_ISDIR(st.st_mode) else "symlink" if stat.S_ISLNK(st.st_mode) else "file"
            rec = {"type": kind, "mode": stat.S_IMODE(st.st_mode)}
            if kind == "file":
                with open(full, "rb") as f:
                    rec["sha256"] = hashlib.sha256(f.read()).hexdigest()
                if st.st_nlink > 1:
                    inodes.setdefault(st.st_ino, []).append(rel)
            if kind == "symlink":
                rec["target"] = os.readlink(full)
                rec.pop("mode")
            else:
                rec["xattrs"] = sorted((x, os.getxattr(full, x, follow_symlinks=False).hex()) for x in os.listxattr(full, follow_symlinks=False))
            out[rel] = rec
    groups = sorted(",".join(sorted(v)) for v in inodes.values())
    return out, groups

a, ga = scan(sys.argv[1])
b, gb = scan(sys.argv[2])
diffs = [p for p in sorted(set(a) | set(b)) if a.get(p) != b.get(p)]
for p in diffs[:20]:
    print("differs:", p, a.get(p), b.get(p))
print(json.dumps({"entries_a": len(a), "entries_b": len(b), "link_groups_a": len(ga), "link_groups_b": len(gb),
                  "groups_equal": ga == gb, "differences": len(diffs),
                  "xattr_files": sum(1 for r in a.values() if r.get("xattrs"))}))
sys.exit(1 if diffs or ga != gb else 0)
