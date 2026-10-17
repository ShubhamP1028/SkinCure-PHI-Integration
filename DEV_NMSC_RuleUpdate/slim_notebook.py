#!/usr/bin/env python3
"""Strip a dev notebook down to the prod house style, without touching a line of logic.

Two transforms, both reversible in the sense that nothing executable changes:

  1. A `%md` cell collapses to its FIRST heading line. The dev notebooks carry the
     reasoning -- why a rule exists, what was measured, what was ruled out -- and that
     belongs in the wiki, not in front of someone running the month.
  2. An inline comment BLOCK longer than `--keep` lines collapses to its first line
     with a "(see wiki)" marker. Short comments stay: they are what makes a dense SQL
     string readable, and deleting them to hit a line count would be a false economy.

Code, blank lines, string literals and cell separators are untouched. The output is
byte-identical to the input everywhere except in comments, which is the property that
makes this safe to run unattended.

    python3 slim_notebook.py in.py -o out.py [--keep 4]
"""
import argparse, re


def slim(src, keep=4):
    lines = src.split("\n")
    out, i = [], 0
    md_cells = md_lines = com_blocks = com_lines = 0

    while i < len(lines):
        ln = lines[i]

        # ---- a %md cell: take the first heading, drop the rest
        if ln.startswith("# MAGIC %md"):
            j, heading = i, None
            while j < len(lines) and (lines[j].startswith("# MAGIC") or lines[j].strip() == ""):
                if lines[j].strip() == "" and not lines[j].startswith("# MAGIC"):
                    break
                m = re.match(r"^# MAGIC\s+(#+\s+.*)$", lines[j])
                if m and heading is None:
                    heading = m.group(1)
                j += 1
            md_cells += 1
            md_lines += (j - i)
            out.append("# MAGIC %md")
            if heading:
                out.append(f"# MAGIC {heading}")
            i = j
            continue

        # ---- a run of full-line code comments
        if re.match(r"^(\s*)#(?!\s*(COMMAND|MAGIC|!|type:|noqa))", ln):
            j = i
            while j < len(lines) and re.match(r"^(\s*)#(?!\s*(COMMAND|MAGIC))", lines[j]):
                j += 1
            block = lines[i:j]
            if len(block) > keep:
                com_blocks += 1
                com_lines += len(block) - 1
                indent = re.match(r"^(\s*)", block[0]).group(1)
                first = block[0].strip().rstrip(".")
                out.append(f"{first}. See wiki." if not first.endswith("wiki") else first)
                out[-1] = indent + out[-1].lstrip()
            else:
                out.extend(block)
            i = j
            continue

        out.append(ln)
        i += 1

    return "\n".join(out), dict(md_cells=md_cells, md_lines=md_lines,
                                comment_blocks=com_blocks, comment_lines=com_lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src"); ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--keep", type=int, default=4,
                    help="comment blocks up to this many lines are left alone")
    A = ap.parse_args()
    src = open(A.src, encoding="utf8").read()
    slimmed, st = slim(src, A.keep)

    # the guarantee: every non-comment line survives, in order
    def code_only(t):
        return [l for l in t.split("\n")
                if not l.startswith("# MAGIC") and not re.match(r"^\s*#", l)]
    assert code_only(src) == code_only(slimmed), "code changed — refusing to write"

    open(A.out, "w", encoding="utf8").write(slimmed)
    b, a = len(src.split("\n")), len(slimmed.split("\n"))
    print(f"{A.src} → {A.out}   {b} → {a} lines  (−{b-a}, {100*(b-a)/b:.0f}%)")
    print(f"    {st['md_cells']} md cells collapsed ({st['md_lines']} lines), "
          f"{st['comment_blocks']} comment blocks ({st['comment_lines']} lines)")


if __name__ == "__main__":
    main()
