import re, io

src = "KawakibNayyirat.Shamela0000309.mARkdown"
raw = io.open(src, encoding="utf-8").read().split("\n")

# 1) split off metadata header
try:
    start = next(i for i,l in enumerate(raw) if l.strip()=="#META#Header#End#") + 1
except StopIteration:
    start = 0
meta = {}
for l in raw[:start]:
    m = re.match(r"#META#\s*\d+\.(\w+)\s*::\s*(.*)", l)
    if m and m.group(2).strip() not in ("NODATA","NOTGIVEN",""):
        meta.setdefault(m.group(1), m.group(2).strip())
body = raw[start:]

# 2) merge soft-wrap continuation lines (lines beginning with ~~)
logical = []
for l in body:
    if l.startswith("~~"):
        logical[-1] = logical[-1] + " " + l[2:]
    else:
        logical.append(l)

def page_repl(m):
    v, p = int(m.group(1)), int(m.group(2))
    if v == 0 and p == 0:
        return ""
    return f"\n【ج{v} ص{p}】"

out = []
entries = 0
for l in logical:
    l = l.strip()
    if not l or l.startswith("######OpenITI"):
        continue
    # strip OpenITI milestone tokens (msNN) — every-300-words markers, not content
    l = re.sub(r"(?<!\S)ms\d+(?!\S)", "", l)
    # page markers -> readable inline ref
    l = re.sub(r"PageV(\d+)P(\d+)", page_repl, l)
    # structural headers
    if l.startswith("### $"):
        entries += 1
        l = re.sub(r"^###\s*\$+\s*", "", l).strip()
        out.append("\n\n### " + l + "\n")
        continue
    if l.startswith("### |") or l.startswith("###|"):
        l = re.sub(r"^###\s*\|\s*", "", l).strip()
        out.append("\n\n## " + l + "\n")
        continue
    l = re.sub(r"^#{1,6}\s*", "", l)      # drop remaining leading # markers
    l = re.sub(r"[ \t]{2,}", " ", l).strip()
    if l:
        out.append(l)

text = "\n".join(out)
text = re.sub(r"\n{3,}", "\n\n", text).strip() + "\n"

front = (
 "الكواكب النيرات في معرفة من اختلط من الرواة الثقات\n"
 f"المؤلف: {meta.get('AuthorNAME','أبو البركات محمد بن أحمد بن الكيال (ت 939هـ)')}\n"
 f"المحقق: {meta.get('EdEDITOR','عبد القيوم عبد رب النبي')}\n"
 f"الناشر: {meta.get('EdPUBLISHER','دار المأمون ـ بيروت')} — {meta.get('EdNUMBER','الأولى 1981م')}\n"
 "المصدر: OpenITI corpus (نسخة الشاملة 0000309، الأساسية المنقّحة المرقّمة)\n"
 f"عدد التراجم (المختلطون): {entries}\n"
 + "="*70 + "\n\n"
)
io.open("al-Kawakib-al-Nayyirat_clean.txt","w",encoding="utf-8").write(front + text)
print("entries (### $ biographical):", entries)
print("clean chars:", len(front)+len(text))
