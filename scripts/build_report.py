# /// script
# requires-python = ">=3.11"
# dependencies = ["reportlab==4.4.3", "matplotlib==3.9.4"]
# ///
"""Typeset the study and its literature review as a standalone submission."""
import re
from html import escape
from pathlib import Path

from matplotlib import get_data_path
from PIL import Image as Bitmap
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "pdf" / "half-hour-risk.pdf"
REPO = "https://github.com/shiv-eshwar/half-hour-risk"
TEAL = "#146962"
INK = "#142F37"
MUTED = "#455E64"
WIDTH = A4[0] - 90
fonts = Path(get_data_path()) / "fonts" / "ttf"
for name, file in (("Sans", "DejaVuSans"), ("SansBold", "DejaVuSans-Bold"),
                   ("SansIt", "DejaVuSans-Oblique"), ("SansBI", "DejaVuSans-BoldOblique"),
                   ("Serif", "DejaVuSerif")):
    pdfmetrics.registerFont(TTFont(name, fonts / (file + ".ttf")))
pdfmetrics.registerFontFamily("Sans", normal="Sans", bold="SansBold", italic="SansIt", boldItalic="SansBI")


def style(name, font, size, leading, color=INK, after=0):
    return ParagraphStyle(name, fontName=font, fontSize=size, leading=leading,
                          textColor=colors.HexColor(color), spaceAfter=after)


body = style("Body", "Sans", 9.6, 15.1, after=8)
title = style("Title", "Serif", 32, 39, after=8)
heading = style("Heading", "Serif", 20, 25, after=9)
minor = style("Minor", "SansBold", 11.2, 16, TEAL, after=5)
deck = style("Deck", "Sans", 13.1, 19.5, MUTED, after=14)
cell = style("Cell", "Sans", 8.25, 12.1)
head = style("Head", "SansBold", 8.1, 11.2, "#FFFFFF")
story = []


def link(match):
    label, url = match.groups()
    if not url.startswith(("http://", "https://")):
        url = f"{REPO}/blob/main/{url}"
    return f'<link href="{url}" color="{TEAL}">{label}</link>'


def rich(text):
    text = escape(text).replace("Cₘ₋₁", "C[m−1]").replace("Cₘ", "C[m]")
    text = re.sub(r'\[([^\]]+)\]\(([^\s)]+)\)', link, text)
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    return re.sub(r'(?<!\*)\*([^*]+)\*(?!\*)', r'<i>\1</i>', text).replace(chr(96), "")


def para(text, fmt=body):
    story.append(Paragraph(rich(text), fmt))


def table(rows, fractions):
    data = [[Paragraph(rich(x), head if i == 0 else cell) for x in row] for i, row in enumerate(rows)]
    tab = Table(data, colWidths=[WIDTH * k for k in fractions], repeatRows=1, hAlign="LEFT")
    tab.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(TEAL)),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F0F5F4")]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"), ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5), ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6)]))
    story.extend([tab, Spacer(1, 10)])


def section(source, name):
    part = source.split(f"## {name}\n", 1)[1].split("\n## ", 1)[0]
    return part.strip().split("\n\n")


def markdown_table(block, widths):
    lines = block.splitlines()
    table([line.strip("|").strip().split(" | ") for line in [lines[0], *lines[2:]]], widths)


def page(title_text):
    story.append(PageBreak())
    para(title_text, heading)


def furniture(canvas, doc):
    w, h = A4
    canvas.setTitle("Half-Hour Risk | Bitcoin volatility warnings with Apache Spark")
    canvas.setAuthor("Half-Hour Risk project")
    canvas.setFont("SansBold", 8.4)
    canvas.setFillColor(colors.HexColor(TEAL))
    canvas.drawString(45, h-34, "MINING OF MASSIVE DATASETS")
    canvas.setFont("Sans", 8.4)
    canvas.drawRightString(w-45, h-34, "QUANTITATIVE FINANCE  /  TECHNICAL REPORT")
    canvas.setStrokeColor(colors.HexColor("#C6D8D5"))
    canvas.line(45, h-42, w-45, h-42)
    canvas.line(45, 41, w-45, 41)
    canvas.setFillColor(colors.HexColor(MUTED))
    canvas.drawString(45, 25, "Half-Hour Risk   ·   BTC/USDT spot   ·   January–June 2024")
    canvas.drawRightString(w-45, 25, str(doc.page))


report = (ROOT / "report.md").read_text()
lit = (ROOT / "literature.md").read_text()
architecture = (ROOT / "architecture.md").read_text()
results = section(report, "Selection and held-out results")
design = section(report, "Research design")
quality = section(report, "Data and leakage controls")
cases = section(report, "Two decisions from the test set")
scale = section(report, "Spark scalability")
para("Half-Hour Risk", title)
para("Can the size and direction of Bitcoin trades warn of the next volatile half-hour?", deck)
para(report.split("\n\n")[2])
para("The decision at the start of each half-hour", minor)
para(design[0])
para(design[1])
para("Study design", minor)
para(design[2])
para(design[3])
para(f"Find the code, data manifest and fitted Spark pipeline at [github.com/shiv-eshwar/half-hour-risk]({REPO})")

page("Data, clocks and features")
para("Six months of publisher-verified market records", minor)
markdown_table(quality[0], [.68, .32])
para(quality[1])
para("At 14:00 UTC the newest input is the close of the 13:59 minute. The 14:00–14:29 minutes determine the future outcome. BTC and ETH contribute ordinary bar signals. The recent Bitcoin trade tape contributes the following quantities.")
para("Let x denote a print’s quote notional and s its taker sign (+1 for a buy). Over the preceding half-hour, N counts prints, V is the sum of x, Q is the sum of x², and S is the sum of s × x².")
moment_table = next(b for b in architecture.split("\n\n") if b.startswith("| Added feature"))
markdown_table(moment_table, [.235, .275, .49])
para("The 1,441 prior minute closes supply one day of minute returns. At a forecast boundary the model can therefore run even when the target half-hour has yet to occur.")

page("Selection and test performance")
para(results[0])
para(results[1])
markdown_table(results[2], [.244, .09, .09, .08, .13, .14, .12, .106])
plot_path = ROOT / "results" / "precision-recall.png"
with Bitmap.open(plot_path) as bitmap:
    w, h = bitmap.size
story.append(Image(str(plot_path), width=WIDTH - 15, height=(WIDTH - 15) * h / w))

page("What the warnings captured")
for item in results[4:]:
    para(item)
para("Two decisions from the held-out period", minor)
markdown_table(cases[0], [.17, .11, .12, .18, .42])
para(cases[1])
para("Interpretation limits", minor)
for item in section(report, "Interpretation limits")[0].splitlines():
    para(item.removeprefix("- "))

page("Distributed execution and replay")
para(scale[0])
markdown_table(scale[1], [.16, .17, .32, .20, .15])
for item in scale[2:]:
    para(item)
para("Checkpoint recovery", minor)
for item in section(report, "Streaming verification"):
    para(item)
para("Reproduction", minor)
para("Install the repository requirements in a Python 3.11 environment with Java 17. From the repository root, scripts/reproduce rebuilds the experiment, replay, benchmark suite, figure and demo. Use scripts/run predict --at 2024-07-01T00:00Z for a warning at the archive boundary. [Engineering guide](engineering.md)")

page("Evidence from earlier work")
para("Published studies motivate both sides of the comparison. The controlled test holds the estimator, ordinary-bar controls and forecast timestamps fixed while adding trade-size information.", deck)
markdown_table(section(lit, "What previous studies establish")[0], [.35, .65])
para("Precision and recall", minor)
para(section(lit, "What result would answer the question?")[-1])
para("Data and publication", minor)
para("The original spot archives and field definitions are available from Binance. Each input URL and its publisher SHA-256 digest are pinned in the public repository. [Binance Public Data](https://github.com/binance/binance-public-data) · [Study manifest](sources/manifest.json) · [Full literature review](literature.md)")

OUT.parent.mkdir(parents=True, exist_ok=True)
doc = SimpleDocTemplate(str(OUT), pagesize=A4, leftMargin=39, rightMargin=39,
                        topMargin=54, bottomMargin=48, title="Half-Hour Risk")
doc.build(story, onFirstPage=furniture, onLaterPages=furniture)
print(OUT)
