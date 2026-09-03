#!/usr/bin/env python3
"""Render full SSH terminal session as PNG."""

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
SESSION = ROOT / "tiktok_api_server_terminal_session.txt"
OUT = ROOT / "tiktok_api_server_terminal_screenshot.png"

HEADER = (
    "(base) comm-naraharisetty@COMM-A92978 ~ % ssh cme-p01\n"
    "Last login: Mon Jun 15 19:12:00 2026 from 10.146.232.167\n"
)

# macOS monospace fonts
FONT_CANDIDATES = [
    "/System/Library/Fonts/Menlo.ttc",
    "/System/Library/Fonts/Monaco.dfont",
    "/Library/Fonts/Courier New.ttf",
]

FONT_SIZE = 13
LINE_HEIGHT = 18
PAD_X = 18
PAD_Y = 16
TITLE_H = 32
WIN_W = 1100
BG_OUTER = (45, 45, 45)
BG_TERM = (0, 0, 0)
BG_TITLE = (60, 60, 60)
FG = (255, 255, 255)
FG_ERR = (255, 107, 107)
FG_TITLE = (200, 200, 200)
DOT_COLORS = [(255, 95, 86), (255, 189, 46), (39, 201, 63)]


def load_font(size: int):
    for path in FONT_CANDIDATES:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size, index=0)
            except OSError:
                try:
                    return ImageFont.truetype(path, size)
                except OSError:
                    continue
    return ImageFont.load_default()


def colorize_line(line: str) -> tuple[str, tuple]:
    err_keys = (
        "SSL:", "curl: (60)", "Traceback", "Error", "SSLError",
        "CERTIFICATE_VERIFY_FAILED", "MaxRetryError", "urllib3.exceptions",
        "requests.exceptions", "ssl.SSLCertVerificationError",
        "prohibitedtech.gw.utexas.edu",
    )
    if any(k in line for k in err_keys):
        return line, FG_ERR
    return line, FG


def main():
    body = SESSION.read_text(encoding="utf-8")
    text = HEADER + body
    lines = text.splitlines()

    font = load_font(FONT_SIZE)
    content_h = PAD_Y * 2 + len(lines) * LINE_HEIGHT
    img_h = TITLE_H + content_h + 24

    img = Image.new("RGB", (WIN_W, img_h), BG_OUTER)
    draw = ImageDraw.Draw(img)

    # window frame
    win_x, win_y = 24, 24
    win_w = WIN_W - 48
    win_h = img_h - 48
    draw.rounded_rectangle(
        (win_x, win_y, win_x + win_w, win_y + win_h),
        radius=8,
        fill=BG_TERM,
        outline=(80, 80, 80),
    )

    # title bar
    draw.rectangle(
        (win_x, win_y, win_x + win_w, win_y + TITLE_H),
        fill=BG_TITLE,
    )
    for i, color in enumerate(DOT_COLORS):
        cx = win_x + 14 + i * 18
        cy = win_y + TITLE_H // 2
        draw.ellipse((cx - 6, cy - 6, cx + 6, cy + 6), fill=color)

    title = "comm-naraharisetty — ssh cme-p01 — 80×24"
    tb = draw.textbbox((0, 0), title, font=font)
    tw = tb[2] - tb[0]
    draw.text(
        (win_x + (win_w - tw) // 2, win_y + 8),
        title,
        fill=FG_TITLE,
        font=font,
    )

    # terminal content
    y = win_y + TITLE_H + PAD_Y
    x = win_x + PAD_X
    for line in lines:
        _, color = colorize_line(line)
        draw.text((x, y), line, fill=color, font=font)
        y += LINE_HEIGHT

    img.save(OUT, "PNG", optimize=True)
    print(f"Saved {OUT} ({img.size[0]}x{img.size[1]})")


if __name__ == "__main__":
    main()
