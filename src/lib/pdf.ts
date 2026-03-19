type RgbColor = readonly [number, number, number];

type PdfBlock =
  | { kind: "spacer"; height: number }
  | { kind: "heading_1" | "heading_2" | "heading_3"; text: string }
  | { kind: "paragraph"; text: string }
  | { kind: "bullet"; text: string }
  | { kind: "metric"; label: string; value: string }
  | { kind: "code"; lines: string[] };

const PDF_PAGE_WIDTH = 595.28;
const PDF_PAGE_HEIGHT = 841.89;
const PDF_PAGE_MARGIN = 36;
const PDF_CONTENT_CARD_X = PDF_PAGE_MARGIN;
const PDF_CONTENT_CARD_WIDTH = PDF_PAGE_WIDTH - PDF_PAGE_MARGIN * 2;
const PDF_CONTENT_LEFT = PDF_CONTENT_CARD_X + 24;
const PDF_CONTENT_WIDTH = PDF_CONTENT_CARD_WIDTH - 48;
const PDF_BOTTOM_MARGIN = 78;
const PDF_FIRST_PAGE_TOP = 536;
const PDF_OTHER_PAGE_TOP = 714;

const PDF_COLORS = {
  accent: [0.306, 0.459, 0.949] as RgbColor,
  slate: [0.082, 0.117, 0.211] as RgbColor,
  text: [0.188, 0.223, 0.305] as RgbColor,
  muted: [0.451, 0.494, 0.576] as RgbColor,
  codeBg: [0.943, 0.957, 0.984] as RgbColor,
  rule: [0.835, 0.862, 0.921] as RgbColor,
  pageBg: [0.972, 0.978, 0.992] as RgbColor,
  cardBg: [1, 1, 1] as RgbColor,
  cardBorder: [0.879, 0.905, 0.952] as RgbColor,
  heroLeft: [0.412, 0.231, 0.925] as RgbColor,
  heroCenter: [0.267, 0.351, 0.933] as RgbColor,
  heroRight: [0.173, 0.592, 0.898] as RgbColor,
  heroSoft: [0.809, 0.863, 0.988] as RgbColor,
  pillBg: [0.516, 0.430, 0.969] as RgbColor,
  pillText: [0.945, 0.953, 0.996] as RgbColor,
  metaLabel: [0.761, 0.815, 0.969] as RgbColor,
  metaValue: [1, 1, 1] as RgbColor,
  sectionBg: [0.961, 0.969, 0.988] as RgbColor,
  sectionText: [0.396, 0.454, 0.588] as RgbColor,
};

function slugify(value: string): string {
  return (
    value
      .normalize("NFKD")
      .replace(/[^\x00-\x7F]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "") || "report"
  );
}

function rgb(color: RgbColor): string {
  return `${color[0].toFixed(3)} ${color[1].toFixed(3)} ${color[2].toFixed(3)}`;
}

function stripInlineMarkdown(value: string): string {
  return String(value || "")
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/\*([^*]+)\*/g, "$1")
    .replace(/__([^_]+)__/g, "$1")
    .replace(/_([^_]+)_/g, "$1")
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function escapePdfText(value: string): string {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[^\x20-\x7E\n]/g, "")
    .replace(/\\/g, "\\\\")
    .replace(/\(/g, "\\(")
    .replace(/\)/g, "\\)");
}

function wrapText(text: string, maxChars: number): string[] {
  const cleaned = String(text || "").trim();
  if (!cleaned) return [""];
  const words = cleaned.split(/\s+/);
  const lines: string[] = [];
  let current = "";

  words.forEach((word) => {
    if (!current) {
      current = word;
      return;
    }
    const candidate = `${current} ${word}`;
    if (candidate.length <= maxChars) {
      current = candidate;
      return;
    }
    lines.push(current);
    current = word;
  });

  if (current) lines.push(current);
  return lines.length > 0 ? lines : [cleaned.slice(0, maxChars)];
}

function parseTableCells(line: string): string[] {
  const stripped = String(line || "").trim();
  if (!stripped.startsWith("|")) return [];
  return stripped
    .replace(/^\||\|$/g, "")
    .split("|")
    .map((cell) => stripInlineMarkdown(cell));
}

function parseMarkdownBlocks(markdown: string): PdfBlock[] {
  const text = String(markdown || "").replace(/\r/g, "");
  const lines = text.split("\n");
  const blocks: PdfBlock[] = [];
  let paragraphLines: string[] = [];
  let codeLines: string[] = [];
  let inCode = false;
  let index = 0;

  const flushParagraph = () => {
    const paragraph = paragraphLines.map((line) => line.trim()).filter(Boolean).join(" ");
    if (paragraph) blocks.push({ kind: "paragraph", text: stripInlineMarkdown(paragraph) });
    paragraphLines = [];
  };

  const flushCode = () => {
    if (codeLines.length > 0) {
      blocks.push({ kind: "code", lines: codeLines.map((line) => line.slice(0, 110)) });
    }
    codeLines = [];
  };

  while (index < lines.length) {
    const rawLine = lines[index];
    const stripped = rawLine.trimEnd();

    if (/^\s*```/.test(stripped)) {
      flushParagraph();
      if (inCode) flushCode();
      inCode = !inCode;
      index += 1;
      continue;
    }

    if (inCode) {
      codeLines.push(stripped);
      index += 1;
      continue;
    }

    if (!stripped.trim()) {
      flushParagraph();
      blocks.push({ kind: "spacer", height: 8 });
      index += 1;
      continue;
    }

    if (
      stripped.trim().startsWith("|") &&
      index + 1 < lines.length &&
      /^\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*$/.test(lines[index + 1].trim())
    ) {
      flushParagraph();
      const headers = parseTableCells(stripped);
      const rows: string[][] = [];
      let cursor = index + 2;

      while (cursor < lines.length) {
        const rowLine = lines[cursor].trimEnd();
        if (!rowLine.trim().startsWith("|")) break;
        const cells = parseTableCells(rowLine);
        if (cells.length > 0) rows.push(cells);
        cursor += 1;
      }

      if (headers.length > 0 && rows.length > 0) {
        if (headers.length === 2) {
          rows.forEach((row) => {
            blocks.push({
              kind: "metric",
              label: row[0] || "",
              value: row[1] || "",
            });
          });
        } else {
          rows.forEach((row) => {
            const parts = headers
              .map((header, cellIndex) => {
                const value = row[cellIndex] || "";
                return value ? `${header}: ${value}` : "";
              })
              .filter(Boolean);
            if (parts.length > 0) blocks.push({ kind: "bullet", text: parts.join(" | ") });
          });
        }
      }

      index = cursor;
      continue;
    }

    const headingMatch = stripped.match(/^\s*(#{1,3})\s+(.+)$/);
    if (headingMatch) {
      flushParagraph();
      const level = headingMatch[1].length;
      const headingKind = `heading_${Math.min(level, 3)}` as "heading_1" | "heading_2" | "heading_3";
      blocks.push({
        kind: headingKind,
        text: stripInlineMarkdown(headingMatch[2]),
      });
      index += 1;
      continue;
    }

    const bulletMatch = stripped.match(/^\s*(?:[-*]|\d+\.)\s+(?:\[[ xX]\]\s+)?(.+)$/);
    if (bulletMatch) {
      flushParagraph();
      blocks.push({ kind: "bullet", text: stripInlineMarkdown(bulletMatch[1]) });
      index += 1;
      continue;
    }

    const quoteMatch = stripped.match(/^\s*>\s?(.*)$/);
    if (quoteMatch) {
      paragraphLines.push(quoteMatch[1]);
      index += 1;
      continue;
    }

    paragraphLines.push(stripped);
    index += 1;
  }

  flushParagraph();
  flushCode();
  return blocks.filter((block) => {
    if (block.kind === "spacer") return true;
    if (block.kind === "code") return block.lines.length > 0;
    if (block.kind === "metric") return Boolean(block.label || block.value);
    return Boolean((block as { text?: string }).text);
  });
}

function addObject(objects: string[], content: string): number {
  objects.push(content);
  return objects.length;
}

function buildStyledPdfDocument(markdown: string, title: string): Uint8Array {
  const cleanTitle = stripInlineMarkdown(title) || "Data Quality Summary";
  const cleanSubtitle = "Executive report generated from RAGnarok";
  const generatedLabel = `Generated on ${new Date().toISOString().slice(0, 16).replace("T", " ")} UTC`;
  const blocks = parseMarkdownBlocks(markdown);
  const pages: string[][] = [];
  let pageIndex = -1;
  let cursorY = 0;

  const newPage = () => {
    pageIndex += 1;
    cursorY = pageIndex === 0 ? PDF_FIRST_PAGE_TOP : PDF_OTHER_PAGE_TOP;
    const ops: string[] = [
      `${rgb(PDF_COLORS.pageBg)} rg`,
      `0 0 ${PDF_PAGE_WIDTH.toFixed(2)} ${PDF_PAGE_HEIGHT.toFixed(2)} re f`,
    ];

    if (pageIndex === 0) {
      const heroX = PDF_PAGE_MARGIN;
      const heroY = 610;
      const heroHeight = 176;
      const heroWidth = PDF_PAGE_WIDTH - PDF_PAGE_MARGIN * 2;
      const metadata = [
        ["GENERATED", generatedLabel.replace("Generated on ", "")],
        ["SOURCE", "RAGnarok"],
        ["FORMAT", "Executive PDF export"],
      ];
      const columnWidth = (heroWidth - 52) / 3;

      ops.push(
        `${rgb(PDF_COLORS.heroLeft)} rg`,
        `${heroX.toFixed(2)} ${heroY.toFixed(2)} ${(heroWidth * 0.36).toFixed(2)} ${heroHeight.toFixed(2)} re f`,
        `${rgb(PDF_COLORS.heroCenter)} rg`,
        `${(heroX + heroWidth * 0.36).toFixed(2)} ${heroY.toFixed(2)} ${(heroWidth * 0.32).toFixed(2)} ${heroHeight.toFixed(2)} re f`,
        `${rgb(PDF_COLORS.heroRight)} rg`,
        `${(heroX + heroWidth * 0.68).toFixed(2)} ${heroY.toFixed(2)} ${(heroWidth * 0.32).toFixed(2)} ${heroHeight.toFixed(2)} re f`,
        `${rgb(PDF_COLORS.pillBg)} rg`,
        `${(heroX + 26).toFixed(2)} ${(heroY + heroHeight - 40).toFixed(2)} 196 18 re f`,
        `${rgb(PDF_COLORS.heroSoft)} rg`,
        `${(heroX + heroWidth - 76).toFixed(2)} ${(heroY + heroHeight - 70).toFixed(2)} 40 40 re f`,
        "BT",
        "/F2 7.5 Tf",
        `${rgb(PDF_COLORS.pillText)} rg`,
        `1 0 0 1 ${(heroX + 34).toFixed(2)} ${(heroY + heroHeight - 28).toFixed(2)} Tm`,
        "([EXECUTIVE REPORT  |  RAGNAROK]) Tj",
        "ET",
        "BT",
        "/F2 24 Tf",
        "1 1 1 rg",
        `1 0 0 1 ${(heroX + 26).toFixed(2)} ${(heroY + heroHeight - 66).toFixed(2)} Tm`,
        `(${escapePdfText(cleanTitle)}) Tj`,
        "ET",
        "BT",
        "/F1 11 Tf",
        "0.89 0.92 0.97 rg",
        `1 0 0 1 ${(heroX + 26).toFixed(2)} ${(heroY + heroHeight - 90).toFixed(2)} Tm`,
        `(${escapePdfText(cleanSubtitle)}) Tj`,
        "ET",
        `${rgb(PDF_COLORS.heroSoft)} rg`,
        `${(heroX + 26).toFixed(2)} ${(heroY + 72).toFixed(2)} ${(heroWidth - 52).toFixed(2)} 1.4 re f`,
      );

      metadata.forEach(([label, value], metadataIndex) => {
        const columnX = heroX + 26 + metadataIndex * columnWidth;
        ops.push(
          "BT",
          "/F2 8 Tf",
          `${rgb(PDF_COLORS.metaLabel)} rg`,
          `1 0 0 1 ${columnX.toFixed(2)} ${(heroY + 52).toFixed(2)} Tm`,
          `(${escapePdfText(label)}) Tj`,
          "ET",
          "BT",
          "/F2 10.5 Tf",
          `${rgb(PDF_COLORS.metaValue)} rg`,
          `1 0 0 1 ${columnX.toFixed(2)} ${(heroY + 34).toFixed(2)} Tm`,
          `(${escapePdfText(value)}) Tj`,
          "ET",
        );
      });

      ops.push(
        `${rgb(PDF_COLORS.slate)} rg`,
        `${heroX.toFixed(2)} ${heroY.toFixed(2)} ${heroWidth.toFixed(2)} 16 re f`,
        "BT",
        "/F1 7.8 Tf",
        `${rgb(PDF_COLORS.metaLabel)} rg`,
        `1 0 0 1 ${(heroX + 26).toFixed(2)} ${(heroY + 4.5).toFixed(2)} Tm`,
        "(Document generated automatically by RAGnarok  |  Reserved for internal reporting use) Tj",
        "ET",
      );

      ops.push(
        `${rgb(PDF_COLORS.cardBg)} rg`,
        `${PDF_CONTENT_CARD_X.toFixed(2)} 56.00 ${PDF_CONTENT_CARD_WIDTH.toFixed(2)} 520.00 re f`,
        `${rgb(PDF_COLORS.cardBorder)} RG`,
        `${PDF_CONTENT_CARD_X.toFixed(2)} 56.00 ${PDF_CONTENT_CARD_WIDTH.toFixed(2)} 520.00 re S`,
      );
    } else {
      const headerHeight = 46;
      const headerY = PDF_PAGE_HEIGHT - headerHeight - PDF_PAGE_MARGIN + 8;
      ops.push(
        `${rgb(PDF_COLORS.slate)} rg`,
        `${PDF_PAGE_MARGIN.toFixed(2)} ${headerY.toFixed(2)} ${(PDF_PAGE_WIDTH - PDF_PAGE_MARGIN * 2).toFixed(2)} ${headerHeight.toFixed(2)} re f`,
        "BT",
        "/F2 11 Tf",
        "1 1 1 rg",
        `1 0 0 1 ${(PDF_PAGE_MARGIN + 20).toFixed(2)} ${(headerY + 17).toFixed(2)} Tm`,
        `(${escapePdfText(cleanTitle)}) Tj`,
        "ET",
        `${rgb(PDF_COLORS.cardBg)} rg`,
        `${PDF_CONTENT_CARD_X.toFixed(2)} 56.00 ${PDF_CONTENT_CARD_WIDTH.toFixed(2)} 676.00 re f`,
        `${rgb(PDF_COLORS.cardBorder)} RG`,
        `${PDF_CONTENT_CARD_X.toFixed(2)} 56.00 ${PDF_CONTENT_CARD_WIDTH.toFixed(2)} 676.00 re S`,
      );
    }

    ops.push(
      `${rgb(PDF_COLORS.sectionBg)} rg`,
      `${(PDF_CONTENT_CARD_X + 18).toFixed(2)} ${(pageIndex === 0 ? 546 : 702).toFixed(2)} 164 18 re f`,
      "BT",
      "/F2 7.8 Tf",
      `${rgb(PDF_COLORS.sectionText)} rg`,
      `1 0 0 1 ${(PDF_CONTENT_CARD_X + 28).toFixed(2)} ${(pageIndex === 0 ? 558 : 714).toFixed(2)} Tm`,
      `(${escapePdfText(pageIndex === 0 ? "ANALYSIS COMPLETE" : "ANALYSIS CONTINUED")}) Tj`,
      "ET",
      "BT",
      "/F1 9 Tf",
      `${rgb(PDF_COLORS.muted)} rg`,
      `1 0 0 1 ${PDF_PAGE_MARGIN.toFixed(2)} 24.00 Tm`,
      `(${escapePdfText(`${generatedLabel}  |  Page ${pageIndex + 1}`)}) Tj`,
      "ET",
    );

    pages.push(ops);
  };

  const ensureSpace = (height: number) => {
    if (pageIndex < 0) newPage();
    if (cursorY - height < PDF_BOTTOM_MARGIN) newPage();
  };

  const addTextLines = (
    lines: string[],
    x: number,
    font: "F1" | "F2" | "F3",
    size: number,
    leading: number,
    color: RgbColor
  ) => {
    lines.forEach((line) => {
      pages[pageIndex].push(
        "BT",
        `/${font} ${size.toFixed(2)} Tf`,
        `${rgb(color)} rg`,
        `1 0 0 1 ${x.toFixed(2)} ${cursorY.toFixed(2)} Tm`,
        `(${escapePdfText(line)}) Tj`,
        "ET",
      );
      cursorY -= leading;
    });
  };

  blocks.forEach((block) => {
    if (block.kind === "spacer") {
      ensureSpace(block.height);
      cursorY -= block.height;
      return;
    }

    if (block.kind === "heading_1") {
      const lines = wrapText(block.text, 54);
      ensureSpace(14 + lines.length * 24);
      cursorY -= 4;
      addTextLines(lines, PDF_CONTENT_LEFT, "F2", 18, 22, PDF_COLORS.slate);
      pages[pageIndex].push(
        `${rgb(PDF_COLORS.accent)} rg`,
        `${PDF_CONTENT_LEFT.toFixed(2)} ${(cursorY + 8).toFixed(2)} 34 2 re f`,
      );
      cursorY -= 10;
      return;
    }

    if (block.kind === "heading_2" || block.kind === "heading_3") {
      const lines = wrapText(block.text, 68);
      ensureSpace(10 + lines.length * 19);
      cursorY -= 2;
      addTextLines(lines, PDF_CONTENT_LEFT, "F2", block.kind === "heading_2" ? 13 : 12, 17, PDF_COLORS.slate);
      cursorY -= 6;
      return;
    }

    if (block.kind === "metric") {
      const labelLines = wrapText(block.label, 28);
      const valueLines = wrapText(block.value, 42);
      const lineCount = Math.max(labelLines.length, valueLines.length, 1);
      const rowHeight = 14 + lineCount * 13;
      ensureSpace(rowHeight + 6);
      const rowBottom = cursorY - rowHeight + 3;
      const rowStartY = cursorY - 14;

      pages[pageIndex].push(
        `${rgb(PDF_COLORS.sectionBg)} rg`,
        `${PDF_CONTENT_LEFT.toFixed(2)} ${rowBottom.toFixed(2)} ${PDF_CONTENT_WIDTH.toFixed(2)} ${rowHeight.toFixed(2)} re f`,
        `${rgb(PDF_COLORS.cardBorder)} RG`,
        `${PDF_CONTENT_LEFT.toFixed(2)} ${rowBottom.toFixed(2)} ${PDF_CONTENT_WIDTH.toFixed(2)} ${rowHeight.toFixed(2)} re S`,
        `${rgb(PDF_COLORS.accent)} rg`,
        `${(PDF_CONTENT_LEFT + 12).toFixed(2)} ${(rowBottom + 8).toFixed(2)} 3.5 ${(rowHeight - 16).toFixed(2)} re f`,
      );

      for (let lineIndex = 0; lineIndex < lineCount; lineIndex += 1) {
        if (lineIndex < labelLines.length) {
          pages[pageIndex].push(
            "BT",
            "/F2 10.5 Tf",
            `${rgb(PDF_COLORS.slate)} rg`,
            `1 0 0 1 ${(PDF_CONTENT_LEFT + 24).toFixed(2)} ${(rowStartY - lineIndex * 13).toFixed(2)} Tm`,
            `(${escapePdfText(labelLines[lineIndex])}) Tj`,
            "ET",
          );
        }
        if (lineIndex < valueLines.length) {
          pages[pageIndex].push(
            "BT",
            "/F1 10.5 Tf",
            `${rgb(PDF_COLORS.text)} rg`,
            `1 0 0 1 ${(PDF_CONTENT_LEFT + PDF_CONTENT_WIDTH * 0.43).toFixed(2)} ${(rowStartY - lineIndex * 13).toFixed(2)} Tm`,
            `(${escapePdfText(valueLines[lineIndex])}) Tj`,
            "ET",
          );
        }
      }

      cursorY -= rowHeight + 8;
      return;
    }

    if (block.kind === "bullet") {
      const lines = wrapText(block.text, 74);
      ensureSpace(lines.length * 15 + 4);
      if (lines.length > 0) {
        pages[pageIndex].push(
          `${rgb(PDF_COLORS.accent)} rg`,
          `${PDF_CONTENT_LEFT.toFixed(2)} ${(cursorY - 5).toFixed(2)} 5 5 re f`,
        );
        addTextLines([lines[0]], PDF_CONTENT_LEFT + 14, "F1", 11, 15, PDF_COLORS.text);
        if (lines.length > 1) {
          addTextLines(lines.slice(1), PDF_CONTENT_LEFT + 14, "F1", 11, 15, PDF_COLORS.text);
        }
      }
      cursorY -= 2;
      return;
    }

    if (block.kind === "code") {
      const codeLines = block.lines.length > 0 ? block.lines : [""];
      const leading = 12;
      const padding = 10;
      const rectHeight = padding * 2 + codeLines.length * leading;
      ensureSpace(rectHeight + 8);
      const rectBottom = cursorY - rectHeight + 4;
      pages[pageIndex].push(
        `${rgb(PDF_COLORS.codeBg)} rg`,
        `${PDF_CONTENT_LEFT.toFixed(2)} ${rectBottom.toFixed(2)} ${PDF_CONTENT_WIDTH.toFixed(2)} ${rectHeight.toFixed(2)} re f`,
        `${rgb(PDF_COLORS.rule)} RG`,
        `${PDF_CONTENT_LEFT.toFixed(2)} ${rectBottom.toFixed(2)} ${PDF_CONTENT_WIDTH.toFixed(2)} ${rectHeight.toFixed(2)} re S`,
      );
      cursorY -= padding;
      addTextLines(codeLines, PDF_CONTENT_LEFT + 10, "F3", 9, leading, PDF_COLORS.text);
      cursorY -= padding + 4;
      return;
    }

    const lines = wrapText(block.text, 84);
    ensureSpace(lines.length * 15 + 6);
    addTextLines(lines, PDF_CONTENT_LEFT, "F1", 11, 15, PDF_COLORS.text);
    cursorY -= 6;
  });

  if (pages.length === 0) newPage();

  const objects: string[] = [];
  const fontRegularId = addObject(objects, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>");
  const fontBoldId = addObject(objects, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>");
  const fontCodeId = addObject(objects, "<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>");
  const pagesId = addObject(objects, "<< /Type /Pages /Count 0 /Kids [] >>");
  const pageRefs: number[] = [];

  pages.forEach((ops) => {
    const stream = ops.join("\n");
    const contentId = addObject(
      objects,
      `<< /Length ${new TextEncoder().encode(stream).length} >>\nstream\n${stream}\nendstream`
    );
    const pageId = addObject(
      objects,
      `<< /Type /Page /Parent ${pagesId} 0 R /MediaBox [0 0 ${PDF_PAGE_WIDTH} ${PDF_PAGE_HEIGHT}] /Resources << /Font << /F1 ${fontRegularId} 0 R /F2 ${fontBoldId} 0 R /F3 ${fontCodeId} 0 R >> >> /Contents ${contentId} 0 R >>`
    );
    pageRefs.push(pageId);
  });

  objects[pagesId - 1] = `<< /Type /Pages /Count ${pageRefs.length} /Kids [${pageRefs.map((pageId) => `${pageId} 0 R`).join(" ")}] >>`;
  const catalogId = addObject(objects, `<< /Type /Catalog /Pages ${pagesId} 0 R >>`);

  let pdf = "%PDF-1.4\n";
  const offsets = [0];
  objects.forEach((object, index) => {
    offsets.push(pdf.length);
    pdf += `${index + 1} 0 obj\n${object}\nendobj\n`;
  });

  const xrefOffset = pdf.length;
  pdf += `xref\n0 ${objects.length + 1}\n`;
  pdf += "0000000000 65535 f \n";
  offsets.slice(1).forEach((offset) => {
    pdf += `${String(offset).padStart(10, "0")} 00000 n \n`;
  });
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root ${catalogId} 0 R >>\nstartxref\n${xrefOffset}\n%%EOF`;
  return new TextEncoder().encode(pdf);
}

export function downloadMarkdownPdf(markdown: string, options?: { fileName?: string; title?: string }) {
  const now = new Date().toISOString().slice(0, 10);
  const title = options?.title?.trim() || "Data Quality Summary";
  const fileName = options?.fileName?.trim() || `${slugify(title)}-${now}.pdf`;
  const pdfBytes = buildStyledPdfDocument(markdown, title);
  const blob = new Blob([pdfBytes], { type: "application/pdf" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName.endsWith(".pdf") ? fileName : `${fileName}.pdf`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}
