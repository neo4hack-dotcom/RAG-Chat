function slugify(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[^\x00-\x7F]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    || "report";
}

function normalizeMarkdownToPlainText(markdown: string): string {
  return markdown
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/\r/g, "")
    .replace(/^```[^\n]*\n/gm, "")
    .replace(/^```$/gm, "")
    .replace(/^######\s+/gm, "")
    .replace(/^#####\s+/gm, "")
    .replace(/^####\s+/gm, "")
    .replace(/^###\s+/gm, "")
    .replace(/^##\s+/gm, "")
    .replace(/^#\s+/gm, "")
    .replace(/^\s*[-*]\s+\[[ xX]\]\s+/gm, "- ")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/\*([^*]+)\*/g, "$1")
    .replace(/^\s*>\s?/gm, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function sanitizePdfText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[^\x20-\x7E\n]/g, "")
    .replace(/\\/g, "\\\\")
    .replace(/\(/g, "\\(")
    .replace(/\)/g, "\\)");
}

function wrapLine(line: string, maxChars: number): string[] {
  const trimmed = line.trimEnd();
  if (!trimmed) return [""];
  if (trimmed.length <= maxChars) return [trimmed];

  const words = trimmed.split(/\s+/);
  const output: string[] = [];
  let current = "";

  words.forEach((word) => {
    if (!word) return;
    if (!current) {
      current = word;
      return;
    }
    if (`${current} ${word}`.length <= maxChars) {
      current = `${current} ${word}`;
      return;
    }
    output.push(current);
    current = word;
  });

  if (current) output.push(current);
  return output;
}

function buildWrappedLines(text: string, maxChars = 90): string[] {
  return text
    .split("\n")
    .flatMap((line) => wrapLine(line, maxChars));
}

type PdfPage = {
  lines: string[];
};

function paginateLines(lines: string[], linesPerPage: number): PdfPage[] {
  const pages: PdfPage[] = [];
  for (let index = 0; index < lines.length; index += linesPerPage) {
    pages.push({ lines: lines.slice(index, index + linesPerPage) });
  }
  return pages.length > 0 ? pages : [{ lines: [""] }];
}

function buildPdfDocument(lines: string[], title: string): Uint8Array {
  const pageWidth = 595.28;
  const pageHeight = 841.89;
  const margin = 48;
  const fontSize = 11;
  const leading = 15;
  const linesPerPage = Math.max(1, Math.floor((pageHeight - margin * 2) / leading) - 2);
  const pages = paginateLines(lines, linesPerPage);
  const objects: string[] = [];
  const addObject = (content: string) => {
    objects.push(content);
    return objects.length;
  };

  const fontId = addObject("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>");
  const pagesId = addObject("<< /Type /Pages /Count 0 /Kids [] >>");
  const pageRefs: number[] = [];

  pages.forEach((page, pageIndex) => {
    const titleLine = pageIndex === 0 ? [title, "", ...page.lines] : [`${title} (continued)`, "", ...page.lines];
    const streamLines = titleLine.map((line) => sanitizePdfText(line));
    const yStart = pageHeight - margin;
    const contentStream = [
      "BT",
      `/F1 ${fontSize} Tf`,
      `${margin} ${yStart} Td`,
      `${leading} TL`,
      ...streamLines.map((line, index) => `${index === 0 ? "" : "T*\n"}(${line}) Tj`).flatMap((item) => item.split("\n")),
      "ET",
    ].join("\n");
    const contentId = addObject(
      `<< /Length ${contentStream.length} >>\nstream\n${contentStream}\nendstream`
    );
    const pageId = addObject(
      `<< /Type /Page /Parent ${pagesId} 0 R /MediaBox [0 0 ${pageWidth} ${pageHeight}] /Resources << /Font << /F1 ${fontId} 0 R >> >> /Contents ${contentId} 0 R >>`
    );
    pageRefs.push(pageId);
  });

  objects[pagesId - 1] = `<< /Type /Pages /Count ${pageRefs.length} /Kids [${pageRefs.map((id) => `${id} 0 R`).join(" ")}] >>`;
  const catalogId = addObject(`<< /Type /Catalog /Pages ${pagesId} 0 R >>`);

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
  const plainText = normalizeMarkdownToPlainText(markdown);
  const wrappedLines = buildWrappedLines(plainText);
  const pdfBytes = buildPdfDocument(wrappedLines, title);
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
