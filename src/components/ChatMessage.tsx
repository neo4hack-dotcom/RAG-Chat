import React, { useState } from "react";
import { Bot, User, ChevronDown, ChevronRight, CheckCircle2, CircleDashed, Loader2, XCircle, BrainCircuit, File, Database, Copy, Check, Star, Cpu, FolderOpen, BarChart3 } from "lucide-react";
import { Message, cn, AgentStep, ChartSpec, ChatAction, preprocessMarkdown } from "../lib/utils";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import remarkBreaks from 'remark-breaks';
import rehypeRaw from 'rehype-raw';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import type { LucideIcon } from "lucide-react";

interface ChatMessageProps {
  message: Message;
  onCheckboxToggle?: (messageId: string, text: string, checked: boolean) => void;
  onAction?: (action: ChatAction, message: Message) => void;
  showSteps?: boolean;
}

const AGENT_INTRO_CARD_CONFIG: Record<string, {
  marker: string;
  title: string;
  icon: LucideIcon;
  containerClass: string;
  iconClass: string;
  titleClass: string;
  bodyClass: string;
  subtleClass: string;
}> = {
  manager: {
    marker: "<!-- agent-intro:manager -->",
    title: "Agent Manager",
    icon: Star,
    containerClass: "bg-gradient-to-br from-amber-50 to-orange-50 border border-amber-200/60 dark:from-amber-900/20 dark:to-orange-900/20 dark:border-amber-700/40",
    iconClass: "text-amber-500",
    titleClass: "text-amber-900 dark:text-amber-200",
    bodyClass: "text-amber-800/90 dark:text-amber-300/90",
    subtleClass: "text-amber-700/75 dark:text-amber-300/75",
  },
  clickhouse_query: {
    marker: "<!-- agent-intro:clickhouse_query -->",
    title: "Clickhouse SQL Agent",
    icon: Database,
    containerClass: "bg-gradient-to-br from-cyan-50 to-sky-50 border border-cyan-200/60 dark:from-cyan-900/20 dark:to-sky-900/20 dark:border-cyan-700/40",
    iconClass: "text-cyan-500",
    titleClass: "text-cyan-900 dark:text-cyan-200",
    bodyClass: "text-cyan-800/90 dark:text-cyan-300/90",
    subtleClass: "text-cyan-700/75 dark:text-cyan-300/75",
  },
  data_analyst: {
    marker: "<!-- agent-intro:data_analyst -->",
    title: "Data Analyst Agent",
    icon: Cpu,
    containerClass: "bg-gradient-to-br from-violet-50 to-indigo-50 border border-violet-200/60 dark:from-violet-900/20 dark:to-indigo-900/20 dark:border-violet-700/40",
    iconClass: "text-violet-500",
    titleClass: "text-violet-900 dark:text-violet-200",
    bodyClass: "text-violet-800/90 dark:text-violet-300/90",
    subtleClass: "text-violet-700/75 dark:text-violet-300/75",
  },
  file_management: {
    marker: "<!-- agent-intro:file_management -->",
    title: "File Management Agent",
    icon: FolderOpen,
    containerClass: "bg-gradient-to-br from-emerald-50 to-teal-50 border border-emerald-200/60 dark:from-emerald-900/20 dark:to-teal-900/20 dark:border-emerald-700/40",
    iconClass: "text-emerald-500",
    titleClass: "text-emerald-900 dark:text-emerald-200",
    bodyClass: "text-emerald-800/90 dark:text-emerald-300/90",
    subtleClass: "text-emerald-700/75 dark:text-emerald-300/75",
  },
  pdf_creator: {
    marker: "<!-- agent-intro:pdf_creator -->",
    title: "PDF Creator Agent",
    icon: File,
    containerClass: "bg-gradient-to-br from-slate-50 to-gray-50 border border-slate-200/60 dark:from-slate-900/30 dark:to-gray-900/20 dark:border-slate-700/40",
    iconClass: "text-slate-500",
    titleClass: "text-slate-900 dark:text-slate-200",
    bodyClass: "text-slate-800/90 dark:text-slate-300/90",
    subtleClass: "text-slate-700/75 dark:text-slate-300/75",
  },
  oracle_analyst: {
    marker: "<!-- agent-intro:oracle_analyst -->",
    title: "Oracle SQL Agent",
    icon: Database,
    containerClass: "bg-gradient-to-br from-orange-50 to-amber-50 border border-orange-200/60 dark:from-orange-900/20 dark:to-amber-900/20 dark:border-orange-700/40",
    iconClass: "text-orange-500",
    titleClass: "text-orange-900 dark:text-orange-200",
    bodyClass: "text-orange-800/90 dark:text-orange-300/90",
    subtleClass: "text-orange-700/75 dark:text-orange-300/75",
  },
  data_quality_tables: {
    marker: "<!-- agent-intro:data_quality_tables -->",
    title: "Data Quality - Tables Agent",
    icon: BarChart3,
    containerClass: "bg-gradient-to-br from-fuchsia-50 to-pink-50 border border-fuchsia-200/60 dark:from-fuchsia-900/20 dark:to-pink-900/20 dark:border-fuchsia-700/40",
    iconClass: "text-fuchsia-500",
    titleClass: "text-fuchsia-900 dark:text-fuchsia-200",
    bodyClass: "text-fuchsia-800/90 dark:text-fuchsia-300/90",
    subtleClass: "text-fuchsia-700/75 dark:text-fuchsia-300/75",
  },
};

function getAgentIntroCardData(content: string) {
  const entry = Object.values(AGENT_INTRO_CARD_CONFIG).find((config) => content.includes(config.marker));
  if (!entry) return null;
  const body = content
    .replace(entry.marker, "")
    .replace(/^##\s+.+?\n+/m, "")
    .trim();
  return {
    ...entry,
    body,
  };
}

function extractNodeText(node: React.ReactNode): string {
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(extractNodeText).join("");
  if (React.isValidElement(node)) {
    const element = node as React.ReactElement<{ children?: React.ReactNode }>;
    return extractNodeText(element.props.children);
  }
  return "";
}

function normalizeChoiceLabel(text: string): string {
  return text
    .replace(/^\s*\[(?: |x|X)\]\s*/, "")
    .replace(/\s+/g, " ")
    .trim();
}

function hasTaskListClassName(className: unknown): boolean {
  if (typeof className === "string") return className.includes("task-list-item");
  if (Array.isArray(className)) return className.some((item) => String(item).includes("task-list-item"));
  return false;
}

function stepActionLabel(step: AgentStep): string {
  const rawType = ((step as any).type ?? '').toString().trim();
  if (!rawType) return '';
  return rawType.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
}

function stepReasoning(step: AgentStep): string {
  return ((step as any).reasoning ?? '').toString().trim();
}

function stepResultSummary(step: AgentStep): string {
  return (((step as any).result_summary ?? (step as any).resultSummary) ?? '').toString().trim();
}

function stepSql(step: AgentStep): string {
  return ((step as any).sql ?? '').toString().trim();
}

function stepRowCount(step: AgentStep): number | null {
  const raw = (step as any).row_count ?? (step as any).rowCount;
  return typeof raw === 'number' && Number.isFinite(raw) ? raw : null;
}

function stepSuggestedPath(step: AgentStep): string {
  return (((step as any).suggested_path ?? (step as any).suggestedPath) ?? '').toString().trim();
}

function MessageCopyButton({ text, isUser }: { text: string; isUser: boolean }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    });
  };

  return (
    <button
      type="button"
      onClick={handleCopy}
      className={cn(
        "absolute top-3 right-3 inline-flex items-center justify-center rounded-full p-2 opacity-0 transition-all group-hover:opacity-100",
        isUser
          ? "bg-white/15 text-white/80 hover:bg-white/25 hover:text-white"
          : "bg-black/5 text-gray-500 hover:bg-black/10 hover:text-gray-900 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15 dark:hover:text-white"
      )}
      title={copied ? "Copied" : "Copy message"}
    >
      {copied ? <Check className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
    </button>
  );
}

// ── Copy button for code blocks ──────────────────────────────────────────────

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };
  return (
    <button
      onClick={handleCopy}
      className="absolute top-2 right-2 p-1.5 rounded-md bg-white/10 hover:bg-white/20 text-gray-300 hover:text-white transition-colors"
      title="Copy code"
    >
      {copied ? <Check className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
    </button>
  );
}

function ChartPreview({ chart }: { chart: ChartSpec }) {
  const width = 760;
  const height = 280;
  const margin = { top: 20, right: 20, bottom: 54, left: 52 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const maxPoints = 24;
  const points = chart.points.slice(0, maxPoints);

  if (points.length < 2) return null;

  const allNumericX = points.every((point) => Number.isFinite(Number(point.x)));
  const xNumericValues = allNumericX ? points.map((point) => Number(point.x)) : points.map((_, index) => index);
  const minX = Math.min(...xNumericValues);
  const maxX = Math.max(...xNumericValues);
  const yValues = points.map((point) => point.y);
  const minY = Math.min(0, ...yValues);
  const maxY = Math.max(...yValues);
  const safeYSpan = maxY - minY || 1;
  const safeXSpan = maxX - minX || 1;

  const toX = (value: number) => margin.left + ((value - minX) / safeXSpan) * innerWidth;
  const toY = (value: number) => margin.top + innerHeight - ((value - minY) / safeYSpan) * innerHeight;
  const baselineY = toY(0);

  const svgPoints = points.map((point, index) => ({
    ...point,
    svgX: toX(xNumericValues[index]),
    svgY: toY(point.y),
  }));

  const linePath = svgPoints
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.svgX} ${point.svgY}`)
    .join(" ");
  const areaPath = `${linePath} L ${svgPoints[svgPoints.length - 1].svgX} ${baselineY} L ${svgPoints[0].svgX} ${baselineY} Z`;

  const tickCount = Math.min(6, points.length);
  const tickIndexes = Array.from({ length: tickCount }, (_, i) =>
    Math.round((i * (points.length - 1)) / Math.max(1, tickCount - 1))
  );
  const uniqueTickIndexes = Array.from(new Set(tickIndexes));
  const yTicks = Array.from({ length: 4 }, (_, i) => minY + (safeYSpan * i) / 3);

  return (
    <div className="mt-4 rounded-2xl border border-gray-200/70 dark:border-gray-700/70 bg-white/80 dark:bg-gray-900/50 p-4 shadow-sm">
      <div className="flex items-center justify-between gap-3 mb-3">
        <div>
          <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100">{chart.title}</h4>
          <p className="text-[11px] text-gray-500 dark:text-gray-400">
            X: {chart.xField} · Y: {chart.yField} · Type: {chart.type}
          </p>
        </div>
      </div>
      <div className="w-full overflow-x-auto">
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full min-w-[620px]">
          <rect x="0" y="0" width={width} height={height} rx="18" className="fill-slate-50 dark:fill-slate-950/60" />

          {yTicks.map((tick) => (
            <g key={tick}>
              <line
                x1={margin.left}
                x2={width - margin.right}
                y1={toY(tick)}
                y2={toY(tick)}
                className="stroke-gray-200 dark:stroke-gray-800"
                strokeDasharray="4 6"
              />
              <text
                x={margin.left - 10}
                y={toY(tick) + 4}
                textAnchor="end"
                className="fill-gray-500 dark:fill-gray-400 text-[10px]"
              >
                {Number(tick.toFixed(2)).toString()}
              </text>
            </g>
          ))}

          <line
            x1={margin.left}
            x2={width - margin.right}
            y1={baselineY}
            y2={baselineY}
            className="stroke-gray-300 dark:stroke-gray-700"
          />

          {chart.type === 'bar' && svgPoints.map((point) => {
            const barWidth = Math.max(12, innerWidth / Math.max(1, svgPoints.length * 1.8));
            const barHeight = Math.abs(point.svgY - baselineY);
            const y = point.y >= 0 ? point.svgY : baselineY;
            return (
              <g key={`${point.x}-${point.y}`}>
                <rect
                  x={point.svgX - barWidth / 2}
                  y={y}
                  width={barWidth}
                  height={Math.max(1, barHeight)}
                  rx="8"
                  className="fill-sky-500/85 dark:fill-sky-400/80"
                />
              </g>
            );
          })}

          {(chart.type === 'line' || chart.type === 'area') && (
            <>
              {chart.type === 'area' && (
                <path d={areaPath} className="fill-cyan-400/20 dark:fill-cyan-300/20" />
              )}
              <path
                d={linePath}
                fill="none"
                className="stroke-cyan-600 dark:stroke-cyan-300"
                strokeWidth="3"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              {svgPoints.map((point) => (
                <circle
                  key={`${point.x}-${point.y}`}
                  cx={point.svgX}
                  cy={point.svgY}
                  r="4"
                  className="fill-white dark:fill-slate-950 stroke-cyan-600 dark:stroke-cyan-300"
                  strokeWidth="2"
                />
              ))}
            </>
          )}

          {chart.type === 'scatter' && svgPoints.map((point) => (
            <circle
              key={`${point.x}-${point.y}`}
              cx={point.svgX}
              cy={point.svgY}
              r="5"
              className="fill-fuchsia-500/80 dark:fill-fuchsia-400/80"
            />
          ))}

          {uniqueTickIndexes.map((index) => {
            const point = svgPoints[index];
            if (!point) return null;
            const label = point.x.length > 16 ? `${point.x.slice(0, 16)}…` : point.x;
            return (
              <g key={`${point.x}-${index}`}>
                <line
                  x1={point.svgX}
                  x2={point.svgX}
                  y1={baselineY}
                  y2={baselineY + 6}
                  className="stroke-gray-300 dark:stroke-gray-700"
                />
                <text
                  x={point.svgX}
                  y={height - 20}
                  textAnchor="middle"
                  className="fill-gray-500 dark:fill-gray-400 text-[10px]"
                >
                  {label}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      {chart.points.length > maxPoints && (
        <p className="mt-2 text-[11px] text-gray-500 dark:text-gray-400">
          Showing the first {maxPoints} points out of {chart.points.length}.
        </p>
      )}
    </div>
  );
}

function buildAgentIntroComponents(bodyClass: string, subtleClass: string) {
  return {
    p: ({ children, ...props }: any) => (
      <p className={`text-[11px] leading-relaxed mb-1.5 last:mb-0 ${bodyClass}`} {...props}>
        {children}
      </p>
    ),
    ul: ({ children, ...props }: any) => (
      <ul className={`list-disc pl-4 space-y-0.5 ${bodyClass}`} {...props}>
        {children}
      </ul>
    ),
    li: ({ children, ...props }: any) => (
      <li className="text-[11px] leading-relaxed" {...props}>
        {children}
      </li>
    ),
    strong: ({ children, ...props }: any) => (
      <strong className={`font-semibold ${subtleClass}`} {...props}>
        {children}
      </strong>
    ),
    em: ({ children, ...props }: any) => (
      <em className={`italic ${subtleClass}`} {...props}>
        {children}
      </em>
    ),
  };
}

// ── Markdown components ───────────────────────────────────────────────────────

function buildComponents(messageId: string, onCheckboxToggle?: (id: string, text: string, checked: boolean) => void) {
  return {
    // Code blocks with syntax highlighting
    code({ node, inline, className, children, ...props }: any) {
      const match = /language-(\w+)/.exec(className || '');
      const codeString = String(children).replace(/\n$/, '');
      if (!inline && match) {
        return (
          <div className="relative my-4 rounded-xl overflow-hidden shadow-md">
            <div className="flex items-center justify-between px-4 py-2 bg-[#1e1e2e] border-b border-white/10">
              <span className="text-xs font-mono text-gray-400 uppercase tracking-wider">{match[1]}</span>
              <CopyButton text={codeString} />
            </div>
            <SyntaxHighlighter
              style={oneDark}
              language={match[1]}
              PreTag="div"
              customStyle={{ margin: 0, borderRadius: 0, fontSize: '0.82rem', padding: '1rem' }}
              {...props}
            >
              {codeString}
            </SyntaxHighlighter>
          </div>
        );
      }
      if (!inline) {
        // Code block without language hint
        return (
          <div className="relative my-4 rounded-xl overflow-hidden shadow-md">
            <div className="flex items-center justify-between px-4 py-2 bg-[#1e1e2e] border-b border-white/10">
              <span className="text-xs font-mono text-gray-400">code</span>
              <CopyButton text={codeString} />
            </div>
            <SyntaxHighlighter
              style={oneDark}
              PreTag="div"
              customStyle={{ margin: 0, borderRadius: 0, fontSize: '0.82rem', padding: '1rem' }}
              {...props}
            >
              {codeString}
            </SyntaxHighlighter>
          </div>
        );
      }
      return (
        <code className="bg-gray-100 dark:bg-gray-800 text-pink-600 dark:text-pink-400 px-1.5 py-0.5 rounded-md text-[0.82em] font-mono border border-gray-200 dark:border-gray-700" {...props}>
          {children}
        </code>
      );
    },

    // Tables with sticky header and horizontal scroll
    table({ children, ...props }: any) {
      return (
        <div className="my-4 overflow-x-auto rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm" {...props}>
            {children}
          </table>
        </div>
      );
    },
    thead({ children, ...props }: any) {
      return (
        <thead className="bg-gray-50 dark:bg-gray-800/80" {...props}>{children}</thead>
      );
    },
    tbody({ children, ...props }: any) {
      return (
        <tbody className="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900/50" {...props}>{children}</tbody>
      );
    },
    th({ children, ...props }: any) {
      return (
        <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-600 dark:text-gray-300 uppercase tracking-wider whitespace-nowrap" {...props}>
          {children}
        </th>
      );
    },
    td({ children, ...props }: any) {
      return (
        <td className="px-4 py-2.5 text-gray-700 dark:text-gray-300 align-top" {...props}>
          {children}
        </td>
      );
    },
    tr({ children, ...props }: any) {
      return (
        <tr className="hover:bg-gray-50/60 dark:hover:bg-white/5 transition-colors" {...props}>
          {children}
        </tr>
      );
    },

    // Headings
    h1: ({ children, ...props }: any) => <h1 className="text-[1.9rem] font-semibold tracking-tight text-gray-900 dark:text-gray-100 mt-6 mb-3 leading-tight" {...props}>{children}</h1>,
    h2: ({ children, ...props }: any) => <h2 className="text-[1.1rem] font-semibold text-gray-900 dark:text-gray-100 mt-6 mb-3 leading-tight" {...props}>{children}</h2>,
    h3: ({ children, ...props }: any) => <h3 className="text-[0.98rem] font-semibold text-gray-800 dark:text-gray-200 mt-4 mb-2" {...props}>{children}</h3>,
    h4: ({ children, ...props }: any) => <h4 className="text-sm font-semibold uppercase tracking-[0.12em] text-gray-500 dark:text-gray-400 mt-4 mb-1.5" {...props}>{children}</h4>,

    // Paragraphs
    p: ({ children, ...props }: any) => <p className="text-[14px] leading-[1.8] text-gray-800 dark:text-gray-200 mb-3 last:mb-0" {...props}>{children}</p>,

    // Lists
    ul: ({ children, ...props }: any) => <ul className="list-none pl-0 mb-3 space-y-1" {...props}>{children}</ul>,
    ol: ({ children, ...props }: any) => <ol className="list-decimal pl-5 mb-3 space-y-1 text-[14px] text-gray-800 dark:text-gray-200" {...props}>{children}</ol>,
    li({ node, checked, className, children, ...props }: any) {
      const rawText = extractNodeText(children).trim();
      const choiceLabel = normalizeChoiceLabel(rawText);
      const looksLikeUncheckedTask = /^\[(?: |x|X)\]\s+/.test(rawText);
      const isTask = (checked !== null && checked !== undefined) || hasTaskListClassName(className) || looksLikeUncheckedTask;
      if (isTask) {
        return (
          <li className="list-none inline-block mr-2 mb-2 align-top" {...props}>
            <button
              type="button"
              onMouseDown={(event) => {
                event.preventDefault();
                event.stopPropagation();
              }}
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                onCheckboxToggle?.(messageId, choiceLabel || rawText, true);
              }}
              className="group pointer-events-auto inline-flex max-w-full items-center gap-2 rounded-md border border-cyan-200 bg-cyan-50 px-3 py-1.5 text-left text-[11px] font-medium text-cyan-700 shadow-sm transition-all hover:bg-cyan-100 hover:border-cyan-300 dark:border-cyan-700 dark:bg-cyan-900/30 dark:text-cyan-300 dark:hover:bg-cyan-900/45 dark:hover:border-cyan-500"
              aria-label={choiceLabel || rawText}
            >
              <span className="h-1.5 w-1.5 rounded-full bg-cyan-400 dark:bg-cyan-300 flex-shrink-0" />
              <span className="truncate">{choiceLabel || rawText}</span>
            </button>
          </li>
        );
      }
      return (
        <li className="flex items-start gap-2 text-[14px] text-gray-800 dark:text-gray-200" {...props}>
          <span className="mt-2 w-1.5 h-1.5 rounded-full bg-gray-400 dark:bg-gray-500 flex-shrink-0" />
          <span>{children}</span>
        </li>
      );
    },

    // Checkbox inputs
    input({ node, checked, type, ...props }: any) {
      if (type === 'checkbox') {
        return null;
      }
      return <input type={type} {...props} />;
    },

    // Blockquote
    blockquote: ({ children, ...props }: any) => (
      <blockquote className="relative overflow-hidden rounded-[1.4rem] border border-sky-200/80 bg-gradient-to-br from-sky-50 to-white px-4 py-3 my-4 text-gray-700 dark:border-sky-800/60 dark:from-sky-950/30 dark:to-transparent dark:text-gray-200 shadow-sm" {...props}>
        <div className="absolute inset-y-0 left-0 w-1.5 bg-gradient-to-b from-sky-400 to-cyan-500 dark:from-sky-500 dark:to-cyan-400" />
        <div className="pl-2 text-[14px] leading-[1.75]">{children}</div>
      </blockquote>
    ),

    details: ({ children, ...props }: any) => (
      <details className="pointer-events-auto my-4 overflow-hidden rounded-[1.35rem] border border-gray-200/80 bg-white/75 shadow-sm dark:border-gray-700/80 dark:bg-gray-900/45" {...props}>
        {children}
      </details>
    ),
    summary: ({ children, ...props }: any) => (
      <summary className="pointer-events-auto cursor-pointer list-none px-4 py-3 text-sm font-semibold text-gray-900 dark:text-gray-100 transition-colors hover:bg-black/5 dark:hover:bg-white/5 [&::-webkit-details-marker]:hidden" {...props}>
        {children}
      </summary>
    ),
    section: ({ children, ...props }: any) => (
      <section className="my-4 rounded-[1.45rem] border border-gray-200/80 bg-white/70 px-4 py-4 shadow-sm dark:border-gray-700/80 dark:bg-gray-900/35" {...props}>
        {children}
      </section>
    ),
    article: ({ children, ...props }: any) => (
      <article className="my-4 rounded-[1.45rem] border border-slate-200/80 bg-gradient-to-br from-white to-slate-50 px-4 py-4 shadow-sm dark:border-slate-700/80 dark:from-slate-900/60 dark:to-slate-950/40" {...props}>
        {children}
      </article>
    ),

    // Horizontal rule
    hr: ({ ...props }: any) => <hr className="my-4 border-gray-200 dark:border-gray-700" {...props} />,

    // Links
    a: ({ href, children, ...props }: any) => (
      <a href={href} target="_blank" rel="noopener noreferrer" className="text-blue-600 dark:text-blue-400 underline underline-offset-2 hover:text-blue-800 dark:hover:text-blue-300 transition-colors" {...props}>
        {children}
      </a>
    ),

    // Strong / em
    strong: ({ children, ...props }: any) => <strong className="font-semibold text-gray-900 dark:text-gray-100" {...props}>{children}</strong>,
    em: ({ children, ...props }: any) => <em className="italic text-gray-700 dark:text-gray-300" {...props}>{children}</em>,

    // Pre (fallback, wraps code)
    pre: ({ children, ...props }: any) => <>{children}</>,
  };
}

// ── Main component ────────────────────────────────────────────────────────────

export function ChatMessage({ message, onCheckboxToggle, onAction, showSteps = true }: ChatMessageProps) {
  const isUser = message.role === "user";

  const [isStepsExpanded, setIsStepsExpanded] = useState(false);
  const [isSourcesExpanded, setIsSourcesExpanded] = useState(false);

  const renderStepIcon = (status: AgentStep['status']) => {
    switch (status) {
      case 'success': return <CheckCircle2 className="w-4 h-4 text-emerald-500" />;
      case 'running': return <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />;
      case 'error': return <XCircle className="w-4 h-4 text-red-500" />;
      default: return <CircleDashed className="w-4 h-4 text-gray-400" />;
    }
  };

  const content = message.content;
  const renderedContent = !isUser ? preprocessMarkdown(content) : content;
  const agentIntroCard = !isUser ? getAgentIntroCardData(content) : null;

  if (agentIntroCard) {
    const Icon = agentIntroCard.icon;
    return (
      <div className={cn("max-w-[77rem] mx-auto mb-4 p-3 rounded-xl shadow-sm animate-fade-in-up", agentIntroCard.containerClass)}>
        <div className="flex items-center gap-2 mb-1.5">
          <Icon className={cn("w-4 h-4", agentIntroCard.iconClass)} />
          <h3 className={cn("font-semibold text-[13px]", agentIntroCard.titleClass)}>
            {agentIntroCard.title}
          </h3>
        </div>
        <div className="space-y-1">
          <ReactMarkdown
            remarkPlugins={[remarkGfm, remarkBreaks]}
            components={buildAgentIntroComponents(agentIntroCard.bodyClass, agentIntroCard.subtleClass) as any}
          >
            {agentIntroCard.body}
          </ReactMarkdown>
        </div>
      </div>
    );
  }

  return (
    <div className={cn("flex gap-3 w-full max-w-[77rem] mx-auto mb-5 animate-fade-in-up", isUser ? "flex-row-reverse" : "flex-row")}>
      <div className={cn(
        "flex-shrink-0 w-9 h-9 rounded-xl flex items-center justify-center shadow-sm",
        isUser
          ? "bg-gradient-to-tr from-blue-500 to-blue-600 text-white"
          : "bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-700 dark:text-gray-300"
      )}>
        {isUser ? <User className="w-4.5 h-4.5" /> : <Bot className="w-4.5 h-4.5" />}
      </div>

      <div className={cn(
        "group pointer-events-auto relative px-5 py-3 rounded-[1.7rem]",
        isUser
          ? "max-w-[88%] bg-blue-500 text-white rounded-tr-sm shadow-md shadow-blue-500/10"
          : "glass-panel rounded-tl-sm w-full"
      )}>
        <MessageCopyButton text={content} isUser={isUser} />

        {/* Agent Thinking Steps */}
        {showSteps && !isUser && message.steps && message.steps.length > 0 && (
          <div className="mb-3 bg-white/60 dark:bg-gray-800/60 border border-gray-200/60 dark:border-gray-700/60 rounded-xl overflow-hidden shadow-sm">
            <button
              onClick={() => setIsStepsExpanded(!isStepsExpanded)}
              className="w-full flex items-center justify-between px-4 py-2.5 hover:bg-white/40 dark:hover:bg-white/5 transition-colors"
            >
              <div className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200">
                <BrainCircuit className="w-4 h-4 text-purple-500" />
                Agent Thinking Process ({message.steps.filter(s => s.status === 'success').length}/{message.steps.length})
              </div>
              {isStepsExpanded ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
            </button>
            {isStepsExpanded && (
              <div className="px-4 pb-3 pt-1 border-t border-gray-100/50">
                <div className="space-y-2.5 mt-2">
                  {message.steps.map((step, idx) => (
                    <div key={step.id || idx} className="flex items-start gap-3">
                      <div className="mt-0.5">{renderStepIcon(step.status)}</div>
                      <div className="flex-1">
                        <p className={cn("text-[13px] font-medium", step.status === 'error' ? "text-red-700 dark:text-red-400" : "text-gray-800 dark:text-gray-200")}>
                          {step.title}
                        </p>
                        {(stepActionLabel(step) || stepRowCount(step) !== null || (step as any).retried || stepSuggestedPath(step)) && (
                          <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px]">
                            {stepActionLabel(step) && (
                              <span className="inline-flex items-center rounded-full bg-purple-100 px-2 py-0.5 font-medium text-purple-700 dark:bg-purple-900/30 dark:text-purple-200">
                                {stepActionLabel(step)}
                              </span>
                            )}
                            {stepRowCount(step) !== null && (
                              <span className="text-gray-500 dark:text-gray-400">
                                {stepRowCount(step)} row(s)
                              </span>
                            )}
                            {(step as any).retried && (
                              <span className="text-amber-600 dark:text-amber-300">
                                Auto-retried
                              </span>
                            )}
                            {stepSuggestedPath(step) && (
                              <span className="text-gray-500 dark:text-gray-400">
                                {stepSuggestedPath(step)}
                              </span>
                            )}
                          </div>
                        )}
                        {stepReasoning(step) && (
                          <p className="text-[12px] text-gray-600 dark:text-gray-300 mt-2 leading-relaxed">
                            {stepReasoning(step)}
                          </p>
                        )}
                        {stepResultSummary(step) && (
                          <p className="text-[12px] text-gray-500 dark:text-gray-400 mt-1 leading-relaxed whitespace-pre-wrap">
                            {stepResultSummary(step)}
                          </p>
                        )}
                        {stepSql(step) && (
                          <div className="relative mt-2 overflow-hidden rounded-xl border border-gray-200 bg-gray-950/95 dark:border-gray-700">
                            <div className="flex items-center justify-between border-b border-white/10 px-3 py-2">
                              <span className="text-[11px] font-medium uppercase tracking-wide text-gray-400">SQL</span>
                              <CopyButton text={stepSql(step)} />
                            </div>
                            <pre className="overflow-x-auto px-3 py-3 text-[11px] leading-relaxed text-gray-100">
                              <code>{stepSql(step)}</code>
                            </pre>
                          </div>
                        )}
                        {step.details && !stepReasoning(step) && !stepResultSummary(step) && !stepSql(step) && (
                          <p className="text-[12px] text-gray-500 dark:text-gray-400 mt-1 leading-relaxed whitespace-pre-wrap font-mono">
                            {step.details}
                          </p>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Attachments */}
        {message.attachments && message.attachments.length > 0 && (
          <div className="flex flex-wrap gap-2 mb-2.5">
            {message.attachments.map(att =>
              att.type.startsWith('image/') ? (
                <img key={att.id} src={att.data} alt={att.name} className="max-w-xs max-h-64 object-contain rounded-lg border border-white/20 shadow-sm" />
              ) : (
                <div key={att.id} className="flex items-center gap-2 bg-white/10 px-3 py-2 rounded-lg text-sm border border-white/20">
                  <File className="w-4 h-4" /><span className="truncate max-w-[150px]">{att.name}</span>
                </div>
              )
            )}
          </div>
        )}

        {/* Message Content */}
        {isUser ? (
          <div className="whitespace-pre-wrap text-[14px] leading-[1.65]">{content}</div>
        ) : (
          <div className="markdown-body pointer-events-auto">
            <ReactMarkdown
              remarkPlugins={[remarkGfm, remarkBreaks]}
              rehypePlugins={[rehypeRaw]}
              components={buildComponents(message.id, onCheckboxToggle) as any}
            >
              {renderedContent}
            </ReactMarkdown>
          </div>
        )}

        {!isUser && message.chart && (
          <ChartPreview chart={message.chart} />
        )}

        {!isUser && message.actions && message.actions.length > 0 && (
          <div className="mt-3 flex flex-wrap gap-2">
            {message.actions.map((action) => (
              <button
                key={action.id}
                type="button"
                onClick={() => onAction?.(action, message)}
                className={cn(
                  "inline-flex items-center gap-2 px-3.5 py-1.5 rounded-2xl text-[13px] font-medium transition-colors",
                  action.variant === 'primary'
                    ? "bg-black text-white hover:bg-gray-800"
                    : "border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800"
                )}
              >
                {action.label}
              </button>
            ))}
          </div>
        )}

        {/* RAG low-confidence warning */}
        {!isUser && message.confidence !== undefined && message.confidence < 0.4 && (
          <div className="mt-3 p-2.5 bg-amber-50 dark:bg-amber-900/30 border border-amber-200 dark:border-amber-700 rounded-lg flex items-start gap-2 text-amber-800 dark:text-amber-200 text-xs">
            <XCircle className="w-4 h-4 text-amber-500 flex-shrink-0 mt-0.5" />
            <p><strong>Warning:</strong> Low-relevance documents detected (score: {Math.round(message.confidence * 100)}%). The answer may be imprecise.</p>
          </div>
        )}

        {/* RAG Sources Inspector */}
        {!isUser && message.sources && message.sources.length > 0 && (
          <div className="mt-3 border-t border-gray-200/60 dark:border-gray-700/60 pt-2.5">
            <button
              onClick={() => setIsSourcesExpanded(!isSourcesExpanded)}
              className="flex items-center gap-2 text-xs font-medium text-gray-500 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200 transition-colors"
            >
              <Database className="w-3.5 h-3.5" />
              RAG Inspector ({message.sources.length} sources)
              {isSourcesExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            </button>
            {isSourcesExpanded && (
              <div className="mt-3 space-y-2">
                {message.sources.map((source, idx) => (
                  <div key={source.id} className="bg-white/60 dark:bg-gray-800/60 border border-gray-200 dark:border-gray-700 rounded-lg p-3 text-xs">
                    <div className="flex items-center justify-between mb-1.5">
                      <span className="font-semibold text-gray-700 dark:text-gray-200 flex items-center gap-1.5">
                        <span className="bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 px-1.5 py-0.5 rounded text-[10px]">[{idx + 1}]</span>
                        {source.docName}
                      </span>
                      <span className={cn("font-mono text-[10px] px-1.5 py-0.5 rounded", source.score > 0.6 ? "bg-emerald-100 dark:bg-emerald-900 text-emerald-700 dark:text-emerald-300" : "bg-amber-100 dark:bg-amber-900 text-amber-700 dark:text-amber-300")}>
                        Score: {Math.round(source.score * 100)}%
                      </span>
                    </div>
                    <p className="text-gray-600 dark:text-gray-400 line-clamp-3 hover:line-clamp-none transition-all duration-300 leading-relaxed">
                      {source.text}
                    </p>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
