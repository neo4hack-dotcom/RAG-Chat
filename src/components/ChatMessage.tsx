import React, { useState } from "react";
import { Bot, User, ChevronDown, ChevronRight, CheckCircle2, CircleDashed, Loader2, XCircle, BrainCircuit, File, Database, Copy, Check } from "lucide-react";
import { Message, cn, AgentStep } from "../lib/utils";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import remarkBreaks from 'remark-breaks';
import rehypeRaw from 'rehype-raw';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

interface ChatMessageProps {
  message: Message;
  onCheckboxToggle?: (messageId: string, text: string, checked: boolean) => void;
  showSteps?: boolean;
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

// ── Detect if content is predominantly HTML ───────────────────────────────────

function isHtmlContent(content: string): boolean {
  const stripped = content.trim();
  // If it starts with an HTML tag and contains closing tags, treat as HTML
  return /^<[a-zA-Z][\s\S]*>[\s\S]*<\/[a-zA-Z]>/.test(stripped);
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
    h1: ({ children, ...props }: any) => <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100 mt-6 mb-3 leading-tight" {...props}>{children}</h1>,
    h2: ({ children, ...props }: any) => <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mt-5 mb-2 leading-tight" {...props}>{children}</h2>,
    h3: ({ children, ...props }: any) => <h3 className="text-base font-semibold text-gray-800 dark:text-gray-200 mt-4 mb-1.5" {...props}>{children}</h3>,
    h4: ({ children, ...props }: any) => <h4 className="text-sm font-semibold text-gray-800 dark:text-gray-200 mt-3 mb-1" {...props}>{children}</h4>,

    // Paragraphs
    p: ({ children, ...props }: any) => <p className="text-[14px] leading-relaxed text-gray-800 dark:text-gray-200 mb-3 last:mb-0" {...props}>{children}</p>,

    // Lists
    ul: ({ children, ...props }: any) => <ul className="list-none pl-0 mb-3 space-y-1" {...props}>{children}</ul>,
    ol: ({ children, ...props }: any) => <ol className="list-decimal pl-5 mb-3 space-y-1 text-[14px] text-gray-800 dark:text-gray-200" {...props}>{children}</ol>,
    li({ node, checked, className, children, ...props }: any) {
      const isTask = checked !== null && checked !== undefined;
      if (isTask) {
        let text = '';
        const extractText = (c: React.ReactNode): string => {
          if (typeof c === 'string') return c;
          if (React.isValidElement(c)) {
            const el = c as React.ReactElement<{ children?: React.ReactNode }>;
            if (el.props.children) return extractText(el.props.children);
          }
          if (Array.isArray(c)) return c.map(extractText).join('');
          return '';
        };
        text = extractText(children).trim();
        return (
          <li
            className="flex items-start gap-2 text-[14px] text-gray-800 dark:text-gray-200 cursor-pointer hover:bg-gray-50 dark:hover:bg-white/5 rounded-lg px-2 py-1 -mx-2 transition-colors"
            onClick={(e) => {
              if ((e.target as HTMLElement).tagName !== 'INPUT') {
                const input = e.currentTarget.querySelector('input');
                if (input) { input.checked = !input.checked; onCheckboxToggle?.(messageId, text, input.checked); }
              }
            }}
            {...props}
          >
            {children}
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
        return (
          <input
            type="checkbox"
            defaultChecked={checked}
            onChange={(e) => {
              const li = e.target.closest('li');
              onCheckboxToggle?.(messageId, (li?.textContent || '').trim(), e.target.checked);
            }}
            className="mt-1 w-4 h-4 rounded border-gray-300 dark:border-gray-600 text-blue-500 cursor-pointer flex-shrink-0"
            {...props}
          />
        );
      }
      return <input type={type} {...props} />;
    },

    // Blockquote
    blockquote: ({ children, ...props }: any) => (
      <blockquote className="border-l-4 border-blue-400 dark:border-blue-600 pl-4 py-1 my-3 bg-blue-50/50 dark:bg-blue-900/10 rounded-r-lg text-gray-700 dark:text-gray-300 italic text-[14px]" {...props}>
        {children}
      </blockquote>
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

export function ChatMessage({ message, onCheckboxToggle, showSteps = true }: ChatMessageProps) {
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
  const htmlMode = !isUser && isHtmlContent(content);

  return (
    <div className={cn("flex gap-4 w-full max-w-4xl mx-auto mb-8 animate-fade-in-up", isUser ? "flex-row-reverse" : "flex-row")}>
      <div className={cn(
        "flex-shrink-0 w-10 h-10 rounded-2xl flex items-center justify-center shadow-sm",
        isUser
          ? "bg-gradient-to-tr from-blue-500 to-blue-600 text-white"
          : "bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-700 dark:text-gray-300"
      )}>
        {isUser ? <User className="w-5 h-5" /> : <Bot className="w-5 h-5" />}
      </div>

      <div className={cn(
        "max-w-[85%] px-6 py-4 rounded-[2rem]",
        isUser
          ? "bg-blue-500 text-white rounded-tr-sm shadow-md shadow-blue-500/10"
          : "glass-panel rounded-tl-sm w-full"
      )}>
        {/* Agent Thinking Steps */}
        {showSteps && !isUser && message.steps && message.steps.length > 0 && (
          <div className="mb-4 bg-white/60 dark:bg-gray-800/60 border border-gray-200/60 dark:border-gray-700/60 rounded-xl overflow-hidden shadow-sm">
            <button
              onClick={() => setIsStepsExpanded(!isStepsExpanded)}
              className="w-full flex items-center justify-between px-4 py-3 hover:bg-white/40 dark:hover:bg-white/5 transition-colors"
            >
              <div className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200">
                <BrainCircuit className="w-4 h-4 text-purple-500" />
                Agent Thinking Process ({message.steps.filter(s => s.status === 'success').length}/{message.steps.length})
              </div>
              {isStepsExpanded ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
            </button>
            {isStepsExpanded && (
              <div className="px-4 pb-4 pt-1 border-t border-gray-100/50">
                <div className="space-y-3 mt-2">
                  {message.steps.map((step, idx) => (
                    <div key={step.id || idx} className="flex items-start gap-3">
                      <div className="mt-0.5">{renderStepIcon(step.status)}</div>
                      <div className="flex-1">
                        <p className={cn("text-[13px] font-medium", step.status === 'error' ? "text-red-700 dark:text-red-400" : "text-gray-800 dark:text-gray-200")}>
                          {step.title}
                        </p>
                        {step.details && (
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
          <div className="flex flex-wrap gap-2 mb-3">
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
          <div className="whitespace-pre-wrap text-[14px] leading-relaxed">{content}</div>
        ) : htmlMode ? (
          // Raw HTML content (e.g. some local LLMs output HTML directly)
          <div
            className="markdown-body prose-sm prose dark:prose-invert max-w-none"
            dangerouslySetInnerHTML={{ __html: content }}
          />
        ) : (
          <div className="markdown-body">
            <ReactMarkdown
              remarkPlugins={[remarkGfm, remarkBreaks]}
              rehypePlugins={[rehypeRaw]}
              components={buildComponents(message.id, onCheckboxToggle) as any}
            >
              {content}
            </ReactMarkdown>
          </div>
        )}

        {/* RAG low-confidence warning */}
        {!isUser && message.confidence !== undefined && message.confidence < 0.4 && (
          <div className="mt-4 p-3 bg-amber-50 dark:bg-amber-900/30 border border-amber-200 dark:border-amber-700 rounded-lg flex items-start gap-2 text-amber-800 dark:text-amber-200 text-xs">
            <XCircle className="w-4 h-4 text-amber-500 flex-shrink-0 mt-0.5" />
            <p><strong>Attention :</strong> Documents peu pertinents (score: {Math.round(message.confidence * 100)}%). La réponse peut être imprécise.</p>
          </div>
        )}

        {/* RAG Sources Inspector */}
        {!isUser && message.sources && message.sources.length > 0 && (
          <div className="mt-4 border-t border-gray-200/60 dark:border-gray-700/60 pt-3">
            <button
              onClick={() => setIsSourcesExpanded(!isSourcesExpanded)}
              className="flex items-center gap-2 text-xs font-medium text-gray-500 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200 transition-colors"
            >
              <Database className="w-3.5 h-3.5" />
              Inspecteur RAG ({message.sources.length} sources)
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
