import React, { useState } from "react";
import { Bot, User, ChevronDown, ChevronRight, CheckCircle2, CircleDashed, Loader2, XCircle, BrainCircuit, File, Database } from "lucide-react";
import { Message, cn, AgentStep, preprocessMarkdown } from "../lib/utils";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface ChatMessageProps {
  message: Message;
  onCheckboxToggle?: (messageId: string, text: string, checked: boolean) => void;
  showSteps?: boolean;
}

/**
 * ChatMessage Component
 * Renders a single message bubble (either from the user or the assistant).
 * Handles markdown rendering, attachments, agent thinking steps, and RAG sources.
 */
export function ChatMessage({ message, onCheckboxToggle, showSteps = true }: ChatMessageProps) {
  const isUser = message.role === "user";
  
  // State for collapsible sections
  const [isStepsExpanded, setIsStepsExpanded] = useState(false);
  const [isSourcesExpanded, setIsSourcesExpanded] = useState(false);

  // Helper to render the appropriate icon for agent thinking steps
  const renderStepIcon = (status: AgentStep['status']) => {
    switch (status) {
      case 'success': return <CheckCircle2 className="w-4 h-4 text-emerald-500" />;
      case 'running': return <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />;
      case 'error': return <XCircle className="w-4 h-4 text-red-500" />;
      default: return <CircleDashed className="w-4 h-4 text-gray-400" />;
    }
  };

  return (
    <div
      className={cn(
        "flex gap-4 w-full max-w-4xl mx-auto mb-8 animate-fade-in-up",
        isUser ? "flex-row-reverse" : "flex-row"
      )}
    >
      <div
        className={cn(
          "flex-shrink-0 w-10 h-10 rounded-2xl flex items-center justify-center shadow-sm",
          isUser
            ? "bg-gradient-to-tr from-blue-500 to-blue-600 text-white"
            : "bg-white border border-gray-200 text-gray-700"
        )}
      >
        {isUser ? <User className="w-5 h-5" /> : <Bot className="w-5 h-5" />}
      </div>

      <div
        className={cn(
          "max-w-[85%] px-6 py-4 rounded-[2rem]",
          isUser
            ? "bg-blue-500 text-white rounded-tr-sm shadow-md shadow-blue-500/10"
            : "glass-panel rounded-tl-sm w-full"
        )}
      >
        {/* Agent Thinking Steps (Collapsible) */}
        {showSteps && !isUser && message.steps && message.steps.length > 0 && (
          <div className="mb-4 bg-white/60 border border-gray-200/60 rounded-xl overflow-hidden shadow-sm">
            <button 
              onClick={() => setIsStepsExpanded(!isStepsExpanded)}
              className="w-full flex items-center justify-between px-4 py-3 hover:bg-white/40 transition-colors"
            >
              <div className="flex items-center gap-2 text-sm font-medium text-gray-700">
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
                        <p className={cn("text-[13px] font-medium", step.status === 'error' ? "text-red-700" : "text-gray-800")}>
                          {step.title}
                        </p>
                        {step.details && (
                          <p className="text-[12px] text-gray-500 mt-1 leading-relaxed">
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

        {/* Attachments Display (Images or Files) */}
        {message.attachments && message.attachments.length > 0 && (
          <div className="flex flex-wrap gap-2 mb-3">
            {message.attachments.map(att => (
              att.type.startsWith('image/') ? (
                <img 
                  key={att.id} 
                  src={att.data} 
                  alt={att.name} 
                  className="max-w-xs max-h-64 object-contain rounded-lg border border-white/20 shadow-sm" 
                />
              ) : (
                <div key={att.id} className="flex items-center gap-2 bg-white/10 px-3 py-2 rounded-lg text-sm border border-white/20">
                  <File className="w-4 h-4" />
                  <span className="truncate max-w-[150px]">{att.name}</span>
                </div>
              )
            ))}
          </div>
        )}

        {/* User messages are shown as plain text */}
        {isUser ? (
          <div className="whitespace-pre-wrap text-[14px] leading-relaxed">
            {message.content}
          </div>
        ) : (
          <>
            {/* Assistant messages are rendered as Markdown */}
            <div className="markdown-body">
              <ReactMarkdown 
                remarkPlugins={[remarkGfm]}
                components={{
                  // Custom rendering for list items to support interactive task lists
                  li: ({ node, checked, className, children, ...props }) => {
                    const isTask = className?.includes('task-list-item');
                    if (isTask) {
                      // Extract text content from children
                      let text = '';
                      React.Children.forEach(children, child => {
                        if (typeof child === 'string') text += child;
                        else if (React.isValidElement(child) && child.props.children) {
                          if (typeof child.props.children === 'string') text += child.props.children;
                        }
                      });
                      text = text.trim();

                      return (
                        <li className={className} {...props} onClick={(e) => {
                          if ((e.target as HTMLElement).tagName !== 'INPUT') {
                            const input = e.currentTarget.querySelector('input');
                            if (input) {
                              input.checked = !input.checked;
                              onCheckboxToggle?.(message.id, text, input.checked);
                            }
                          }
                        }}>
                          {children}
                        </li>
                      );
                    }
                    return <li className={className} {...props}>{children}</li>;
                  },
                  // Custom rendering for input elements (specifically checkboxes)
                  input: ({ node, checked, type, ...props }) => {
                    if (type === 'checkbox') {
                      return (
                        <input 
                          type="checkbox" 
                          checked={checked} 
                          onChange={(e) => {
                            const li = e.target.closest('li');
                            let text = '';
                            if (li) {
                              text = li.textContent || '';
                            }
                            onCheckboxToggle?.(message.id, text.trim(), e.target.checked);
                          }}
                          {...props} 
                          disabled={false} // Enable the checkbox
                        />
                      );
                    }
                    return <input type={type} {...props} />;
                  }
                }}
              >
                {/* Preprocess markdown to fix common formatting issues before rendering */}
                {preprocessMarkdown(message.content)}
              </ReactMarkdown>
            </div>
            
            {/* RAG Confidence Warning: Show if the retrieval score is too low */}
            {message.confidence !== undefined && message.confidence < 0.4 && (
              <div className="mt-4 p-3 bg-amber-50 border border-amber-200 rounded-lg flex items-start gap-2 text-amber-800 text-xs">
                <XCircle className="w-4 h-4 text-amber-500 flex-shrink-0 mt-0.5" />
                <p><strong>Attention :</strong> Les documents trouvés semblent peu pertinents par rapport à votre question (score: {Math.round(message.confidence * 100)}%). La réponse peut être imprécise.</p>
              </div>
            )}

            {/* RAG Sources Inspector (Collapsible) */}
            {message.sources && message.sources.length > 0 && (
              <div className="mt-4 border-t border-gray-200/60 pt-3">
                <button 
                  onClick={() => setIsSourcesExpanded(!isSourcesExpanded)}
                  className="flex items-center gap-2 text-xs font-medium text-gray-500 hover:text-gray-800 transition-colors"
                >
                  <Database className="w-3.5 h-3.5" />
                  Inspecteur RAG ({message.sources.length} sources)
                  {isSourcesExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                </button>
                
                {isSourcesExpanded && (
                  <div className="mt-3 space-y-2">
                    {message.sources.map((source, idx) => (
                      <div key={source.id} className="bg-white/60 border border-gray-200 rounded-lg p-3 text-xs">
                        <div className="flex items-center justify-between mb-1.5">
                          <span className="font-semibold text-gray-700 flex items-center gap-1.5">
                            <span className="bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded text-[10px]">[{idx + 1}]</span>
                            {source.docName}
                          </span>
                          <span className={cn("font-mono text-[10px] px-1.5 py-0.5 rounded", source.score > 0.6 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700")}>
                            Score: {Math.round(source.score * 100)}%
                          </span>
                        </div>
                        <p className="text-gray-600 line-clamp-3 hover:line-clamp-none transition-all duration-300 leading-relaxed">
                          {source.text}
                        </p>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
