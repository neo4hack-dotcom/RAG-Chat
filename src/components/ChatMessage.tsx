import React, { useState } from "react";
import { Bot, User, ChevronDown, ChevronRight, CheckCircle2, CircleDashed, Loader2, XCircle, BrainCircuit, File } from "lucide-react";
import { Message, cn, parseMarkdownToHTML, AgentStep } from "../lib/utils";

interface ChatMessageProps {
  message: Message;
  onCheckboxToggle?: (messageId: string, text: string, checked: boolean) => void;
  showSteps?: boolean;
}

export function ChatMessage({ message, onCheckboxToggle, showSteps = true }: ChatMessageProps) {
  const isUser = message.role === "user";
  const [isStepsExpanded, setIsStepsExpanded] = useState(false);

  const handleHtmlClick = (e: React.MouseEvent<HTMLDivElement>) => {
    const target = e.target as HTMLElement;
    if (target.tagName === 'INPUT' && target.getAttribute('type') === 'checkbox') {
      const li = target.closest('.task-list-item');
      if (li) {
        const text = li.getAttribute('data-task') || '';
        const checked = (target as HTMLInputElement).checked;
        onCheckboxToggle?.(message.id, text, checked);
      }
    } else if (target.closest('.task-list-item')) {
      // Allow clicking the li to toggle the checkbox
      const li = target.closest('.task-list-item');
      const input = li?.querySelector('input');
      if (input && target.tagName !== 'INPUT') {
        (input as HTMLInputElement).click();
      }
    }
  };

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

        {isUser ? (
          <div className="whitespace-pre-wrap text-[14px] leading-relaxed">
            {message.content}
          </div>
        ) : (
          <div 
            className="markdown-body"
            onClick={handleHtmlClick}
            dangerouslySetInnerHTML={{ __html: parseMarkdownToHTML(message.content) }}
          />
        )}
      </div>
    </div>
  );
}
