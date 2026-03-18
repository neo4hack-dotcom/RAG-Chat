import React, { useState, useRef, useEffect } from "react";
import { Send, Settings, Hammer, Loader2, Bot, Plus, MessageSquare, Trash2, Database, Network, Cpu, Users, BarChart, Search, PanelLeftClose, PanelLeftOpen, Star, Paperclip, X, File } from "lucide-react";
import { Message, AppConfig, Conversation, Attachment } from "../lib/utils";
import { ChatMessage } from "./ChatMessage";

interface ChatInterfaceProps {
  config: AppConfig;
  onOpenSettings: () => void;
}

export function ChatInterface({ config, onOpenSettings }: ChatInterfaceProps) {
  // --- STATE MANAGEMENT ---
  
  // Load conversation history from local storage
  const [conversations, setConversations] = useState<Conversation[]>(() => {
    const saved = localStorage.getItem('ragnarok_conversations');
    return saved ? JSON.parse(saved) : [];
  });
  
  // Track the currently active conversation ID
  const [currentId, setCurrentId] = useState<string | null>(() => {
    const saved = localStorage.getItem('ragnarok_conversations');
    if (saved) {
      const parsed = JSON.parse(saved);
      if (parsed.length > 0) return parsed[0].id;
    }
    return null;
  });

  // UI and Interaction states
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isConnected, setIsConnected] = useState(true);
  
  // Workflow mode: Standard LLM, RAG (Retrieval-Augmented Generation), or Multi-Agent
  const [workflow, setWorkflow] = useState<'LLM' | 'RAG' | 'AGENT'>('LLM');
  const [agentRole, setAgentRole] = useState<'manager' | 'analyst' | 'researcher'>('manager');
  
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  
  // Refs for DOM elements
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Persist conversations whenever they change
  useEffect(() => {
    localStorage.setItem('ragnarok_conversations', JSON.stringify(conversations));
  }, [conversations]);

  const currentConversation = conversations.find(c => c.id === currentId);
  const messages = currentConversation?.messages || [
    {
      id: "1",
      role: "assistant",
      content: "# Welcome to RAGnarok ⚡️\n\nI'm your AI assistant, ready to connect to your LLMs, RAG system, or agents.",
      timestamp: Date.now(),
      steps: [
        { id: 'init-1', title: 'System Initialization', status: 'success', details: 'Loaded configuration and connected to local environment.' },
        { id: 'init-2', title: 'Ready for Instructions', status: 'success', details: 'Awaiting your commands to orchestrate sub-agents or query the RAG database.' }
      ]
    },
  ];

  // --- ACTIONS ---
  
  // Start a completely new chat session
  const createNewChat = () => {
    setCurrentId(null);
    setInput("");
  };

  // Reset the current chat to its initial state (welcome message only)
  const clearCurrentChat = () => {
    if (!currentId) return;
    setConversations(prev => prev.map(c => 
      c.id === currentId 
        ? { ...c, messages: [{
            id: Date.now().toString(),
            role: "assistant",
            content: "# Welcome to RAGnarok ⚡️\n\nI'm your AI assistant, ready to connect to your LLMs, RAG system, or agents.",
            timestamp: Date.now(),
            steps: [
              { id: 'init-1', title: 'System Initialization', status: 'success', details: 'Loaded configuration and connected to local environment.' },
              { id: 'init-2', title: 'Ready for Instructions', status: 'success', details: 'Awaiting your commands to orchestrate sub-agents or query the RAG database.' }
            ]
          }], updatedAt: Date.now() } 
        : c
    ));
  };

  // Delete a specific conversation from history
  const deleteConversation = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    const updated = conversations.filter(c => c.id !== id);
    setConversations(updated);
    if (currentId === id) {
      setCurrentId(updated.length > 0 ? updated[0].id : null);
    }
  };

  // Auto-scroll to the bottom of the chat when new messages arrive
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isLoading]);

  // Handle file selection for attachments
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    files.forEach(file => {
      const reader = new FileReader();
      reader.onload = (event) => {
        const base64 = event.target?.result as string;
        setAttachments(prev => [...prev, {
          id: Math.random().toString(36).substring(7),
          name: file.name,
          type: file.type,
          data: base64
        }]);
      };
      reader.readAsDataURL(file);
    });
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  // Remove an attachment before sending
  const removeAttachment = (id: string) => {
    setAttachments(prev => prev.filter(a => a.id !== id));
  };

  // Auto-resize the textarea based on content
  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value);
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 200)}px`;
    }
  };

  // Main function to handle sending a message
  const handleSend = async (text: string = input) => {
    if ((!text.trim() && attachments.length === 0) || isLoading) return;

    // Construct the user message object
    const userMsg: Message = {
      id: Date.now().toString(),
      role: "user",
      content: text,
      timestamp: Date.now(),
      attachments: attachments.length > 0 ? attachments : undefined
    };

    let activeConvId = currentId;
    let activeMessages = [...messages, userMsg];

    // Create a new conversation if one doesn't exist
    if (!activeConvId) {
      activeConvId = Date.now().toString();
      const newConv: Conversation = {
        id: activeConvId,
        title: text.slice(0, 30) + (text.length > 30 ? '...' : ''),
        messages: activeMessages,
        updatedAt: Date.now()
      };
      setConversations(prev => [newConv, ...prev]);
      setCurrentId(activeConvId);
    } else {
      // Update the existing conversation and move it to the top of the list
      setConversations(prev => {
        const idx = prev.findIndex(c => c.id === activeConvId);
        if (idx === -1) return prev;
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          messages: activeMessages,
          updatedAt: Date.now()
        };
        const [conv] = updated.splice(idx, 1);
        updated.unshift(conv);
        return updated;
      });
    }

    // Reset input fields and UI state
    setInput("");
    setAttachments([]);
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
    }
    setIsLoading(true);

    try {
      let reply = "";
      let sources = undefined;
      let confidence = undefined;

      // Route the request based on the selected workflow
      if (workflow === 'RAG') {
        // Call our full-stack RAG backend
        const response = await fetch('/api/chat/rag', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: activeMessages.filter(m => m.role !== 'system'),
            elasticsearchUrl:      config.elasticsearchUrl,
            elasticsearchIndex:    config.elasticsearchIndex,
            elasticsearchUsername: config.elasticsearchUsername,
            elasticsearchPassword: config.elasticsearchPassword,
          })
        });

        if (!response.ok) throw new Error(`RAG Backend error! status: ${response.status}`);
        const data = await response.json();
        reply = data.answer;
        sources = data.sources;
        confidence = data.confidence;
        setIsConnected(true);
      } else {
        // Standard LLM / Agent flow (calls external API directly from frontend)
        let dynamicSystemPrompt = config.systemPrompt;
        if (workflow === 'AGENT') {
          dynamicSystemPrompt += `\n\n[SYSTEM: You are currently operating as an Agent with the role: ${agentRole.toUpperCase()}. Act accordingly.]`;
        }

        // Prepare messages for OpenAI/Ollama format, handling attachments
        const apiMessages = [
          { role: "system", content: dynamicSystemPrompt },
          ...activeMessages.map((m) => {
            if (m.role === 'user' && m.attachments && m.attachments.length > 0) {
              if (config.provider === 'ollama') {
                const images = m.attachments
                  .filter(a => a.type.startsWith('image/'))
                  .map(a => a.data.split(',')[1]);
                const textAttachments = m.attachments
                  .filter(a => !a.type.startsWith('image/'))
                  .map(a => `[Attached File: ${a.name}]`);
                const content = textAttachments.length > 0 
                  ? `${m.content}\n\n${textAttachments.join('\n')}`
                  : m.content;
                return {
                  role: m.role,
                  content: content,
                  images: images.length > 0 ? images : undefined
                };
              } else {
                const content: any[] = [{ type: "text", text: m.content }];
                m.attachments.forEach(a => {
                  if (a.type.startsWith('image/')) {
                    content.push({ type: "image_url", image_url: { url: a.data } });
                  } else {
                    content[0].text += `\n\n[Attached File: ${a.name}]`;
                  }
                });
                return { role: m.role, content };
              }
            }
            return { role: m.role, content: m.content };
          }),
        ];

        const baseUrl = (config.baseUrl || (config as any).endpoint || 'http://localhost:11434').replace(/\/$/, '');
        const chatEndpoint = config.provider === 'ollama' 
          ? `${baseUrl}/api/chat` 
          : `${baseUrl}/chat/completions`;

        const payload = {
          model: config.model,
          messages: apiMessages,
          stream: false,
        };

        const headers: Record<string, string> = {
          "Content-Type": "application/json",
        };

        if (config.apiKey) {
          headers["Authorization"] = `Bearer ${config.apiKey}`;
        }

        const response = await fetch(chatEndpoint, {
          method: "POST",
          headers,
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        setIsConnected(true);
        const data = await response.json();
        
        reply = config.provider === 'ollama' 
          ? data.message?.content 
          : data.choices?.[0]?.message?.content;

        if (!reply) {
          throw new Error("Invalid response format from API");
        }
      }

      const assistantMsg: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: reply,
        timestamp: Date.now(),
        sources,
        confidence,
      };

      // Mock steps for Agent Manager to showcase the UI for LangGraph integration
      if (workflow === 'AGENT' && agentRole === 'manager') {
        assistantMsg.steps = [
          { id: 'step-1', title: 'Analyzing Request', status: 'success', details: 'Parsed user intent and identified required sub-tasks.' },
          { id: 'step-2', title: 'Delegating to Sub-Agents', status: 'success', details: 'Dispatched tasks to Researcher and Analyst nodes via LangGraph.' },
          { id: 'step-3', title: 'Synthesizing Results', status: 'success', details: 'Aggregated outputs and formulated final response.' }
        ];
      }

      setConversations(prev => {
        const idx = prev.findIndex(c => c.id === activeConvId);
        if (idx === -1) return prev;
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          messages: [...updated[idx].messages, assistantMsg],
          updatedAt: Date.now()
        };
        return updated;
      });
    } catch (error) {
      console.error("Error fetching from LLM:", error);
      setIsConnected(false);
      const errorMsg: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: `**Error:** Could not connect to the LLM endpoint.\n\n\`\`\`\n${error instanceof Error ? error.message : "Unknown error"}\n\`\`\`\n\nPlease check your configuration settings.`,
        timestamp: Date.now(),
      };
      setConversations(prev => {
        const idx = prev.findIndex(c => c.id === activeConvId);
        if (idx === -1) return prev;
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          messages: [...updated[idx].messages, errorMsg],
          updatedAt: Date.now()
        };
        return updated;
      });
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleCheckboxToggle = (messageId: string, text: string, checked: boolean) => {
    // When a user clicks a checkbox proposed by the LLM, we can automatically send a message
    // or just update local state. Let's send a contextual message if checked.
    if (checked) {
      handleSend(`I choose: ${text}`);
    }
  };

  return (
    <div className="flex h-screen relative overflow-hidden bg-[#f5f5f5]">
      <div className="mesh-bg" />

      {/* Sidebar */}
      <aside className={`bg-white/60 border-r border-gray-200/50 backdrop-blur-xl flex flex-col z-20 flex-shrink-0 transition-all duration-300 ease-in-out hidden md:flex ${isSidebarOpen ? 'w-64 md:w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden border-none'}`}>
        <div className="p-4 border-b border-gray-200/50 w-64 md:w-72">
          <button
            onClick={createNewChat}
            className="w-full flex items-center justify-center gap-2 bg-black text-white px-4 py-3 rounded-xl hover:bg-gray-800 transition-all shadow-lg shadow-black/5 font-medium"
          >
            <Plus className="w-4 h-4" /> New Chat
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-3 space-y-2">
          {conversations.map((conv) => (
            <div
              key={conv.id}
              onClick={() => setCurrentId(conv.id)}
              className={`group relative w-full text-left p-3 rounded-xl text-sm transition-all cursor-pointer border ${
                currentId === conv.id
                  ? "bg-white border-gray-200 shadow-sm"
                  : "border-transparent hover:bg-white/50 hover:border-gray-200/50"
              }`}
            >
              <div className="flex items-start gap-3">
                <MessageSquare className={`w-4 h-4 mt-0.5 flex-shrink-0 ${currentId === conv.id ? 'text-blue-500' : 'text-gray-400'}`} />
                <div className="flex-1 overflow-hidden">
                  <div className="truncate font-medium text-gray-900">{conv.title}</div>
                  <div className="text-xs text-gray-400 mt-1">
                    {new Date(conv.updatedAt).toLocaleDateString()}
                  </div>
                </div>
              </div>
              <button
                onClick={(e) => deleteConversation(e, conv.id)}
                className={`absolute right-2 top-1/2 -translate-y-1/2 p-1.5 rounded-lg text-gray-400 hover:text-red-500 hover:bg-red-50 transition-colors ${currentId === conv.id ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'}`}
                title="Delete chat"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          ))}
          {conversations.length === 0 && (
            <div className="text-center text-sm text-gray-400 mt-10 px-4">
              No previous conversations. Start a new chat!
            </div>
          )}
        </div>
      </aside>

      {/* Main Chat Area */}
      <div className="flex-1 flex flex-col relative z-10 h-screen overflow-hidden">
        {/* Header */}
      <header className="glass-panel m-2 rounded-xl px-4 py-2 flex items-center justify-between z-10 relative">
        {/* Left: Sidebar Toggle */}
        <div className="flex items-center w-1/3">
          <button 
            onClick={() => setIsSidebarOpen(!isSidebarOpen)} 
            className="p-2 -ml-2 rounded-xl hover:bg-black/5 text-gray-600 transition-colors hidden md:block"
            title={isSidebarOpen ? "Close sidebar" : "Open sidebar"}
          >
            {isSidebarOpen ? <PanelLeftClose className="w-5 h-5" /> : <PanelLeftOpen className="w-5 h-5" />}
          </button>
        </div>

        {/* Center: Title & Icons */}
        <div className="flex flex-col items-center justify-center w-1/3">
          <div className="flex items-center gap-5">
            <div className="w-9 h-9 bg-gradient-to-tr from-slate-700 to-slate-900 rounded-xl flex items-center justify-center shadow-md shadow-slate-900/20">
              <Hammer className="w-5 h-5 text-white" />
            </div>
            <h1 className="text-3xl font-bold text-gray-900 tracking-tight">RAGnarok</h1>
            <div className="w-9 h-9 bg-gradient-to-tr from-slate-700 to-slate-900 rounded-xl flex items-center justify-center shadow-md shadow-slate-900/20">
              <Hammer className="w-5 h-5 text-white" />
            </div>
          </div>
          <div className="flex items-center gap-2 text-[10px] font-medium text-gray-500 mt-0.5">
            <span className={`w-1.5 h-1.5 rounded-full ${isConnected ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)]' : 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]'}`} />
            {isConnected ? 'Active' : 'Offline'}
          </div>
        </div>

        {/* Right: Settings */}
        <div className="flex items-center justify-end w-1/3 gap-1">
          {currentId && (
            <button
              onClick={clearCurrentChat}
              className="glass-button p-2 rounded-xl text-red-600 hover:text-red-700 hover:bg-red-50 flex items-center gap-1.5"
              title="Reset conversation"
            >
              <Trash2 className="w-4 h-4" />
              <span className="hidden sm:inline text-xs font-medium">Reset</span>
            </button>
          )}
          <button
            onClick={onOpenSettings}
            className="glass-button p-2 rounded-xl text-gray-600 hover:text-gray-900"
            title="Configuration"
          >
            <Settings className="w-4 h-4" />
          </button>
        </div>
      </header>

      {/* Chat Area */}
      <main className="flex-1 overflow-y-auto p-4 md:p-8 z-10 scroll-smooth">
        <div className="max-w-[67rem] mx-auto pb-20">
          {messages.map((msg) => (
            <ChatMessage
              key={msg.id}
              message={msg}
              onCheckboxToggle={handleCheckboxToggle}
              showSteps={workflow === 'AGENT'}
            />
          ))}
          
          {isLoading && (
            <div className="flex gap-4 w-full max-w-[67rem] mx-auto mb-8 animate-fade-in-up">
              <div className="flex-shrink-0 w-10 h-10 rounded-2xl bg-white border border-gray-200 flex items-center justify-center shadow-sm">
                <Bot className="w-5 h-5 text-gray-400" />
              </div>
              <div className="glass-panel px-6 py-4 rounded-[2rem] rounded-tl-sm flex items-center gap-2">
                <Loader2 className="w-5 h-5 text-blue-500 animate-spin" />
                <span className="text-sm text-gray-500 font-medium">Thinking...</span>
              </div>
            </div>
          )}

          {workflow === 'AGENT' && agentRole === 'manager' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-3 rounded-xl bg-gradient-to-br from-amber-50 to-orange-50 border border-amber-200/50 shadow-sm animate-fade-in-up">
              <div className="flex items-center gap-2 mb-1.5">
                <Star className="w-4 h-4 text-amber-500 fill-amber-500" />
                <h3 className="font-semibold text-amber-900 text-[13px]">Agent Manager Brief</h3>
              </div>
              <div className="text-[11px] text-amber-800/90 leading-relaxed space-y-1">
                <p>Welcome to the <strong>Agent Manager</strong> mode. Here is what you can do:</p>
                <ul className="list-disc pl-4 space-y-0.5">
                  <li>Orchestrate multiple sub-agents to solve complex tasks.</li>
                  <li>Delegate research, analysis, and coding to specialized AI roles.</li>
                  <li>Review and synthesize the final output from all agents.</li>
                </ul>
                <p className="italic mt-1 text-amber-700/70">Tip: Start by describing your overarching goal, and the Manager will handle the rest.</p>
              </div>
            </div>
          )}

          {workflow === 'RAG' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-3 rounded-xl bg-gradient-to-br from-blue-50 to-indigo-50 border border-blue-200/50 shadow-sm animate-fade-in-up">
              <div className="flex items-center gap-2 mb-1.5">
                <Database className="w-4 h-4 text-blue-500" />
                <h3 className="font-semibold text-blue-900 text-[13px]">Retrieval-Augmented Generation (RAG)</h3>
              </div>
              <div className="text-[11px] text-blue-800/90 leading-relaxed space-y-1">
                <p>Welcome to <strong>RAG</strong> mode. This workflow helps you find and synthesize information from your documents:</p>
                <ul className="list-disc pl-4 space-y-0.5">
                  <li><strong>Retrieve:</strong> Your query is converted into a vector using the local embedding model and searched against the Elasticsearch database.</li>
                  <li><strong>Augment:</strong> The most relevant document chunks (optimized with overlap) are retrieved using K-Nearest Neighbors (KNN).</li>
                  <li><strong>Generate:</strong> The LLM uses this retrieved context to provide accurate, grounded answers.</li>
                </ul>
                <p className="italic mt-1 text-blue-700/70">Tip: You can configure the Elasticsearch URL, embedding model, chunk size, and KNN neighbors in the settings.</p>
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>
      </main>

      {/* Input Area */}
      <div className="p-4 md:p-8 pt-0 z-10 w-full max-w-[67rem] mx-auto">
        <div className="glass-panel rounded-[2rem] p-2 flex flex-col gap-2 shadow-2xl shadow-black/5">
          {attachments.length > 0 && (
            <div className="flex flex-wrap gap-2 px-4 pt-3 pb-1">
              {attachments.map(att => (
                <div key={att.id} className="relative group flex items-center gap-2 bg-white/60 border border-gray-200/60 px-3 py-1.5 rounded-xl text-sm shadow-sm">
                  {att.type.startsWith('image/') ? (
                    <img src={att.data} alt={att.name} className="w-6 h-6 object-cover rounded-md" />
                  ) : (
                    <File className="w-4 h-4 text-blue-500" />
                  )}
                  <span className="truncate max-w-[120px] font-medium text-gray-700">{att.name}</span>
                  <button
                    onClick={() => removeAttachment(att.id)}
                    className="absolute -top-2 -right-2 bg-red-500 text-white rounded-full p-0.5 opacity-0 group-hover:opacity-100 transition-opacity shadow-sm"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              ))}
            </div>
          )}
          
          <div className="flex items-end gap-2 w-full">
            <input
              type="file"
              multiple
              ref={fileInputRef}
              onChange={handleFileChange}
              className="hidden"
              accept="image/*,.pdf,.txt,.csv,.md"
            />
            <button
              onClick={() => fileInputRef.current?.click()}
              className="flex-shrink-0 w-12 h-12 rounded-2xl text-gray-400 hover:text-blue-500 hover:bg-blue-50 flex items-center justify-center transition-colors mb-1 ml-1"
              title="Attach file"
            >
              <Paperclip className="w-5 h-5" />
            </button>
            <textarea
              ref={textareaRef}
              value={input}
              onChange={handleInputChange}
              onKeyDown={handleKeyDown}
              placeholder="Message your AI agent..."
              className="w-full min-h-[56px] bg-transparent border-none resize-none focus:ring-0 px-2 py-4 text-[14px] leading-relaxed text-gray-900 placeholder-gray-400 outline-none overflow-y-auto"
              rows={1}
            />
            <button
              onClick={() => handleSend()}
              disabled={(!input.trim() && attachments.length === 0) || isLoading}
              className="flex-shrink-0 w-12 h-12 rounded-2xl bg-black text-white flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-800 transition-colors mb-1 mr-1"
            >
              <Send className="w-5 h-5 ml-0.5" />
            </button>
          </div>
        </div>

        {/* Workflow Selector */}
        <div className="mt-3 px-2 flex flex-col gap-2">
          <div className="flex items-center gap-2 overflow-x-auto pb-1 scrollbar-hide">
            <button
              onClick={() => setWorkflow('LLM')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'LLM' ? 'bg-blue-100 text-blue-700 border border-blue-200 shadow-sm' : 'bg-white/50 text-gray-600 border border-gray-200 hover:bg-white/80'}`}
            >
              <Cpu className="w-3.5 h-3.5" /> Pure LLM
            </button>
            <button
              onClick={() => setWorkflow('RAG')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'RAG' ? 'bg-emerald-100 text-emerald-700 border border-emerald-200 shadow-sm' : 'bg-white/50 text-gray-600 border border-gray-200 hover:bg-white/80'}`}
            >
              <Database className="w-3.5 h-3.5" /> RAG Knowledge
            </button>
            <button
              onClick={() => setWorkflow('AGENT')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'AGENT' ? 'bg-purple-100 text-purple-700 border border-purple-200 shadow-sm' : 'bg-white/50 text-gray-600 border border-gray-200 hover:bg-white/80'}`}
            >
              <Network className="w-3.5 h-3.5" /> Agents
            </button>
          </div>

          {/* Sub-options for AGENT */}
          <div className={`flex items-center gap-2 overflow-hidden transition-all duration-300 ease-in-out ${workflow === 'AGENT' ? 'max-h-10 opacity-100' : 'max-h-0 opacity-0'}`}>
            <div className="flex items-center gap-2 pl-2 border-l-2 border-purple-200 overflow-x-auto pb-1 scrollbar-hide">
              <button
                onClick={() => setAgentRole('manager')}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${agentRole === 'manager' ? 'bg-gradient-to-r from-amber-500 to-orange-500 text-white shadow-md shadow-orange-500/20 border-none' : 'bg-white/60 text-gray-600 border border-gray-200 hover:bg-white'}`}
              >
                <Star className={`w-3.5 h-3.5 ${agentRole === 'manager' ? 'fill-white text-white' : 'text-amber-500 fill-amber-500'}`} /> Agent Manager
              </button>
              <button
                onClick={() => setAgentRole('analyst')}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${agentRole === 'analyst' ? 'bg-purple-500 text-white shadow-md shadow-purple-500/20' : 'bg-white/60 text-gray-600 border border-gray-200 hover:bg-white'}`}
              >
                <BarChart className="w-3.5 h-3.5" /> Data Analyst
              </button>
              <button
                onClick={() => setAgentRole('researcher')}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${agentRole === 'researcher' ? 'bg-purple-500 text-white shadow-md shadow-purple-500/20' : 'bg-white/60 text-gray-600 border border-gray-200 hover:bg-white'}`}
              >
                <Search className="w-3.5 h-3.5" /> Researcher
              </button>
            </div>
          </div>
        </div>

        <div className="text-center mt-3 text-xs text-gray-400 font-medium">
          AI can make mistakes. Consider verifying important information.
        </div>
      </div>
      </div>
    </div>
  );
}
