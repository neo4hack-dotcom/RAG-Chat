import React, { useState, useRef, useEffect } from "react";
import { Send, Settings, Hammer, Loader2, Bot, Plus, MessageSquare, Trash2, Database, Network, Cpu, PanelLeftClose, PanelLeftOpen, Star, Paperclip, X, File, Moon, Sun, Home, CalendarDays, ChevronDown, ChevronRight, FolderOpen, BarChart3 } from "lucide-react";
import { Message, AppConfig, Conversation, Attachment, McpTool, WorkflowMode, AgentRole, ChatAction, CrewPlan, CrewPlanDraft, PlanningBackendState, FileManagerAgentConfig, buildConversationMemory, createEmptyCrewPlanDraft, normalizeCrewPlanDraft, normalizePlanningAgentState, normalizePlanningBackendState, normalizeFileManagerAgentState, normalizeManagerAgentState, normalizeDataQualityState, normalizeAppConfig } from "../lib/utils";
import { ChatMessage } from "./ChatMessage";
import { PlanningModal } from "./PlanningModal";
import { FileManagerConfigModal } from "./FileManagerConfigModal";

interface ChatInterfaceProps {
  config: AppConfig;
  conversations: Conversation[];
  currentId: string | null;
  workflow: WorkflowMode;
  agentRole: AgentRole;
  mcpToolId: string;
  onConversationsChange: React.Dispatch<React.SetStateAction<Conversation[]>>;
  onCurrentIdChange: (id: string | null) => void;
  onWorkflowChange: (workflow: WorkflowMode) => void;
  onAgentRoleChange: (role: AgentRole) => void;
  onMcpToolIdChange: (id: string) => void;
  onConfigChange: (config: AppConfig) => void;
  isDark: boolean;
  onToggleDark: () => void;
  onGoHome?: () => void;
}

export function ChatInterface({
  config,
  conversations,
  currentId,
  workflow,
  agentRole,
  mcpToolId,
  onConversationsChange,
  onCurrentIdChange,
  onWorkflowChange,
  onAgentRoleChange,
  onMcpToolIdChange,
  onConfigChange,
  isDark,
  onToggleDark,
  onGoHome,
}: ChatInterfaceProps) {
  // --- STATE MANAGEMENT ---

  // UI and Interaction states
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isConnected, setIsConnected] = useState(true);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  
  // Refs for DOM elements
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const dataQualityBootstrapRef = useRef<string | null>(null);
  const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';

  const currentConversation = conversations.find(c => c.id === currentId);
  const managerAgentState = normalizeManagerAgentState((currentConversation?.agentState as any)?.manager);
  const clickhouseAgentState = currentConversation?.agentState?.clickhouse;
  const planningAgentState = normalizePlanningAgentState((currentConversation?.agentState as any)?.planning, browserTimeZone);
  const fileManagerAgentState = normalizeFileManagerAgentState((currentConversation?.agentState as any)?.fileManager);
  const dataQualityAgentState = normalizeDataQualityState((currentConversation?.agentState as any)?.dataQuality);
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
  const [isPlanningModalOpen, setIsPlanningModalOpen] = useState(false);
  const [planningState, setPlanningState] = useState<PlanningBackendState>(() => normalizePlanningBackendState(undefined, browserTimeZone));
  const [plannerDraft, setPlannerDraft] = useState<CrewPlanDraft>(() => normalizeCrewPlanDraft(planningAgentState.draft, browserTimeZone));
  const [editingPlanningPlanId, setEditingPlanningPlanId] = useState<string | null>(null);
  const [planningBusy, setPlanningBusy] = useState(false);
  const [planningError, setPlanningError] = useState<string | null>(null);
  const [isOtherAgentsOpen, setIsOtherAgentsOpen] = useState(agentRole === 'clickhouse_query' || agentRole === 'file_management' || agentRole === 'data_quality_tables');
  const [isFileManagerConfigOpen, setIsFileManagerConfigOpen] = useState(false);

  // --- ACTIONS ---
  
  // Start a completely new chat session
  const createNewChat = () => {
    onCurrentIdChange(null);
    setInput("");
  };

  // Reset the current chat to its initial state (welcome message only)
  const clearCurrentChat = () => {
    if (!currentId) return;
    onConversationsChange(prev => prev.map(c => 
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
          }], memory: buildConversationMemory([{
            id: Date.now().toString(),
            role: "assistant",
            content: "# Welcome to RAGnarok ⚡️\n\nI'm your AI assistant, ready to connect to your LLMs, RAG system, or agents.",
            timestamp: Date.now(),
          }]), updatedAt: Date.now(), agentState: undefined } 
        : c
    ));
  };

  // Delete a specific conversation from history
  const deleteConversation = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    const updated = conversations.filter(c => c.id !== id);
    onConversationsChange(updated);
    if (currentId === id) {
      onCurrentIdChange(updated.length > 0 ? updated[0].id : null);
    }
  };

  // Auto-scroll to the bottom of the chat when new messages arrive
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isLoading]);

  useEffect(() => {
    if ((config.mcpTools ?? []).length === 0) {
      if (mcpToolId) onMcpToolIdChange('');
      return;
    }

    const currentToolExists = (config.mcpTools ?? []).some((tool: McpTool) => tool.id === mcpToolId);
    if (!currentToolExists) {
      onMcpToolIdChange(config.mcpTools[0]?.id ?? '');
    }
  }, [config.mcpTools, mcpToolId, onMcpToolIdChange]);

  useEffect(() => {
    if (editingPlanningPlanId) return;
    setPlannerDraft(normalizeCrewPlanDraft(planningAgentState.draft, browserTimeZone));
  }, [planningAgentState.draft, browserTimeZone, editingPlanningPlanId]);

  const updatePlanningConversationState = (nextPlanningState: unknown) => {
    if (!currentId) return;
    const normalizedState = normalizePlanningAgentState(nextPlanningState as any, browserTimeZone);
    onConversationsChange(prev => prev.map((conversation) =>
      conversation.id === currentId
        ? {
            ...conversation,
            agentState: {
              ...(conversation.agentState ?? {}),
              planning: normalizedState,
            },
            updatedAt: Date.now(),
          }
        : conversation
    ));
  };

  const loadPlanningState = async () => {
    setPlanningBusy(true);
    try {
      const response = await fetch('/api/planning/state');
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning state error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to load planning state.');
    } finally {
      setPlanningBusy(false);
    }
  };

  useEffect(() => {
    if (workflow === 'CREWAI' || isPlanningModalOpen) {
      void loadPlanningState();
    }
  }, [workflow, isPlanningModalOpen]);

  useEffect(() => {
    if (workflow === 'AGENT' && (agentRole === 'clickhouse_query' || agentRole === 'file_management' || agentRole === 'data_quality_tables')) {
      setIsOtherAgentsOpen(true);
    }
  }, [workflow, agentRole]);

  useEffect(() => {
    if (workflow !== 'AGENT' || agentRole !== 'data_quality_tables' || isLoading) return;

    const hasStartedSetup =
      dataQualityAgentState.stage !== 'idle' ||
      Boolean(dataQualityAgentState.table) ||
      Boolean(dataQualityAgentState.finalAnswer);
    const hasGuidanceMessage = messages.some(
      (message) =>
        message.role === 'assistant' &&
        (
          message.content.includes('## Data quality - Tables') ||
          message.content.includes('## Data Quality Review') ||
          message.content.includes('## Table Selection')
        )
    );
    const bootstrapKey = `${currentId ?? 'new'}:${currentConversation?.updatedAt ?? 0}`;
    if (hasStartedSetup || hasGuidanceMessage || dataQualityBootstrapRef.current === bootstrapKey) {
      return;
    }

    dataQualityBootstrapRef.current = bootstrapKey;

    void (async () => {
      try {
        const response = await fetch('/api/chat/data-quality-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: '',
            history: currentConversation?.memory?.steps ?? [],
            agent_state: dataQualityAgentState ?? undefined,
            clickhouse: {
              host: config.clickhouseHost,
              port: config.clickhousePort,
              database: config.clickhouseDatabase,
              username: config.clickhouseUsername,
              password: config.clickhousePassword,
              secure: config.clickhouseSecure,
              verify_ssl: config.clickhouseVerifySsl,
              http_path: config.clickhouseHttpPath,
              query_limit: config.clickhouseQueryLimit,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
          }),
        });

        if (!response.ok) {
          return;
        }

        const data = await response.json();
        const assistantMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: 'assistant',
          content: data.answer,
          timestamp: Date.now(),
          steps: data.steps,
        };
        const nextState = normalizeDataQualityState(data.agent_state);

        if (!currentId) {
          const conversationId = Date.now().toString();
          const newConversation: Conversation = {
            id: conversationId,
            title: 'Data quality - Tables',
            messages: [assistantMsg],
            memory: buildConversationMemory([assistantMsg]),
            updatedAt: Date.now(),
            agentState: {
              dataQuality: nextState,
            },
          };
          onConversationsChange((prev) => [newConversation, ...prev]);
          onCurrentIdChange(conversationId);
          return;
        }

        onConversationsChange((prev) =>
          prev.map((conversation) =>
            conversation.id === currentId
              ? {
                  ...conversation,
                  messages: [...conversation.messages, assistantMsg],
                  memory: buildConversationMemory([...conversation.messages, assistantMsg]),
                  updatedAt: Date.now(),
                  agentState: {
                    ...(conversation.agentState ?? {}),
                    dataQuality: nextState,
                  },
                }
              : conversation
          )
        );
      } catch {
        // Keep the UI responsive even if the backend onboarding call fails.
      }
    })();
  }, [
    workflow,
    agentRole,
    isLoading,
    currentId,
    currentConversation,
    messages,
    dataQualityAgentState,
    config.clickhouseHost,
    config.clickhousePort,
    config.clickhouseDatabase,
    config.clickhouseUsername,
    config.clickhousePassword,
    config.clickhouseSecure,
    config.clickhouseVerifySsl,
    config.clickhouseHttpPath,
    config.clickhouseQueryLimit,
    config.baseUrl,
    config.model,
    config.apiKey,
    config.provider,
    onConversationsChange,
    onCurrentIdChange,
  ]);

  const openPlanningModal = (nextDraft?: Partial<CrewPlanDraft> | null) => {
    if (nextDraft) {
      setPlannerDraft(normalizeCrewPlanDraft(nextDraft, browserTimeZone));
      setEditingPlanningPlanId(null);
    }
    setPlanningError(null);
    setIsPlanningModalOpen(true);
    void loadPlanningState();
  };

  const startNewPlanningDraft = () => {
    const emptyDraft = createEmptyCrewPlanDraft(browserTimeZone);
    setEditingPlanningPlanId(null);
    setPlannerDraft(emptyDraft);
    updatePlanningConversationState({
      draft: emptyDraft,
      missing_fields: [],
      last_question: '',
      ready_to_review: false,
    });
  };

  const savePlanningPlan = async (draft: CrewPlanDraft, editingPlanId: string | null) => {
    setPlanningBusy(true);
    try {
      const response = await fetch('/api/planning/plans', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          plan: {
            ...draft,
            id: editingPlanId || undefined,
          },
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning save error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
      setEditingPlanningPlanId(null);
      const emptyDraft = createEmptyCrewPlanDraft(browserTimeZone);
      setPlannerDraft(emptyDraft);
      updatePlanningConversationState({
        draft: emptyDraft,
        missing_fields: [],
        last_question: '',
        ready_to_review: false,
      });
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to save the planning job.');
      throw error;
    } finally {
      setPlanningBusy(false);
    }
  };

  const editPlanningPlan = (plan: CrewPlan) => {
    setEditingPlanningPlanId(plan.id);
    setPlannerDraft(normalizeCrewPlanDraft(plan, browserTimeZone));
    setPlanningError(null);
    setIsPlanningModalOpen(true);
  };

  const togglePlanningPlanStatus = async (plan: CrewPlan) => {
    setPlanningBusy(true);
    try {
      const response = await fetch(`/api/planning/plans/${plan.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          status: plan.status === 'active' ? 'paused' : 'active',
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning status error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to update the planning status.');
    } finally {
      setPlanningBusy(false);
    }
  };

  const deletePlanningPlan = async (planId: string) => {
    setPlanningBusy(true);
    try {
      const response = await fetch(`/api/planning/plans/${planId}`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning delete error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
      if (editingPlanningPlanId === planId) {
        setEditingPlanningPlanId(null);
        setPlannerDraft(createEmptyCrewPlanDraft(browserTimeZone));
      }
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to delete the planning job.');
    } finally {
      setPlanningBusy(false);
    }
  };

  const runPlanningPlan = async (planId: string) => {
    setPlanningBusy(true);
    try {
      const response = await fetch(`/api/planning/plans/${planId}/run`, {
        method: 'POST',
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning run error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to run the planning job.');
    } finally {
      setPlanningBusy(false);
    }
  };

  const handleChatAction = (action: ChatAction) => {
    if (action.actionType === 'open_planning_form') {
      openPlanningModal(planningAgentState.draft);
      return;
    }
    if (action.actionType === 'edit_planning_plan') {
      const planId = String(action.payload?.planId || '');
      const existingPlan = planningState.plans.find((plan) => plan.id === planId);
      if (existingPlan) {
        editPlanningPlan(existingPlan);
      } else {
        openPlanningModal(planningAgentState.draft);
      }
      return;
    }
    if (action.actionType === 'refresh_planning_state') {
      void loadPlanningState();
      return;
    }
    if (action.actionType === 'confirm_file_action') {
      void handleSend('confirm');
      return;
    }
    if (action.actionType === 'cancel_file_action') {
      void handleSend('cancel');
    }
  };

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
        memory: buildConversationMemory(activeMessages),
        updatedAt: Date.now()
      };
      onConversationsChange(prev => [newConv, ...prev]);
      onCurrentIdChange(activeConvId);
    } else {
      // Update the existing conversation and move it to the top of the list
      onConversationsChange(prev => {
        const idx = prev.findIndex(c => c.id === activeConvId);
        if (idx === -1) return prev;
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          messages: activeMessages,
          memory: buildConversationMemory(activeMessages),
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
      const memoryHistory = buildConversationMemory(activeMessages).steps.map((step) => ({
        role: step.role,
        content: step.content,
      }));
      let reply = "";
      let sources = undefined;
      let confidence = undefined;
      let nextManagerAgentState = managerAgentState;
      let nextClickhouseAgentState = clickhouseAgentState;
      let nextPlanningAgentState = planningAgentState;
      let nextFileManagerAgentState = fileManagerAgentState;
      let nextDataQualityAgentState = dataQualityAgentState;

      // Route the request based on the selected workflow
      if (workflow === 'RAG') {
        // Call our full-stack RAG backend
        const response = await fetch('/api/chat/rag', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            opensearch: {
              url:      config.elasticsearchUrl,
              index:    config.elasticsearchIndex,
              username: config.elasticsearchUsername || undefined,
              password: config.elasticsearchPassword || undefined,
            },
            embedding_base_url:   config.embeddingBaseUrl,
            embedding_api_key:    config.embeddingApiKey  || undefined,
            embedding_model:      config.embeddingModel,
            embedding_verify_ssl: config.embeddingVerifySsl ?? true,
            knn_neighbors:        config.knnNeighbors,
            llm_base_url:       config.baseUrl,
            llm_model:          config.model,
            llm_api_key:        config.apiKey || undefined,
            llm_provider:       config.provider,
          })
        });

        if (!response.ok) throw new Error(`RAG Backend error! status: ${response.status}`);
        const data = await response.json();
        reply = data.answer;
        sources = data.sources;
        confidence = data.confidence;
        setIsConnected(true);
      } else if (workflow === 'MCP') {
        const activeTool = (config.mcpTools ?? []).find((t: McpTool) => t.id === mcpToolId);
        if (!activeTool?.url) {
          throw new Error("Aucun outil MCP sélectionné ou URL manquante. Configurez un outil MCP dans les paramètres.");
        }
        const response = await fetch('/api/chat/mcp', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            mcp_url: activeTool.url,
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            system_prompt: config.systemPrompt,
          }),
        });
        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `MCP Backend error: ${response.status}`);
        }
        const data = await response.json();
        reply = data.answer;
        setIsConnected(true);

        const assistantMcpMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.tool_calls?.length > 0
            ? data.tool_calls.map((tc: { tool: string; args: Record<string, unknown>; result: string }, i: number) => ({
                id: `mcp-${i}`,
                title: `Tool: ${tc.tool}`,
                status: 'success' as const,
                details: `Args: ${JSON.stringify(tc.args)}\n\nResult: ${tc.result}`,
              }))
            : undefined,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          const nextMessages = [...updated[idx].messages, assistantMcpMsg];
          updated[idx] = { ...updated[idx], messages: nextMessages, memory: buildConversationMemory(nextMessages), updatedAt: Date.now() };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (workflow === 'CREWAI') {
        const response = await fetch('/api/chat/crewai-planning', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: nextPlanningAgentState ?? undefined,
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `CrewAI Planning error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextPlanningAgentState = normalizePlanningAgentState(data.agent_state, browserTimeZone);
        setIsConnected(true);
        setPlannerDraft(nextPlanningAgentState.draft);
        setEditingPlanningPlanId(null);

        const assistantPlanningMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          actions: data.actions,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              planning: nextPlanningAgentState,
            },
            messages: [...updated[idx].messages, assistantPlanningMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantPlanningMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (workflow === 'AGENT' && agentRole === 'manager') {
        const response = await fetch('/api/chat/manager-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            manager_state: managerAgentState ?? undefined,
            clickhouse_state: clickhouseAgentState ?? undefined,
            file_manager_state: fileManagerAgentState ?? undefined,
            data_quality_state: dataQualityAgentState ?? undefined,
            clickhouse: {
              host: config.clickhouseHost,
              port: config.clickhousePort,
              database: config.clickhouseDatabase,
              username: config.clickhouseUsername,
              password: config.clickhousePassword,
              secure: config.clickhouseSecure,
              verify_ssl: config.clickhouseVerifySsl,
              http_path: config.clickhouseHttpPath,
              query_limit: config.clickhouseQueryLimit,
            },
            file_manager_config: {
              base_path: config.fileManagerConfig.basePath,
              max_iterations: config.fileManagerConfig.maxIterations,
              system_prompt: config.fileManagerConfig.systemPrompt,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            system_prompt: config.systemPrompt,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `Manager Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextManagerAgentState = normalizeManagerAgentState((data.agent_state as any)?.manager);
        nextClickhouseAgentState = (data.agent_state as any)?.clickhouse ?? nextClickhouseAgentState;
        nextFileManagerAgentState = normalizeFileManagerAgentState((data.agent_state as any)?.fileManager);
        nextDataQualityAgentState = normalizeDataQualityState((data.agent_state as any)?.dataQuality);
        setIsConnected(true);

        const assistantManagerMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          actions: data.actions,
          chart: data.chart,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              manager: nextManagerAgentState,
              clickhouse: nextClickhouseAgentState,
              fileManager: nextFileManagerAgentState,
              dataQuality: nextDataQualityAgentState,
            },
            messages: [...updated[idx].messages, assistantManagerMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantManagerMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (workflow === 'AGENT' && agentRole === 'clickhouse_query') {
        const response = await fetch('/api/chat/clickhouse-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: clickhouseAgentState ?? undefined,
            clickhouse: {
              host: config.clickhouseHost,
              port: config.clickhousePort,
              database: config.clickhouseDatabase,
              username: config.clickhouseUsername,
              password: config.clickhousePassword,
              secure: config.clickhouseSecure,
              verify_ssl: config.clickhouseVerifySsl,
              http_path: config.clickhouseHttpPath,
              query_limit: config.clickhouseQueryLimit,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `ClickHouse Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextClickhouseAgentState = data.agent_state;
        setIsConnected(true);

        const assistantClickHouseMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          chart: data.chart,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              clickhouse: nextClickhouseAgentState,
            },
            messages: [...updated[idx].messages, assistantClickHouseMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantClickHouseMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (workflow === 'AGENT' && agentRole === 'file_management') {
        const response = await fetch('/api/chat/file-manager-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: fileManagerAgentState ?? undefined,
            file_manager_config: {
              base_path: config.fileManagerConfig.basePath,
              max_iterations: config.fileManagerConfig.maxIterations,
              system_prompt: config.fileManagerConfig.systemPrompt,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `File Management Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextFileManagerAgentState = normalizeFileManagerAgentState(data.agent_state);
        setIsConnected(true);

        const assistantFileManagerMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          actions: data.actions,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              fileManager: nextFileManagerAgentState,
            },
            messages: [...updated[idx].messages, assistantFileManagerMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantFileManagerMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (workflow === 'AGENT' && agentRole === 'data_quality_tables') {
        const response = await fetch('/api/chat/data-quality-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: dataQualityAgentState ?? undefined,
            clickhouse: {
              host: config.clickhouseHost,
              port: config.clickhousePort,
              database: config.clickhouseDatabase,
              username: config.clickhouseUsername,
              password: config.clickhousePassword,
              secure: config.clickhouseSecure,
              verify_ssl: config.clickhouseVerifySsl,
              http_path: config.clickhouseHttpPath,
              query_limit: config.clickhouseQueryLimit,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `Data Quality Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextDataQualityAgentState = normalizeDataQualityState(data.agent_state);
        setIsConnected(true);

        const assistantDataQualityMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              dataQuality: nextDataQualityAgentState,
            },
            messages: [...updated[idx].messages, assistantDataQualityMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantDataQualityMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
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

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: workflow === 'AGENT' && agentRole === 'clickhouse_query'
              ? {
                  ...(updated[idx].agentState ?? {}),
                  clickhouse: nextClickhouseAgentState,
                }
              : updated[idx].agentState,
            messages: [...updated[idx].messages, assistantMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantMsg]),
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
        content: `**Error:** Could not complete the request.\n\n\`\`\`\n${error instanceof Error ? error.message : "Unknown error"}\n\`\`\`\n\nPlease check your configuration settings.`,
        timestamp: Date.now(),
      };
      onConversationsChange(prev => {
        const idx = prev.findIndex(c => c.id === activeConvId);
        if (idx === -1) return prev;
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          messages: [...updated[idx].messages, errorMsg],
          memory: buildConversationMemory([...updated[idx].messages, errorMsg]),
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

  const inputPlaceholder =
    workflow === 'CREWAI'
      ? "Describe the automation you want to schedule..."
      : workflow === 'AGENT' && agentRole === 'clickhouse_query'
        ? "Ask a ClickHouse question or request a chart..."
        : workflow === 'AGENT' && agentRole === 'file_management'
          ? "Ask to list, read, create, move, edit, or delete files..."
        : workflow === 'AGENT' && agentRole === 'data_quality_tables'
          ? "Start the guided setup or paste a structured data-quality JSON payload..."
        : workflow === 'AGENT' && agentRole === 'manager'
          ? "Describe the outcome you want, and the Manager will route it if needed..."
        : workflow === 'MCP'
          ? "Message your MCP tool..."
          : "Message your AI agent...";

  return (
    <div className="flex h-screen relative overflow-hidden bg-[#f5f5f5] dark:bg-[#0f0f13] transition-colors duration-300">
      <div className="mesh-bg" />

      {/* Sidebar */}
      <aside className={`bg-white/60 dark:bg-black/60 border-r border-gray-200/50 dark:border-gray-800/50 backdrop-blur-xl flex flex-col z-20 flex-shrink-0 transition-all duration-300 ease-in-out hidden md:flex ${isSidebarOpen ? 'w-64 md:w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden border-none'}`}>
        <div className="p-4 border-b border-gray-200/50 dark:border-gray-800/50 w-64 md:w-72">
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
              onClick={() => onCurrentIdChange(conv.id)}
              className={`group relative w-full text-left p-3 rounded-xl text-sm transition-all cursor-pointer border ${
                currentId === conv.id
                  ? "bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700 shadow-sm"
                  : "border-transparent hover:bg-white/50 dark:hover:bg-white/5 hover:border-gray-200/50 dark:hover:border-gray-700/50"
              }`}
            >
              <div className="flex items-start gap-3">
                <MessageSquare className={`w-4 h-4 mt-0.5 flex-shrink-0 ${currentId === conv.id ? 'text-blue-500' : 'text-gray-400'}`} />
                <div className="flex-1 overflow-hidden">
                  <div className="truncate font-medium text-gray-900 dark:text-gray-100">{conv.title}</div>
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
            className="p-2 -ml-2 rounded-xl hover:bg-black/5 dark:hover:bg-white/10 text-gray-600 dark:text-gray-300 transition-colors hidden md:block"
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
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white tracking-tight">RAGnarok</h1>
            <div className="w-9 h-9 bg-gradient-to-tr from-slate-700 to-slate-900 rounded-xl flex items-center justify-center shadow-md shadow-slate-900/20">
              <Hammer className="w-5 h-5 text-white" />
            </div>
          </div>
          <div className="flex items-center gap-2 text-[10px] font-medium text-gray-500 dark:text-gray-400 mt-0.5">
            <span className={`w-1.5 h-1.5 rounded-full ${isConnected ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)]' : 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]'}`} />
            {isConnected ? 'Active' : 'Offline'}
          </div>
        </div>

        {/* Right: Settings */}
        <div className="flex items-center justify-end w-1/3 gap-1">
          {currentId && (
            <button
              onClick={clearCurrentChat}
              className="glass-button p-2 rounded-xl text-red-600 dark:text-red-400 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-900/30 flex items-center gap-1.5"
              title="Reset conversation"
            >
              <Trash2 className="w-4 h-4" />
              <span className="hidden sm:inline text-xs font-medium">Reset</span>
            </button>
          )}
          <button
            onClick={onToggleDark}
            className="glass-button p-2 rounded-xl text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white"
            title={isDark ? "Light mode" : "Dark mode"}
          >
            {isDark ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
          </button>
          {onGoHome && (
            <button
              onClick={onGoHome}
              className="glass-button p-2 rounded-xl text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white"
              title="Accueil"
            >
              <Home className="w-4 h-4" />
            </button>
          )}
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
              onAction={handleChatAction}
              showSteps={workflow === 'AGENT' || workflow === 'CREWAI'}
            />
          ))}
          
          {isLoading && (
            <div className="flex gap-4 w-full max-w-[67rem] mx-auto mb-8 animate-fade-in-up">
              <div className="flex-shrink-0 w-10 h-10 rounded-2xl bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 flex items-center justify-center shadow-sm">
                <Bot className="w-5 h-5 text-gray-400 dark:text-gray-500" />
              </div>
              <div className="glass-panel px-6 py-4 rounded-[2rem] rounded-tl-sm flex items-center gap-2">
                <Loader2 className="w-5 h-5 text-blue-500 animate-spin" />
                <span className="text-sm text-gray-500 dark:text-gray-400 font-medium">Thinking...</span>
              </div>
            </div>
          )}

          {workflow === 'AGENT' && agentRole === 'manager' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-3 rounded-xl bg-gradient-to-br from-amber-50 to-orange-50 dark:from-amber-900/20 dark:to-orange-900/20 border border-amber-200/50 dark:border-amber-700/40 shadow-sm animate-fade-in-up">
              <div className="flex items-center gap-2 mb-1.5">
                <Star className="w-4 h-4 text-amber-500 fill-amber-500" />
                <h3 className="font-semibold text-amber-900 dark:text-amber-200 text-[13px]">Agent Manager Brief</h3>
              </div>
              <div className="text-[11px] text-amber-800/90 dark:text-amber-300/90 leading-relaxed space-y-1">
                <p>Welcome to the <strong>Agent Manager</strong> mode. Here is what you can do:</p>
                <ul className="list-disc pl-4 space-y-0.5">
                  <li>Route a request to the best specialist when ClickHouse querying, table quality analysis, or file operations are needed.</li>
                  <li>Keep the conversation context while following clarifications from delegated agents.</li>
                  <li>Answer directly when no specialist tool is necessary.</li>
                </ul>
                <p className="italic mt-1 text-amber-700/70">Tip: Ask for the business outcome you want, and the Manager will decide whether to answer directly or orchestrate a specialist.</p>
              </div>
            </div>
          )}

          {workflow === 'AGENT' && agentRole === 'clickhouse_query' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-3 rounded-xl bg-gradient-to-br from-cyan-50 to-sky-50 dark:from-cyan-900/20 dark:to-sky-900/20 border border-cyan-200/60 dark:border-cyan-700/40 shadow-sm animate-fade-in-up">
              <div className="flex items-center gap-2 mb-1.5">
                <Database className="w-4 h-4 text-cyan-600 dark:text-cyan-300" />
                <h3 className="font-semibold text-cyan-900 dark:text-cyan-200 text-[13px]">ClickHouse Query Agent</h3>
              </div>
              <div className="text-[11px] text-cyan-900/85 dark:text-cyan-300/90 leading-relaxed space-y-1">
                <p>This agent works in English and guides the analysis safely before running SQL.</p>
                <ul className="list-disc pl-4 space-y-0.5">
                  <li>It tries to infer the best table automatically from your question whenever the intent is clear.</li>
                  <li>It only asks you to choose a table, field, or date column when the request stays ambiguous.</li>
                  <li>It can generate charts on demand and also suggests a visualization when the result deserves one.</li>
                  <li>It returns a short final answer, the executed SQL, and a concise reasoning summary.</li>
                </ul>
              </div>
            </div>
          )}

          {workflow === 'AGENT' && agentRole === 'file_management' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-3 rounded-xl bg-gradient-to-br from-emerald-50 to-lime-50 dark:from-emerald-900/20 dark:to-lime-900/20 border border-emerald-200/60 dark:border-emerald-700/40 shadow-sm animate-fade-in-up">
              <div className="flex items-center justify-between gap-4">
                <div className="min-w-0">
                  <div className="flex items-center gap-2 mb-1.5">
                    <FolderOpen className="w-4 h-4 text-emerald-600 dark:text-emerald-300" />
                    <h3 className="font-semibold text-emerald-900 dark:text-emerald-200 text-[13px]">File Management Agent</h3>
                  </div>
                  <div className="text-[11px] text-emerald-900/85 dark:text-emerald-300/90 leading-relaxed space-y-1">
                    <p>This agent works in English and uses backend Python tools to inspect and manage files safely.</p>
                    <ul className="list-disc pl-4 space-y-0.5">
                      <li>Use it to browse folders, read files, summarize CSV or Excel data, and create or edit supported files.</li>
                      <li>Overwrite, move, and delete operations always require an explicit confirmation step before execution.</li>
                      <li>Double-click the agent chip below to configure the sandbox base path, iteration limit, or custom system prompt.</li>
                    </ul>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={() => setIsFileManagerConfigOpen(true)}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-2xl bg-black text-white text-xs font-medium hover:bg-gray-800 transition-colors flex-shrink-0"
                >
                  <Settings className="w-3.5 h-3.5" />
                  Configure
                </button>
              </div>
            </div>
          )}

          {workflow === 'AGENT' && agentRole === 'data_quality_tables' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-4 rounded-xl bg-gradient-to-br from-fuchsia-50 to-rose-50 dark:from-fuchsia-900/20 dark:to-rose-900/20 border border-fuchsia-200/60 dark:border-fuchsia-700/40 shadow-sm animate-fade-in-up">
              <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
                <div>
                  <div className="flex items-center gap-2 mb-1.5">
                    <BarChart3 className="w-4 h-4 text-fuchsia-600 dark:text-fuchsia-300" />
                    <h3 className="font-semibold text-fuchsia-900 dark:text-fuchsia-200 text-[13px]">Data quality - Tables</h3>
                  </div>
                  <div className="text-[11px] text-fuchsia-900/85 dark:text-fuchsia-300/90 leading-relaxed space-y-1">
                    <p>This agent profiles table columns statistically, then uses the local LLM to score data quality and recommend fixes.</p>
                    <ul className="list-disc pl-4 space-y-0.5">
                      <li>It guides you through table, columns, sample size, optional row filter, and optional time column.</li>
                      <li>It accepts a structured JSON payload with <code>__dq__</code> if you want to launch directly.</li>
                      <li>It currently runs on the configured ClickHouse connection and returns the full report in English.</li>
                    </ul>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={() => void handleSend('start guided setup')}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-2xl bg-black text-white text-xs font-medium hover:bg-gray-800 transition-colors flex-shrink-0"
                >
                  <BarChart3 className="w-3.5 h-3.5" />
                  Start guided setup
                </button>
              </div>
            </div>
          )}

          {workflow === 'CREWAI' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-4 rounded-xl bg-gradient-to-br from-emerald-50 via-teal-50 to-cyan-50 dark:from-emerald-900/20 dark:via-teal-900/20 dark:to-cyan-900/20 border border-emerald-200/60 dark:border-emerald-700/40 shadow-sm animate-fade-in-up">
              <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
                <div>
                  <div className="flex items-center gap-2 mb-1.5">
                    <CalendarDays className="w-4 h-4 text-emerald-600 dark:text-emerald-300" />
                    <h3 className="font-semibold text-emerald-900 dark:text-emerald-200 text-[13px]">CrewAI - Planning</h3>
                  </div>
                  <div className="text-[11px] text-emerald-900/85 dark:text-emerald-300/90 leading-relaxed space-y-1">
                    <p>This mode schedules existing agents from natural language or from a guided planner form.</p>
                    <ul className="list-disc pl-4 space-y-0.5">
                      <li>Use natural language to describe what should run, when it should run, and which agents should execute.</li>
                      <li>Open the planner form to configure fixed schedules, ClickHouse watches, or file-arrival triggers.</li>
                      <li>Saved plans are persisted in the backend and executed automatically by the Python server.</li>
                    </ul>
                  </div>
                </div>
                <div className="flex flex-col items-start md:items-end gap-2">
                  <div className="text-[11px] text-emerald-800/80 dark:text-emerald-300/80">
                    {planningState.plans.length} plan(s) · {planningState.runs.length} run(s)
                  </div>
                  <button
                    type="button"
                    onClick={() => openPlanningModal(planningAgentState.draft)}
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-2xl bg-black text-white text-xs font-medium hover:bg-gray-800 transition-colors"
                  >
                    <CalendarDays className="w-3.5 h-3.5" />
                    Open planner form
                  </button>
                </div>
              </div>
            </div>
          )}

          {workflow === 'RAG' && (
            <div className="max-w-[67rem] mx-auto mb-4 p-3 rounded-xl bg-gradient-to-br from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 border border-blue-200/50 dark:border-blue-700/40 shadow-sm animate-fade-in-up">
              <div className="flex items-center gap-2 mb-1.5">
                <Database className="w-4 h-4 text-blue-500" />
                <h3 className="font-semibold text-blue-900 dark:text-blue-200 text-[13px]">Retrieval-Augmented Generation (RAG)</h3>
              </div>
              <div className="text-[11px] text-blue-800/90 dark:text-blue-300/90 leading-relaxed space-y-1">
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
                <div key={att.id} className="relative group flex items-center gap-2 bg-white/60 dark:bg-gray-800/60 border border-gray-200/60 dark:border-gray-700/60 px-3 py-1.5 rounded-xl text-sm shadow-sm">
                  {att.type.startsWith('image/') ? (
                    <img src={att.data} alt={att.name} className="w-6 h-6 object-cover rounded-md" />
                  ) : (
                    <File className="w-4 h-4 text-blue-500 dark:text-blue-400" />
                  )}
                  <span className="truncate max-w-[120px] font-medium text-gray-700 dark:text-gray-200">{att.name}</span>
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
              className="flex-shrink-0 w-12 h-12 rounded-2xl text-gray-400 dark:text-gray-500 hover:text-blue-500 dark:hover:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 flex items-center justify-center transition-colors mb-1 ml-1"
              title="Attach file"
            >
              <Paperclip className="w-5 h-5" />
            </button>
            <textarea
              ref={textareaRef}
              value={input}
              onChange={handleInputChange}
              onKeyDown={handleKeyDown}
              placeholder={inputPlaceholder}
              className="w-full min-h-[56px] bg-transparent border-none resize-none focus:ring-0 px-2 py-4 text-[14px] leading-relaxed text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 outline-none overflow-y-auto"
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
              onClick={() => onWorkflowChange('LLM')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'LLM' ? 'bg-blue-100 dark:bg-blue-900/50 text-blue-700 dark:text-blue-200 border border-blue-200 dark:border-blue-700 shadow-sm' : 'bg-white/50 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white/80 dark:hover:bg-white/10'}`}
            >
              <Cpu className="w-3.5 h-3.5" /> Pure LLM
            </button>
            <button
              onClick={() => onWorkflowChange('RAG')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'RAG' ? 'bg-emerald-100 dark:bg-emerald-900/50 text-emerald-700 dark:text-emerald-200 border border-emerald-200 dark:border-emerald-700 shadow-sm' : 'bg-white/50 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white/80 dark:hover:bg-white/10'}`}
            >
              <Database className="w-3.5 h-3.5" /> RAG Knowledge
            </button>
            <button
              onClick={() => onWorkflowChange('AGENT')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'AGENT' ? 'bg-purple-100 dark:bg-purple-900/50 text-purple-700 dark:text-purple-200 border border-purple-200 dark:border-purple-700 shadow-sm' : 'bg-white/50 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white/80 dark:hover:bg-white/10'}`}
            >
              <Network className="w-3.5 h-3.5" /> Agents
            </button>
            <button
              onClick={() => onWorkflowChange('MCP')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'MCP' ? 'bg-teal-100 dark:bg-teal-900/50 text-teal-700 dark:text-teal-200 border border-teal-200 dark:border-teal-700 shadow-sm' : 'bg-white/50 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white/80 dark:hover:bg-white/10'}`}
            >
              <Cpu className="w-3.5 h-3.5" /> MCP
            </button>
            <button
              onClick={() => onWorkflowChange('CREWAI')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${workflow === 'CREWAI' ? 'bg-emerald-100 dark:bg-emerald-900/50 text-emerald-700 dark:text-emerald-200 border border-emerald-200 dark:border-emerald-700 shadow-sm' : 'bg-white/50 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white/80 dark:hover:bg-white/10'}`}
            >
              <CalendarDays className="w-3.5 h-3.5" /> CrewAI - Planning
            </button>
          </div>

          {/* Sub-options for MCP */}
          <div className={`flex items-center gap-2 overflow-hidden transition-all duration-300 ease-in-out ${workflow === 'MCP' ? 'max-h-10 opacity-100' : 'max-h-0 opacity-0'}`}>
            <div className="flex items-center gap-2 pl-2 border-l-2 border-teal-200 overflow-x-auto pb-1 scrollbar-hide">
              {(config.mcpTools ?? []).length === 0 ? (
                <span className="text-xs text-gray-400 italic">Aucun outil MCP configuré — ouvrez les paramètres.</span>
              ) : (
                (config.mcpTools ?? []).map((tool: McpTool) => (
                  <button
                    key={tool.id}
                    onClick={() => onMcpToolIdChange(tool.id)}
                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${mcpToolId === tool.id ? 'bg-teal-500 text-white shadow-md shadow-teal-500/20' : 'bg-white/60 text-gray-600 border border-gray-200 hover:bg-white'}`}
                  >
                    <Network className="w-3.5 h-3.5" /> {tool.label}
                  </button>
                ))
              )}
            </div>
          </div>

          {/* Sub-options for AGENT */}
          <div className={`overflow-hidden transition-all duration-300 ease-in-out ${workflow === 'AGENT' ? 'max-h-40 opacity-100' : 'max-h-0 opacity-0'}`}>
            <div className="flex flex-col gap-2 pl-2 border-l-2 border-purple-200 pb-1">
              <div className="flex items-center gap-2 overflow-x-auto scrollbar-hide">
                <button
                  onClick={() => onAgentRoleChange('manager')}
                  className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${agentRole === 'manager' ? 'bg-gradient-to-r from-amber-500 to-orange-500 text-white shadow-md shadow-orange-500/20 border-none' : 'bg-white/60 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white dark:hover:bg-white/10'}`}
                >
                  <Star className={`w-3.5 h-3.5 ${agentRole === 'manager' ? 'fill-white text-white' : 'text-amber-500 fill-amber-500'}`} /> Agent Manager
                </button>
                <button
                  onClick={() => setIsOtherAgentsOpen((open) => !open)}
                  className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${isOtherAgentsOpen ? 'bg-purple-100 dark:bg-purple-900/50 text-purple-700 dark:text-purple-200 border border-purple-200 dark:border-purple-700 shadow-sm' : 'bg-white/60 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white dark:hover:bg-white/10'}`}
                >
                  {isOtherAgentsOpen ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                  Other agents
                </button>
              </div>

              <div className={`overflow-hidden transition-all duration-300 ease-in-out ${isOtherAgentsOpen ? 'max-h-24 opacity-100' : 'max-h-0 opacity-0'}`}>
                <div className="flex items-center gap-2 pl-3 ml-1 border-l-2 border-cyan-200 overflow-x-auto scrollbar-hide">
                  <button
                    onClick={() => onAgentRoleChange('clickhouse_query')}
                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${agentRole === 'clickhouse_query' ? 'bg-cyan-500 text-white shadow-md shadow-cyan-500/20' : 'bg-white/60 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white dark:hover:bg-white/10'}`}
                  >
                    <Database className="w-3.5 h-3.5" /> ClickHouse Query
                  </button>
                  <button
                    onClick={() => onAgentRoleChange('file_management')}
                    onDoubleClick={() => setIsFileManagerConfigOpen(true)}
                    title="Double-click to configure"
                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${agentRole === 'file_management' ? 'bg-emerald-500 text-white shadow-md shadow-emerald-500/20' : 'bg-white/60 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white dark:hover:bg-white/10'}`}
                  >
                    <FolderOpen className="w-3.5 h-3.5" /> File management
                  </button>
                  <button
                    onClick={() => onAgentRoleChange('data_quality_tables')}
                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap ${agentRole === 'data_quality_tables' ? 'bg-fuchsia-500 text-white shadow-md shadow-fuchsia-500/20' : 'bg-white/60 dark:bg-white/5 text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:bg-white dark:hover:bg-white/10'}`}
                  >
                    <BarChart3 className="w-3.5 h-3.5" /> Data quality - Tables
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="text-center mt-3 text-xs text-gray-400 dark:text-gray-600 font-medium">
          AI can make mistakes. Consider verifying important information.
        </div>
      </div>
      </div>

      <PlanningModal
        isOpen={isPlanningModalOpen}
        onClose={() => setIsPlanningModalOpen(false)}
        draft={plannerDraft}
        editingPlanId={editingPlanningPlanId}
        planningState={planningState}
        isBusy={planningBusy}
        error={planningError}
        onDraftChange={(draft) => setPlannerDraft(normalizeCrewPlanDraft(draft, browserTimeZone))}
        onStartNewDraft={startNewPlanningDraft}
        onSavePlan={savePlanningPlan}
        onEditPlan={editPlanningPlan}
        onTogglePlanStatus={togglePlanningPlanStatus}
        onDeletePlan={deletePlanningPlan}
        onRunPlan={runPlanningPlan}
        onRefresh={loadPlanningState}
      />
      <FileManagerConfigModal
        isOpen={isFileManagerConfigOpen}
        config={config.fileManagerConfig}
        onClose={() => setIsFileManagerConfigOpen(false)}
        onSave={(nextConfig: FileManagerAgentConfig) => {
          onConfigChange(normalizeAppConfig({
            ...config,
            fileManagerConfig: nextConfig,
          }));
          setIsFileManagerConfigOpen(false);
        }}
      />
    </div>
  );
}
