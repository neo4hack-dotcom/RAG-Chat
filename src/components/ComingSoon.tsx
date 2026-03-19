import React from 'react';
import { motion } from 'motion/react';
import { ArrowLeft, Construction, Clock } from 'lucide-react';

type Page = 'landing' | 'chat' | 'dataviz' | 'agents';

interface ComingSoonProps {
  page: 'dataviz' | 'agents';
  onBack: () => void;
}

const PAGE_CONFIG = {
  dataviz: {
    name: 'Agentic Data Viz',
    tagline: 'Intelligent Visualization',
    description:
      'Autonomous AI agents will turn your raw data into interactive visualizations and dynamic narrative insights.',
    gradient: 'linear-gradient(135deg, #d1fae5 0%, #a7f3d0 60%, #99f6e4 100%)',
    orb1: 'rgba(16,185,129,0.25)',
    orb2: 'rgba(52,211,153,0.20)',
    accent: '#10b981',
    accentBg: 'rgba(16,185,129,0.10)',
    features: [
      'Dashboard generation from natural-language prompts',
      'Specialized agents for statistical analysis',
      'Interactive export to HTML / PDF / Notion',
      'BigQuery, Snowflake, and PostgreSQL connectors',
    ],
  },
  agents: {
    name: 'Agents & Tools',
    tagline: 'Multi-Agent Orchestration',
    description:
      'A workflow is a fixed, "If This, Then That" sequence designed for predictable, repetitive tasks like data entry or status updates. In contrast, an AI agent uses reasoning to independently choose tools and adapt its steps to reach a goal.',
    gradient: 'linear-gradient(135deg, #ede9fe 0%, #ddd6fe 60%, #fae8ff 100%)',
    orb1: 'rgba(139,92,246,0.25)',
    orb2: 'rgba(167,139,250,0.20)',
    accent: '#8b5cf6',
    accentBg: 'rgba(139,92,246,0.10)',
    features: [
      'Visual editor for agent graphs',
      'MCP (Model Context Protocol) integration',
      'Shared long-term memory across agents',
      'Session monitoring and replay',
    ],
  },
};

export function ComingSoon({ page, onBack }: ComingSoonProps) {
  const cfg = PAGE_CONFIG[page];

  return (
    <div className="min-h-screen w-full bg-white relative overflow-hidden flex flex-col items-center justify-center px-6">
      {/* Background orbs */}
      <motion.div
        className="fixed pointer-events-none rounded-full"
        style={{
          width: 700,
          height: 700,
          top: '-20%',
          left: '-15%',
          background: cfg.orb1,
          filter: 'blur(100px)',
        }}
        animate={{ scale: [1, 1.08, 1], x: [0, 20, 0] }}
        transition={{ duration: 20, repeat: Infinity, ease: 'easeInOut' }}
      />
      <motion.div
        className="fixed pointer-events-none rounded-full"
        style={{
          width: 500,
          height: 500,
          bottom: '-10%',
          right: '-10%',
          background: cfg.orb2,
          filter: 'blur(80px)',
        }}
        animate={{ scale: [1, 1.1, 1], y: [0, -25, 0] }}
        transition={{ duration: 16, repeat: Infinity, ease: 'easeInOut', delay: 4 }}
      />

      {/* Back button */}
      <motion.button
        initial={{ opacity: 0, x: -16 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.4 }}
        onClick={onBack}
        className="fixed top-6 left-6 z-20 flex items-center gap-2 px-4 py-2.5 rounded-full bg-white/80 backdrop-blur-md border border-gray-200 shadow-sm text-[13px] font-medium text-gray-600 hover:text-gray-900 hover:shadow-md transition-all duration-200"
      >
        <ArrowLeft size={14} />
        Back
      </motion.button>

      {/* Card */}
      <motion.div
        initial={{ opacity: 0, y: 40, scale: 0.96 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.7, ease: [0.16, 1, 0.3, 1] }}
        className="relative z-10 w-full max-w-xl rounded-3xl overflow-hidden shadow-2xl"
        style={{ background: cfg.gradient }}
      >
        {/* Inner shine */}
        <div
          className="absolute top-0 left-0 right-0 h-px"
          style={{ background: 'rgba(255,255,255,0.8)' }}
        />

        <div className="p-10 flex flex-col items-center text-center gap-6">
          {/* Icon */}
          <motion.div
            initial={{ scale: 0.7, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="w-20 h-20 rounded-3xl flex items-center justify-center shadow-lg"
            style={{ background: cfg.accentBg }}
          >
            <Construction size={36} style={{ color: cfg.accent }} strokeWidth={1.6} />
          </motion.div>

          {/* Badge */}
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            className="flex items-center gap-2 px-4 py-1.5 rounded-full"
            style={{ background: cfg.accentBg }}
          >
            <Clock size={12} style={{ color: cfg.accent }} />
            <span className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: cfg.accent }}>
              Under construction
            </span>
          </motion.div>

          {/* Text */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.35 }}
            className="flex flex-col gap-2"
          >
            <h1 className="text-[32px] font-bold tracking-[-0.5px] text-gray-900">{cfg.name}</h1>
            <p className="text-[13px] font-semibold text-gray-400 uppercase tracking-widest">{cfg.tagline}</p>
            <p className="text-[15px] text-gray-600 leading-relaxed mt-2 max-w-sm mx-auto">
              {cfg.description}
            </p>
          </motion.div>

          {/* Features list */}
          <motion.ul
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.5 }}
            className="w-full flex flex-col gap-2 text-left"
          >
            {cfg.features.map((feat, i) => (
              <motion.li
                key={feat}
                initial={{ opacity: 0, x: -12 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.55 + i * 0.07 }}
                className="flex items-center gap-3 px-4 py-3 rounded-2xl bg-white/50 backdrop-blur-sm border border-white/60"
              >
                <span
                  className="w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-[10px] font-bold"
                  style={{ background: cfg.accentBg, color: cfg.accent }}
                >
                  {i + 1}
                </span>
                <span className="text-[13px] text-gray-700 font-medium">{feat}</span>
              </motion.li>
            ))}
          </motion.ul>

          {/* CTA */}
          <motion.button
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.85 }}
            whileHover={{ scale: 1.03 }}
            whileTap={{ scale: 0.97 }}
            onClick={onBack}
            className="mt-2 px-8 py-3.5 rounded-full text-white text-[14px] font-semibold shadow-lg transition-all duration-200"
            style={{ background: `linear-gradient(135deg, ${cfg.accent}, ${cfg.accent}cc)` }}
          >
            Back to home
          </motion.button>
        </div>
      </motion.div>
    </div>
  );
}
