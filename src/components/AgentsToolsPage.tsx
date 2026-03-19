import React from 'react';
import { motion } from 'motion/react';
import { ArrowLeft, ArrowUpRight, Layers, Link2, Sparkles } from 'lucide-react';
import { PortalApp } from '../lib/utils';

interface AgentsToolsPageProps {
  apps: PortalApp[];
  onBack: () => void;
}

function normalizeExternalUrl(rawUrl: string): string {
  const value = rawUrl.trim();
  if (!value) return '';
  if (/^https?:\/\//i.test(value)) return value;
  if (value.startsWith('//')) return `https:${value}`;
  return `https://${value}`;
}

function formatHostLabel(rawUrl: string): string {
  const normalized = normalizeExternalUrl(rawUrl);
  if (!normalized) return 'URL missing';
  try {
    const parsed = new URL(normalized);
    return parsed.host || normalized;
  } catch {
    return normalized.replace(/^https?:\/\//i, '');
  }
}

function PortalTile({ app, index }: { app: PortalApp; index: number }) {
  const targetUrl = normalizeExternalUrl(app.url);

  return (
    <motion.a
      href={targetUrl}
      target="_blank"
      rel="noopener noreferrer"
      initial={{ opacity: 0, y: 24, scale: 0.96 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.5, delay: 0.08 * index, ease: [0.16, 1, 0.3, 1] }}
      whileHover={{ y: -10, scale: 1.015 }}
      whileTap={{ scale: 0.99 }}
      className="group relative min-h-[240px] rounded-[2rem] border border-white/70 bg-[linear-gradient(145deg,rgba(255,255,255,0.78),rgba(255,255,255,0.50),rgba(240,249,255,0.58))] backdrop-blur-2xl p-6 shadow-[0_24px_60px_rgba(15,23,42,0.10)] overflow-hidden block"
    >
      <motion.div
        aria-hidden
        className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300"
        style={{
          background:
            'radial-gradient(circle at 20% 20%, rgba(255,255,255,0.72) 0%, transparent 42%), radial-gradient(circle at 85% 25%, rgba(125,211,252,0.25) 0%, transparent 35%), radial-gradient(circle at 50% 100%, rgba(186,230,253,0.20) 0%, transparent 45%)',
        }}
      />

      <div className="relative z-10 flex h-full flex-col">
        <div className="flex items-start justify-between gap-3">
          <div className="w-12 h-12 rounded-2xl bg-white/75 border border-white/80 shadow-[inset_0_1px_0_rgba(255,255,255,0.9)] flex items-center justify-center">
            <Layers className="w-5 h-5 text-sky-500" />
          </div>
          <div className="flex items-center gap-2 rounded-full border border-white/80 bg-white/65 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-sky-700">
            <Sparkles className="w-3 h-3" />
            App {index + 1}
          </div>
        </div>

        <div className="mt-6 flex-1">
          <h2 className="text-[24px] leading-[1.1] font-semibold tracking-[-0.03em] text-gray-950">
            {app.name}
          </h2>
          <div className="mt-3 inline-flex items-center gap-2 rounded-full border border-white/80 bg-white/60 px-3 py-1.5 text-[12px] font-medium text-gray-600">
            <Link2 className="w-3.5 h-3.5 text-sky-500" />
            {formatHostLabel(app.url)}
          </div>
        </div>

        <div className="relative mt-5 min-h-[88px]">
          <div className="absolute inset-0 transition-all duration-300 group-hover:opacity-0 group-hover:translate-y-2">
            <p className="text-sm leading-relaxed text-gray-500">
              Click to open this application in a new browser tab.
            </p>
          </div>
          <div className="absolute inset-0 opacity-0 translate-y-3 transition-all duration-300 group-hover:opacity-100 group-hover:translate-y-0">
            <p className="text-sm leading-relaxed text-gray-700">
              {app.description || 'No description provided for this application yet.'}
            </p>
          </div>
        </div>

        <div className="relative mt-6 flex items-center justify-between">
          <p className="text-[13px] font-medium text-gray-500">Open external app</p>
          <motion.div
            className="w-11 h-11 rounded-full border border-white/80 bg-white/80 flex items-center justify-center shadow-[0_12px_24px_rgba(14,165,233,0.10)]"
            animate={{ x: 0 }}
            whileHover={{ x: 4 }}
          >
            <ArrowUpRight className="w-4.5 h-4.5 text-sky-600" />
          </motion.div>
        </div>
      </div>
    </motion.a>
  );
}

export function AgentsToolsPage({ apps, onBack }: AgentsToolsPageProps) {
  const configuredApps = apps.filter((app) => app.name.trim() && app.url.trim());

  return (
    <div className="min-h-screen w-full bg-[linear-gradient(180deg,#f8fbff_0%,#f3f7fb_45%,#eef4fb_100%)] relative overflow-hidden">
      <motion.div
        className="fixed inset-0 pointer-events-none"
        aria-hidden
      >
        <div
          className="absolute -top-24 -left-24 w-[28rem] h-[28rem] rounded-full"
          style={{ background: 'rgba(125,211,252,0.20)', filter: 'blur(90px)' }}
        />
        <div
          className="absolute top-[18%] right-[-8%] w-[24rem] h-[24rem] rounded-full"
          style={{ background: 'rgba(191,219,254,0.18)', filter: 'blur(80px)' }}
        />
        <div
          className="absolute bottom-[-8%] left-[20%] w-[26rem] h-[26rem] rounded-full"
          style={{ background: 'rgba(224,231,255,0.22)', filter: 'blur(110px)' }}
        />
      </motion.div>

      <motion.button
        initial={{ opacity: 0, x: -16 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.35 }}
        onClick={onBack}
        className="fixed top-6 left-6 z-20 flex items-center gap-2 px-4 py-2.5 rounded-full bg-white/72 backdrop-blur-xl border border-white/80 shadow-[0_16px_32px_rgba(15,23,42,0.08)] text-[13px] font-medium text-gray-600 hover:text-gray-900 transition-all"
      >
        <ArrowLeft size={14} />
        Back
      </motion.button>

      <div className="relative z-10 max-w-7xl mx-auto px-6 pt-24 pb-24">
        <motion.div
          initial={{ opacity: 0, y: 18 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.55, ease: [0.16, 1, 0.3, 1] }}
          className="max-w-3xl"
        >
          <div className="inline-flex items-center gap-2 rounded-full border border-white/80 bg-white/65 px-4 py-2 text-[11px] font-semibold uppercase tracking-[0.2em] text-sky-700 shadow-[0_12px_24px_rgba(15,23,42,0.05)]">
            <Sparkles className="w-3.5 h-3.5" />
            Multi-Agent Orchestration
          </div>
          <h1 className="mt-6 text-[44px] md:text-[58px] leading-[0.95] tracking-[-0.05em] font-semibold text-gray-950">
            Agents & Tools
          </h1>
          <p className="mt-5 text-[16px] leading-[1.8] text-gray-600 max-w-2xl">
            A workflow is a fixed, "If This, Then That" sequence designed for predictable, repetitive tasks like data entry or status updates. In contrast, an AI agent uses reasoning to independently choose tools and adapt its steps to reach a goal.
          </p>
          <div className="mt-6 inline-flex items-center gap-2 rounded-full border border-white/80 bg-white/62 px-4 py-2 text-sm font-medium text-gray-700">
            <Layers className="w-4 h-4 text-sky-500" />
            {configuredApps.length} configured app{configuredApps.length === 1 ? '' : 's'}
          </div>
        </motion.div>

        {configuredApps.length === 0 ? (
          <motion.div
            initial={{ opacity: 0, y: 26 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.15, duration: 0.45 }}
            className="mt-14 max-w-2xl rounded-[2rem] border border-white/75 bg-[linear-gradient(145deg,rgba(255,255,255,0.78),rgba(255,255,255,0.58),rgba(248,250,252,0.68))] backdrop-blur-2xl p-8 shadow-[0_28px_60px_rgba(15,23,42,0.08)]"
          >
            <p className="text-lg font-semibold text-gray-900">No application has been configured yet.</p>
            <p className="mt-3 text-sm leading-relaxed text-gray-600">
              Open the protected settings panel, go to <strong>App Portal</strong>, and add the apps you want to expose here.
            </p>
          </motion.div>
        ) : (
          <div className="mt-14 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
            {configuredApps.map((app, index) => (
              <PortalTile key={app.id || `portal-app-${index}`} app={app} index={index} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
