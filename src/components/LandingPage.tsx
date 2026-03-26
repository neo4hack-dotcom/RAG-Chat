import React, { useRef, useState } from 'react';
import { motion, useMotionValue, useSpring } from 'motion/react';
import { ArrowRight, Brain, BarChart3, Cpu } from 'lucide-react';
import { ContactModal } from './ContactModal';

type Page = 'landing' | 'chat' | 'dataviz' | 'agents' | 'admin';

interface LandingPageProps {
  onNavigate: (page: Page) => void;
  documentationUrl?: string;
  agenticDataVizUrl?: string;
  portalAppsCount?: number;
}

const CARDS = [
  {
    id: 'chat' as Page,
    name: 'RAGnarok',
    tagline: 'Retrieval-Augmented Generation',
    description:
      'Connect your knowledge base, search precisely, and work with grounded answers in a clean AI workspace.',
    Icon: Brain,
    gradient: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 62%, #eef2ff 100%)',
    hoverGradient: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 52%, #e0e7ff 100%)',
    shadowColor: 'rgba(15,23,42,0.14)',
    iconBg: 'rgba(37,99,235,0.10)',
    iconColor: '#2563eb',
    tag: 'Available',
    tagColor: '#1d4ed8',
    tagBg: 'rgba(37,99,235,0.10)',
  },
  {
    id: 'dataviz' as Page,
    name: 'Agentic Data Viz',
    tagline: 'Intelligent Visualization',
    description:
      'Route analytical outputs into a dedicated visualization surface when a richer visual narrative is needed.',
    Icon: BarChart3,
    gradient: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 58%, #f1f5f9 100%)',
    hoverGradient: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 52%, #ecfccb 100%)',
    shadowColor: 'rgba(15,23,42,0.12)',
    iconBg: 'rgba(21,128,61,0.10)',
    iconColor: '#15803d',
    tag: 'Coming Soon',
    tagColor: '#166534',
    tagBg: 'rgba(21,128,61,0.10)',
  },
  {
    id: 'agents' as Page,
    name: 'Agents & Tools',
    tagline: 'Multi-Agent Orchestration',
    description:
      'Open the application portal and move between specialist tools with a cleaner, more operational workflow.',
    Icon: Cpu,
    gradient: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 54%, #f1f5f9 100%)',
    hoverGradient: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 50%, #ede9fe 100%)',
    shadowColor: 'rgba(15,23,42,0.12)',
    iconBg: 'rgba(79,70,229,0.10)',
    iconColor: '#4f46e5',
    tag: 'App Portal',
    tagColor: '#4338ca',
    tagBg: 'rgba(79,70,229,0.10)',
  },
];

/* ---------- Tilt card with cursor spotlight ---------- */
function FeatureCard({
  card,
  index,
  onNavigate,
  agenticDataVizUrl,
}: {
  card: (typeof CARDS)[0];
  index: number;
  onNavigate: (page: Page) => void;
  agenticDataVizUrl?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [hovered, setHovered] = useState(false);
  const [spotlight, setSpotlight] = useState({ x: 50, y: 50 });

  const rotateX = useMotionValue(0);
  const rotateY = useMotionValue(0);
  const springConfig = { stiffness: 260, damping: 30 };
  const sRotX = useSpring(rotateX, springConfig);
  const sRotY = useSpring(rotateY, springConfig);

  const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
    const el = ref.current;
    if (!el) return;
    const { left, top, width, height } = el.getBoundingClientRect();
    const x = e.clientX - left;
    const y = e.clientY - top;
    rotateY.set(((x / width) - 0.5) * 16);
    rotateX.set(-((y / height) - 0.5) * 16);
    setSpotlight({ x: (x / width) * 100, y: (y / height) * 100 });
  };

  const handleMouseLeave = () => {
    rotateX.set(0);
    rotateY.set(0);
    setHovered(false);
  };

  const { Icon } = card;
  const datavizRedirectActive = card.id === 'dataviz' && Boolean(agenticDataVizUrl?.trim());

  return (
    <motion.div
      ref={ref}
      initial={{ opacity: 0, y: 60 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.7, delay: 0.3 + index * 0.12, ease: [0.16, 1, 0.3, 1] }}
      style={{ perspective: 1200, rotateX: sRotX, rotateY: sRotY }}
      onMouseMove={handleMouseMove}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={handleMouseLeave}
      onClick={() => {
        if (datavizRedirectActive) {
          window.open(agenticDataVizUrl!.trim(), '_blank', 'noopener,noreferrer');
          return;
        }
        onNavigate(card.id);
      }}
      className="relative cursor-pointer select-none h-full"
    >
      {/* Outer glow ring */}
      <motion.div
        className="absolute inset-0 rounded-3xl pointer-events-none"
        animate={{
          boxShadow: hovered
            ? `0 0 0 1.5px ${card.iconColor}55, 0 32px 64px -8px ${card.shadowColor}`
            : `0 0 0 0px transparent, 0 8px 24px -4px rgba(0,0,0,0.06)`,
        }}
        transition={{ duration: 0.35 }}
        style={{ borderRadius: 24 }}
      />

      {/* Card body */}
      <motion.div
        className="relative overflow-hidden rounded-3xl p-8 flex flex-col gap-5 h-full"
        animate={{
          background: hovered ? card.hoverGradient : card.gradient,
          y: hovered ? -10 : 0,
        }}
        transition={{ duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
        style={{ minHeight: 340 }}
      >
        {/* Cursor spotlight */}
        <div
          className="absolute inset-0 pointer-events-none rounded-3xl transition-opacity duration-300"
          style={{
            opacity: hovered ? 1 : 0,
            background: `radial-gradient(circle 220px at ${spotlight.x}% ${spotlight.y}%, rgba(255,255,255,0.62) 0%, transparent 72%)`,
          }}
        />

        {/* Top row: icon + tag */}
        <div className="flex items-start justify-between relative z-10">
          <motion.div
            className="w-14 h-14 rounded-2xl flex items-center justify-center"
            style={{ background: card.iconBg }}
            animate={{ scale: hovered ? 1.08 : 1 }}
            transition={{ duration: 0.3 }}
          >
            <Icon size={26} style={{ color: card.iconColor }} strokeWidth={1.8} />
          </motion.div>

          <span
            className="text-[11px] font-semibold px-3 py-1 rounded-full tracking-wide"
            style={{ color: card.tagColor, background: card.tagBg }}
          >
            {card.tag}
          </span>
        </div>

        {/* Content */}
        <div className="flex flex-col gap-2 relative z-10 flex-1">
          <h3 className="text-[22px] font-semibold tracking-[-0.3px] text-slate-950">
            {card.name}
          </h3>
          <p className="text-[13px] font-medium text-slate-500 uppercase tracking-widest">
            {card.tagline}
          </p>
          <p className="text-[14px] leading-[1.65] text-slate-600 mt-1">
            {card.description}
          </p>
        </div>

        {/* CTA row */}
        <motion.div
          className="flex items-center gap-2 relative z-10"
          animate={{ x: hovered ? 4 : 0 }}
          transition={{ duration: 0.3 }}
        >
          <span className="text-[14px] font-semibold" style={{ color: card.iconColor }}>
            {card.id === 'chat'
              ? 'Get started'
              : card.id === 'agents'
                ? 'Open portal'
                : datavizRedirectActive
                  ? 'Open app'
                  : 'Learn more'}
          </span>
          <motion.div animate={{ x: hovered ? 4 : 0 }} transition={{ duration: 0.25 }}>
            <ArrowRight size={16} style={{ color: card.iconColor }} />
          </motion.div>
        </motion.div>
      </motion.div>
    </motion.div>
  );
}

/* ---------- Floating orb background ---------- */
function FloatingOrbs() {
  return (
    <div className="fixed inset-0 pointer-events-none overflow-hidden" aria-hidden>
      {[
        { w: 620, h: 620, top: '-18%', left: '-10%', color: 'rgba(226,232,240,0.55)', dur: 18 },
        { w: 520, h: 520, top: '52%', right: '-8%', color: 'rgba(241,245,249,0.48)', dur: 22 },
        { w: 430, h: 430, top: '18%', left: '58%', color: 'rgba(219,234,254,0.22)', dur: 26 },
        { w: 340, h: 340, top: '72%', left: '12%', color: 'rgba(229,231,235,0.35)', dur: 20 },
      ].map((orb, i) => (
        <motion.div
          key={i}
          className="absolute rounded-full"
          style={{
            width: orb.w,
            height: orb.h,
            top: orb.top,
            left: (orb as any).left,
            right: (orb as any).right,
            background: orb.color,
            filter: 'blur(80px)',
          }}
          animate={{ y: [0, -30, 0], x: [0, 15, 0], scale: [1, 1.06, 1] }}
          transition={{ duration: orb.dur, repeat: Infinity, ease: 'easeInOut', delay: i * 3 }}
        />
      ))}
    </div>
  );
}

/* ---------- Hero headline: "ODIN AI Portal" ---------- */
function HeroHeadline() {
  // "ODIN" gets the gradient, "AI Portal" stays dark
  const words = [
    { text: 'ODIN', gradient: true },
    { text: 'AI', gradient: false },
    { text: 'Portal', gradient: false },
  ];

  return (
    <div className="flex flex-wrap justify-center gap-x-4 gap-y-2">
      {words.map((word, i) => (
        <motion.span
          key={word.text}
          initial={{ opacity: 0, y: 30, filter: 'blur(8px)' }}
          animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
          transition={{ duration: 0.7, delay: 0.1 + i * 0.1, ease: [0.16, 1, 0.3, 1] }}
          className="text-[56px] md:text-[72px] font-bold tracking-[-2px] leading-none"
          style={
            word.gradient
              ? {
                  backgroundImage: 'linear-gradient(90deg,#111827,#334155,#2563eb)',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent',
                  backgroundClip: 'text',
                }
              : { color: '#1d1d1f' }
          }
        >
          {word.text}
        </motion.span>
      ))}
    </div>
  );
}

/* ---------- Main component ---------- */
export function LandingPage({ onNavigate, documentationUrl, agenticDataVizUrl, portalAppsCount = 0 }: LandingPageProps) {
  const [contactOpen, setContactOpen] = useState(false);
  const cards = CARDS.map((card) =>
    card.id === 'agents'
      ? {
          ...card,
          tag: portalAppsCount > 0 ? `${portalAppsCount} App${portalAppsCount > 1 ? 's' : ''}` : 'App Portal',
        }
      : card.id === 'dataviz'
        ? {
            ...card,
            tag: agenticDataVizUrl?.trim() ? 'External App' : card.tag,
          }
        : card
  );

  return (
    <div className="min-h-screen w-full bg-white overflow-auto relative">
      <FloatingOrbs />

      {/* Nav bar */}
      <motion.nav
        initial={{ opacity: 0, y: -16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, ease: 'easeOut' }}
        className="relative z-10 flex items-center justify-between px-8 py-5 max-w-7xl mx-auto"
      >
        <div />
        <div className="flex items-center gap-1">
          {documentationUrl ? (
            <a
              href={documentationUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="px-4 py-2 text-[13px] text-gray-500 hover:text-gray-900 font-medium rounded-full hover:bg-gray-100 transition-all duration-200"
            >
              Documentation
            </a>
          ) : null}
          <button
            onClick={() => setContactOpen(true)}
            className="px-4 py-2 text-[13px] text-gray-500 hover:text-gray-900 font-medium rounded-full hover:bg-gray-100 transition-all duration-200"
          >
            Contact us
          </button>
        </div>
      </motion.nav>

      {/* Hero */}
      <section className="relative z-10 flex flex-col items-center text-center px-6 pt-16 pb-20 max-w-5xl mx-auto">
        <HeroHeadline />

        <motion.div
          initial={{ opacity: 0, y: 18 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.7, delay: 0.45, ease: [0.16, 1, 0.3, 1] }}
          className="mt-6 max-w-3xl"
        >
          <div className="text-[13px] font-semibold uppercase tracking-[0.28em] text-slate-500">
            Your AI Augmented AI assistant
          </div>
          <p className="mt-5 text-[18px] leading-[1.8] text-slate-600 md:text-[20px]">
            The true power of AI agents with MCP providers. Building the bridge that connects the autonomy to real-world data and tools.
          </p>
        </motion.div>

        {/* Scroll hint */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.9, duration: 0.8 }}
          className="mt-14 flex flex-col items-center gap-2"
        >
          <span className="text-[12px] text-gray-400 font-medium tracking-widest uppercase">
            Choose your experience
          </span>
          <motion.div
            animate={{ y: [0, 6, 0] }}
            transition={{ duration: 1.6, repeat: Infinity, ease: 'easeInOut' }}
            className="w-5 h-8 rounded-full border-2 border-gray-200 flex items-start justify-center pt-1.5"
          >
            <div className="w-1 h-2 rounded-full bg-gray-400" />
          </motion.div>
        </motion.div>
      </section>

      {/* Cards grid */}
      <section className="relative z-10 px-6 pb-28 max-w-6xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {cards.map((card, i) => (
            <FeatureCard key={card.id} card={card} index={i} onNavigate={onNavigate} agenticDataVizUrl={agenticDataVizUrl} />
          ))}
        </div>
      </section>

      {/* Contact modal */}
      <ContactModal isOpen={contactOpen} onClose={() => setContactOpen(false)} />
    </div>
  );
}
