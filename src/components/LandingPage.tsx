import React, { useRef, useState, useEffect } from 'react';
import { motion, useInView, useMotionValue, useSpring } from 'motion/react';
import { ArrowRight, Brain, BarChart3, Cpu, Sparkles } from 'lucide-react';

type Page = 'landing' | 'chat' | 'dataviz' | 'agents';

interface LandingPageProps {
  onNavigate: (page: Page) => void;
}

const CARDS = [
  {
    id: 'chat' as Page,
    name: 'RAGnarok',
    tagline: 'Retrieval-Augmented Generation',
    description:
      'Interrogez vos données avec une IA de pointe. Connectez Elasticsearch et dialoguez avec vos documents en temps réel.',
    Icon: Brain,
    gradient: 'linear-gradient(135deg, #e0e7ff 0%, #c7d2fe 60%, #ddd6fe 100%)',
    hoverGradient: 'linear-gradient(135deg, #c7d2fe 0%, #a5b4fc 60%, #c4b5fd 100%)',
    shadowColor: 'rgba(99,102,241,0.40)',
    glowColor: 'rgba(99,102,241,0.18)',
    iconBg: 'rgba(99,102,241,0.15)',
    iconColor: '#6366f1',
    tag: 'Disponible',
    tagColor: '#6366f1',
    tagBg: 'rgba(99,102,241,0.12)',
  },
  {
    id: 'dataviz' as Page,
    name: 'Agentic Data Viz',
    tagline: 'Visualisation Intelligente',
    description:
      'Transformez des données brutes en visualisations dynamiques générées par des agents IA autonomes.',
    Icon: BarChart3,
    gradient: 'linear-gradient(135deg, #d1fae5 0%, #a7f3d0 60%, #99f6e4 100%)',
    hoverGradient: 'linear-gradient(135deg, #a7f3d0 0%, #6ee7b7 60%, #5eead4 100%)',
    shadowColor: 'rgba(16,185,129,0.40)',
    glowColor: 'rgba(16,185,129,0.18)',
    iconBg: 'rgba(16,185,129,0.15)',
    iconColor: '#10b981',
    tag: 'Bientôt',
    tagColor: '#059669',
    tagBg: 'rgba(16,185,129,0.12)',
  },
  {
    id: 'agents' as Page,
    name: 'Agents & Tools',
    tagline: 'Orchestration Multi-Agents',
    description:
      "Déployez des réseaux d'agents intelligents capables de planifier, collaborer et exécuter des tâches complexes.",
    Icon: Cpu,
    gradient: 'linear-gradient(135deg, #ede9fe 0%, #ddd6fe 60%, #fae8ff 100%)',
    hoverGradient: 'linear-gradient(135deg, #ddd6fe 0%, #c4b5fd 60%, #f5d0fe 100%)',
    shadowColor: 'rgba(139,92,246,0.40)',
    glowColor: 'rgba(139,92,246,0.18)',
    iconBg: 'rgba(139,92,246,0.15)',
    iconColor: '#8b5cf6',
    tag: 'Bientôt',
    tagColor: '#7c3aed',
    tagBg: 'rgba(139,92,246,0.12)',
  },
];

/* ---------- Tilt card with cursor spotlight ---------- */
function FeatureCard({
  card,
  index,
  onNavigate,
}: {
  card: (typeof CARDS)[0];
  index: number;
  onNavigate: (page: Page) => void;
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
    // tilt: max ±8deg
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
      onClick={() => onNavigate(card.id)}
      className="relative cursor-pointer select-none"
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
        className="relative overflow-hidden rounded-3xl p-8 flex flex-col gap-5"
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
            background: `radial-gradient(circle 220px at ${spotlight.x}% ${spotlight.y}%, rgba(255,255,255,0.55) 0%, transparent 70%)`,
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
          <h3 className="text-[22px] font-semibold tracking-[-0.3px] text-gray-900">
            {card.name}
          </h3>
          <p className="text-[13px] font-medium text-gray-500 uppercase tracking-widest">
            {card.tagline}
          </p>
          <p className="text-[14px] leading-[1.65] text-gray-600 mt-1">
            {card.description}
          </p>
        </div>

        {/* CTA row */}
        <motion.div
          className="flex items-center gap-2 relative z-10"
          animate={{ x: hovered ? 4 : 0 }}
          transition={{ duration: 0.3 }}
        >
          <span
            className="text-[14px] font-semibold"
            style={{ color: card.iconColor }}
          >
            {card.id === 'chat' ? 'Accéder' : 'En savoir plus'}
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
        { w: 600, h: 600, top: '-15%', left: '-10%', color: 'rgba(199,210,254,0.45)', dur: 18 },
        { w: 500, h: 500, top: '55%', right: '-8%', color: 'rgba(167,243,208,0.40)', dur: 22 },
        { w: 450, h: 450, top: '20%', left: '55%', color: 'rgba(221,214,254,0.35)', dur: 26 },
        { w: 350, h: 350, top: '70%', left: '10%', color: 'rgba(253,230,138,0.28)', dur: 20 },
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

/* ---------- Animated headline words ---------- */
function HeroHeadline() {
  const words = ['Intelligence', 'Sans', 'Limites.'];
  const colors = ['#1d1d1f', '#1d1d1f', 'url(#grad)'];

  return (
    <div className="flex flex-wrap justify-center gap-x-4 gap-y-2">
      <svg width="0" height="0" className="absolute">
        <defs>
          <linearGradient id="grad" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#6366f1" />
            <stop offset="50%" stopColor="#8b5cf6" />
            <stop offset="100%" stopColor="#06b6d4" />
          </linearGradient>
        </defs>
      </svg>
      {words.map((word, i) => (
        <motion.span
          key={word}
          initial={{ opacity: 0, y: 30, filter: 'blur(8px)' }}
          animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
          transition={{ duration: 0.7, delay: 0.1 + i * 0.1, ease: [0.16, 1, 0.3, 1] }}
          className="text-[56px] md:text-[72px] font-bold tracking-[-2px] leading-none"
          style={
            colors[i] === 'url(#grad)'
              ? { backgroundImage: 'linear-gradient(90deg,#6366f1,#8b5cf6,#06b6d4)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text' }
              : { color: colors[i] }
          }
        >
          {word}
        </motion.span>
      ))}
    </div>
  );
}

/* ---------- Main component ---------- */
export function LandingPage({ onNavigate }: LandingPageProps) {
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
        <div className="flex items-center gap-2.5">
          <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shadow-lg shadow-indigo-200">
            <Sparkles size={16} className="text-white" strokeWidth={2} />
          </div>
          <span className="text-[15px] font-semibold text-gray-900 tracking-tight">RAGnarok</span>
        </div>
        <div className="flex items-center gap-1">
          {['Produit', 'Documentation', 'À propos'].map(item => (
            <button
              key={item}
              className="px-4 py-2 text-[13px] text-gray-500 hover:text-gray-900 font-medium rounded-full hover:bg-gray-100 transition-all duration-200"
            >
              {item}
            </button>
          ))}
        </div>
      </motion.nav>

      {/* Hero */}
      <section className="relative z-10 flex flex-col items-center text-center px-6 pt-16 pb-20 max-w-5xl mx-auto">
        {/* Pill badge */}
        <motion.div
          initial={{ opacity: 0, scale: 0.85 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.5, delay: 0.05 }}
          className="mb-8 inline-flex items-center gap-2 px-4 py-2 rounded-full border border-gray-200 bg-white/80 backdrop-blur-sm shadow-sm"
        >
          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
          <span className="text-[12px] font-medium text-gray-600 tracking-wide">
            Plateforme IA · v2.0
          </span>
        </motion.div>

        <HeroHeadline />

        <motion.p
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.45 }}
          className="mt-7 text-[19px] text-gray-500 leading-relaxed max-w-2xl font-light"
        >
          Trois expériences. Un écosystème unifié.
          <br />
          Explorez la puissance des agents IA, de la RAG et de la visualisation intelligente.
        </motion.p>

        {/* Scroll hint */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 1.2, duration: 0.8 }}
          className="mt-14 flex flex-col items-center gap-2"
        >
          <span className="text-[12px] text-gray-400 font-medium tracking-widest uppercase">
            Choisissez votre expérience
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
          {CARDS.map((card, i) => (
            <FeatureCard key={card.id} card={card} index={i} onNavigate={onNavigate} />
          ))}
        </div>
      </section>

      {/* Footer */}
      <motion.footer
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 1, duration: 0.5 }}
        className="relative z-10 text-center pb-10 text-[12px] text-gray-400 tracking-wide"
      >
        © 2026 RAGnarok · Plateforme IA Générative
      </motion.footer>
    </div>
  );
}
