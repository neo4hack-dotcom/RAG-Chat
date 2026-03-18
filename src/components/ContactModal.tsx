import React, { useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { X, Send, CheckCircle, Bug, MessageCircle } from 'lucide-react';

interface ContactModalProps {
  isOpen: boolean;
  onClose: () => void;
}

type SubjectType = 'question' | 'bug';

export function ContactModal({ isOpen, onClose }: ContactModalProps) {
  const [subject, setSubject] = useState<SubjectType>('question');
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [message, setMessage] = useState('');
  const [submitted, setSubmitted] = useState(false);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // Simulate submission
    setSubmitted(true);
  };

  const handleClose = () => {
    onClose();
    setTimeout(() => {
      setSubmitted(false);
      setName('');
      setEmail('');
      setMessage('');
      setSubject('question');
    }, 300);
  };

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="fixed inset-0 z-50 bg-black/20 backdrop-blur-sm"
            onClick={handleClose}
          />

          {/* Modal */}
          <motion.div
            initial={{ opacity: 0, scale: 0.94, y: 20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.96, y: 10 }}
            transition={{ duration: 0.35, ease: [0.16, 1, 0.3, 1] }}
            className="fixed inset-0 z-50 flex items-center justify-center p-4 pointer-events-none"
          >
            <div
              className="pointer-events-auto w-full max-w-lg bg-white rounded-3xl shadow-2xl overflow-hidden"
              onClick={e => e.stopPropagation()}
            >
              {/* Header */}
              <div className="flex items-center justify-between px-8 pt-8 pb-6 border-b border-gray-100">
                <div>
                  <h2 className="text-[20px] font-semibold text-gray-900 tracking-tight">
                    Contact Us
                  </h2>
                  <p className="text-[13px] text-gray-400 mt-0.5">
                    We typically reply within 24 hours.
                  </p>
                </div>
                <button
                  onClick={handleClose}
                  className="w-8 h-8 rounded-full bg-gray-100 hover:bg-gray-200 flex items-center justify-center transition-colors duration-150"
                >
                  <X size={14} className="text-gray-500" />
                </button>
              </div>

              <AnimatePresence mode="wait">
                {submitted ? (
                  /* Success state */
                  <motion.div
                    key="success"
                    initial={{ opacity: 0, y: 16 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.4 }}
                    className="flex flex-col items-center justify-center gap-4 px-8 py-14 text-center"
                  >
                    <div className="w-16 h-16 rounded-full bg-emerald-50 flex items-center justify-center">
                      <CheckCircle size={32} className="text-emerald-500" strokeWidth={1.6} />
                    </div>
                    <div>
                      <p className="text-[17px] font-semibold text-gray-900">Message sent!</p>
                      <p className="text-[13px] text-gray-500 mt-1">
                        Thanks for reaching out. We'll get back to you shortly.
                      </p>
                    </div>
                    <button
                      onClick={handleClose}
                      className="mt-2 px-6 py-2.5 rounded-full bg-gray-900 text-white text-[13px] font-medium hover:bg-gray-700 transition-colors duration-200"
                    >
                      Close
                    </button>
                  </motion.div>
                ) : (
                  /* Form */
                  <motion.form
                    key="form"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    onSubmit={handleSubmit}
                    className="px-8 py-6 flex flex-col gap-5"
                  >
                    {/* Subject toggle */}
                    <div>
                      <label className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-2 block">
                        Subject
                      </label>
                      <div className="flex gap-2">
                        {([
                          { value: 'question', label: 'Ask a question', Icon: MessageCircle },
                          { value: 'bug', label: 'Report a bug', Icon: Bug },
                        ] as { value: SubjectType; label: string; Icon: React.FC<any> }[]).map(opt => (
                          <button
                            key={opt.value}
                            type="button"
                            onClick={() => setSubject(opt.value)}
                            className="flex items-center gap-2 px-4 py-2.5 rounded-full text-[13px] font-medium transition-all duration-200 border"
                            style={
                              subject === opt.value
                                ? {
                                    background: opt.value === 'bug' ? 'rgba(239,68,68,0.08)' : 'rgba(99,102,241,0.08)',
                                    borderColor: opt.value === 'bug' ? 'rgba(239,68,68,0.3)' : 'rgba(99,102,241,0.3)',
                                    color: opt.value === 'bug' ? '#ef4444' : '#6366f1',
                                  }
                                : {
                                    background: 'transparent',
                                    borderColor: '#e5e7eb',
                                    color: '#6b7280',
                                  }
                            }
                          >
                            <opt.Icon size={14} />
                            {opt.label}
                          </button>
                        ))}
                      </div>
                    </div>

                    {/* Name + Email row */}
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-1.5 block">
                          Name
                        </label>
                        <input
                          type="text"
                          required
                          value={name}
                          onChange={e => setName(e.target.value)}
                          placeholder="Jane Smith"
                          className="w-full px-4 py-2.5 rounded-xl border border-gray-200 bg-gray-50 text-[14px] text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-transparent transition-all duration-200"
                        />
                      </div>
                      <div>
                        <label className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-1.5 block">
                          Email
                        </label>
                        <input
                          type="email"
                          required
                          value={email}
                          onChange={e => setEmail(e.target.value)}
                          placeholder="jane@company.com"
                          className="w-full px-4 py-2.5 rounded-xl border border-gray-200 bg-gray-50 text-[14px] text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-transparent transition-all duration-200"
                        />
                      </div>
                    </div>

                    {/* Message */}
                    <div>
                      <label className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-1.5 block">
                        Message
                      </label>
                      <textarea
                        required
                        rows={4}
                        value={message}
                        onChange={e => setMessage(e.target.value)}
                        placeholder={
                          subject === 'bug'
                            ? 'Describe the bug and steps to reproduce it...'
                            : 'What would you like to know?'
                        }
                        className="w-full px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 text-[14px] text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-transparent transition-all duration-200 resize-none"
                      />
                    </div>

                    {/* Submit */}
                    <motion.button
                      type="submit"
                      whileHover={{ scale: 1.02 }}
                      whileTap={{ scale: 0.98 }}
                      className="w-full py-3 rounded-full bg-gray-900 text-white text-[14px] font-semibold flex items-center justify-center gap-2 hover:bg-gray-700 transition-colors duration-200"
                    >
                      <Send size={14} />
                      Send message
                    </motion.button>
                  </motion.form>
                )}
              </AnimatePresence>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}
