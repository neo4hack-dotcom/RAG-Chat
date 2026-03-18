import express from 'express';
import cors from 'cors';
import { GoogleGenAI, Type } from '@google/genai';
import dotenv from 'dotenv';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import fs from 'fs';

// Load environment variables from .env file
dotenv.config();

// Initialize Express application
const app = express();
const PORT = 3000;

// Enable Cross-Origin Resource Sharing and JSON parsing
app.use(cors());
app.use(express.json());

// --- RAG IN-MEMORY STORE ---
// Define the structure for a chunk of a document
interface DocumentChunk {
  id: string;
  docId: string;
  docName: string;
  text: string;
  embedding: number[];
  page?: number;
}

// Define the structure for document metadata
interface DocumentMeta {
  id: string;
  name: string;
  size: number;
  uploadedAt: number;
  chunkCount: number;
}

// In-memory storage for documents and their chunks
let documents: DocumentMeta[] = [];
let chunks: DocumentChunk[] = [];

// Initialize the Gemini AI client using the API key from environment variables
const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY || '' });

// --- UTILS ---
// Calculate cosine similarity between two vectors (used for semantic search)
function cosineSimilarity(a: number[], b: number[]) {
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  if (normA === 0 || normB === 0) return 0;
  return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
}

// Simple BM25 / TF-IDF mock (keyword matching score)
function keywordScore(query: string, text: string) {
  const queryTerms = query.toLowerCase().split(/\W+/).filter(t => t.length > 2);
  const textLower = text.toLowerCase();
  let score = 0;
  for (const term of queryTerms) {
    if (textLower.includes(term)) {
      score += 1;
    }
  }
  return score / (queryTerms.length || 1);
}

// Semantic Chunking (sentence-based with overlap)
function chunkText(text: string, maxWords = 200, overlapSentences = 2): string[] {
  // Split by sentence boundaries roughly
  const sentences = text.match(/[^.!?]+[.!?]+/g) || [text];
  const chunks: string[] = [];
  
  let currentChunk: string[] = [];
  let currentWordCount = 0;

  for (let i = 0; i < sentences.length; i++) {
    const sentence = sentences[i].trim();
    if (!sentence) continue;
    
    const wordCount = sentence.split(/\s+/).length;
    
    if (currentWordCount + wordCount > maxWords && currentChunk.length > 0) {
      chunks.push(currentChunk.join(' '));
      // Keep overlap sentences
      const overlap = currentChunk.slice(-overlapSentences);
      currentChunk = [...overlap, sentence];
      currentWordCount = overlap.reduce((acc, s) => acc + s.split(/\s+/).length, 0) + wordCount;
    } else {
      currentChunk.push(sentence);
      currentWordCount += wordCount;
    }
  }
  
  if (currentChunk.length > 0) {
    chunks.push(currentChunk.join(' '));
  }
  
  return chunks;
}

// --- API ROUTES ---

// Chat RAG Pipeline Endpoint
// Handles user queries by retrieving relevant document chunks and generating an answer
app.post('/api/chat/rag', async (req, res) => {
  try {
    const { message, history } = req.body;

    // 1. HyDE (Hypothetical Document Embeddings) & Query Expansion
    // Generate a hypothetical answer to the user's query to capture semantic intent
    const hydeResponse = await ai.models.generateContent({
      model: 'gemini-3-flash-preview',
      contents: `Write a hypothetical, factual answer to the following question to help with semantic search. Do not include conversational filler, just the facts: ${message}`,
    });
    const expandedQuery = hydeResponse.text || message;

    // 2. Embed the expanded query
    // Convert the hypothetical answer into a vector for similarity comparison
    const queryEmbeddingResult = await ai.models.embedContent({
      model: 'gemini-embedding-2-preview',
      contents: [expandedQuery],
    });
    const queryVector = queryEmbeddingResult.embeddings?.[0]?.values || [];

    // 3. Hybrid Search (Vector + Keyword)
    // Combine semantic similarity (cosine) with keyword matching (BM25-like)
    const scoredChunks = chunks.map(chunk => {
      const vecScore = cosineSimilarity(queryVector, chunk.embedding);
      const kwScore = keywordScore(message, chunk.text);
      // RRF (Reciprocal Rank Fusion) simplified: weight vector score higher
      const hybridScore = (vecScore * 0.7) + (kwScore * 0.3);
      return { ...chunk, score: hybridScore, vecScore, kwScore };
    });

    // Sort chunks by hybrid score and take the top 10 candidates
    scoredChunks.sort((a, b) => b.score - a.score);
    let topChunks = scoredChunks.slice(0, 10);

    // 4. Reranking (Using LLM to score relevance)
    // Ask a fast LLM to explicitly score the relevance of the top candidates
    if (topChunks.length > 0) {
      try {
        const rerankPrompt = `You are a relevance scorer. Score the relevance of the following document chunks to the user's query on a scale of 0 to 10.
Query: ${message}

Chunks:
${topChunks.map((c, i) => `[Chunk ${i}]\n${c.text}`).join('\n\n')}

Return a JSON array of objects with 'index' and 'relevanceScore' (0-10).`;

        const rerankResponse = await ai.models.generateContent({
          model: 'gemini-3-flash-preview',
          contents: rerankPrompt,
          config: {
            responseMimeType: 'application/json',
            responseSchema: {
              type: Type.ARRAY,
              items: {
                type: Type.OBJECT,
                properties: {
                  index: { type: Type.INTEGER },
                  relevanceScore: { type: Type.NUMBER }
                },
                required: ['index', 'relevanceScore']
              }
            }
          }
        });

        const rerankScores = JSON.parse(rerankResponse.text || '[]');
        
        // Update scores based on LLM reranking results
        for (const scoreObj of rerankScores) {
          if (scoreObj.index >= 0 && scoreObj.index < topChunks.length) {
            // Normalize LLM score (0-10) to (0-1) and combine with hybrid score
            const llmScore = scoreObj.relevanceScore / 10;
            topChunks[scoreObj.index].score = (topChunks[scoreObj.index].score * 0.3) + (llmScore * 0.7);
          }
        }
        
        // Re-sort based on the new combined scores and take the top 5
        topChunks.sort((a, b) => b.score - a.score);
        topChunks = topChunks.filter(c => c.score > 0.3).slice(0, 5);
      } catch (e) {
        console.error("Reranking failed, falling back to hybrid scores", e);
        // Fallback: just use the hybrid scores if reranking fails
        topChunks = topChunks.filter(c => c.score > 0.2).slice(0, 5);
      }
    }

    // 5. Generate Answer with Citations
    // Construct the final prompt with the retrieved context
    const contextText = topChunks.map((c, i) => `[Source ${i + 1}: ${c.docName}]\n${c.text}`).join('\n\n');
    
    const systemPrompt = `You are a helpful assistant. Use the following retrieved context to answer the user's question. 
Always cite your sources using [1], [2], etc. based on the Source number provided.
If the answer is not in the context, say you don't know based on the provided documents.

Context:
${contextText}`;

    // Call the main LLM to generate the final response
    const generateResponse = await ai.models.generateContent({
      model: 'gemini-3.1-pro-preview',
      contents: [
        { role: 'user', parts: [{ text: systemPrompt }] },
        ...history.map((m: any) => ({
          role: m.role === 'user' ? 'user' : 'model',
          parts: [{ text: m.content }]
        })),
        { role: 'user', parts: [{ text: message }] }
      ]
    });

    // Return the generated answer, the sources used, and the confidence score
    res.json({
      answer: generateResponse.text,
      sources: topChunks.map(c => ({
        id: c.id,
        docName: c.docName,
        text: c.text,
        score: c.score,
      })),
      confidence: topChunks.length > 0 ? topChunks[0].score : 0,
    });
  } catch (error) {
    console.error('RAG error:', error);
    res.status(500).json({ error: 'Failed to generate RAG response' });
  }
});

// --- VITE MIDDLEWARE ---
async function startServer() {
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
