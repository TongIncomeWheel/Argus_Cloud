"""
Strategy Instructions Module
Handles markdown file uploads, chunking, and semantic retrieval for AI context
"""

import os
import json
import re
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import pandas as pd
import streamlit as st

# Try to import sentence-transformers for embeddings
try:
    from sentence_transformers import SentenceTransformer
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    SentenceTransformer = None

# Fallback: simple keyword-based search
import numpy as np
from collections import Counter


class StrategyInstructions:
    """Manage strategy instructions from markdown files with semantic search"""
    
    def __init__(self, storage_dir: str = "strategy_data"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        
        self.chunks_file = self.storage_dir / "chunks.json"
        self.embeddings_file = self.storage_dir / "embeddings.npy"
        self.metadata_file = self.storage_dir / "metadata.json"
        
        # Initialize embeddings model if available
        self.embedding_model = None
        if EMBEDDINGS_AVAILABLE:
            try:
                # Use a lightweight model for efficiency
                self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            except Exception as e:
                st.warning(f"Could not load embedding model: {e}")
                self.embedding_model = None
        
        # Load existing data
        self.chunks = self._load_chunks()
        self.embeddings = self._load_embeddings()
        self.metadata = self._load_metadata()
    
    def _load_chunks(self) -> List[Dict]:
        """Load chunks from storage"""
        if self.chunks_file.exists():
            try:
                with open(self.chunks_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return []
        return []
    
    def _load_embeddings(self) -> Optional[np.ndarray]:
        """Load embeddings from storage"""
        if self.embeddings_file.exists() and self.embedding_model is not None:
            try:
                return np.load(self.embeddings_file)
            except Exception:
                return None
        return None
    
    def _load_metadata(self) -> Dict:
        """Load metadata from storage"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}
    
    def _save_chunks(self):
        """Save chunks to storage"""
        with open(self.chunks_file, 'w', encoding='utf-8') as f:
            json.dump(self.chunks, f, ensure_ascii=False, indent=2)
    
    def _save_embeddings(self, embeddings: np.ndarray):
        """Save embeddings to storage"""
        if self.embedding_model is not None:
            np.save(self.embeddings_file, embeddings)
    
    def _save_metadata(self):
        """Save metadata to storage"""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
    
    def _chunk_markdown(self, content: str, source: str) -> List[Dict]:
        """
        Chunk markdown into semantic sections
        Tries to preserve logical structure (headers, sections)
        """
        chunks = []
        
        # Split by headers (##, ###, ####)
        sections = re.split(r'\n(#{2,4}\s+.+?)\n', content)
        
        current_section = ""
        current_title = "Introduction"
        
        for i, section in enumerate(sections):
            if section.strip().startswith('#'):
                # This is a header
                if current_section.strip():
                    # Save previous section
                    chunks.append({
                        'text': current_section.strip(),
                        'title': current_title,
                        'source': source,
                        'chunk_id': len(chunks)
                    })
                current_title = section.strip().lstrip('#').strip()
                current_section = ""
            else:
                current_section += "\n" + section
        
        # Add last section
        if current_section.strip():
            chunks.append({
                'text': current_section.strip(),
                'title': current_title,
                'source': source,
                'chunk_id': len(chunks)
            })
        
        # If no headers found, chunk by paragraphs
        if len(chunks) == 0:
            paragraphs = [p.strip() for p in content.split('\n\n') if p.strip()]
            for i, para in enumerate(paragraphs):
                if len(para) > 50:  # Only chunk substantial paragraphs
                    chunks.append({
                        'text': para,
                        'title': f"Section {i+1}",
                        'source': source,
                        'chunk_id': i
                    })
        
        return chunks
    
    def _compute_embeddings(self, texts: List[str]) -> np.ndarray:
        """Compute embeddings for texts"""
        if self.embedding_model is not None:
            return self.embedding_model.encode(texts, show_progress_bar=False)
        return None
    
    def _simple_keyword_search(self, query: str, chunks: List[Dict], top_k: int = 3) -> List[Dict]:
        """Fallback keyword-based search"""
        query_words = set(query.lower().split())
        
        scored_chunks = []
        for chunk in chunks:
            text_words = set(chunk['text'].lower().split())
            # Simple intersection score
            score = len(query_words & text_words) / max(len(query_words), 1)
            scored_chunks.append((score, chunk))
        
        # Sort by score and return top_k
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        return [chunk for score, chunk in scored_chunks[:top_k] if score > 0]
    
    def _semantic_search(self, query: str, top_k: int = 3) -> List[Dict]:
        """Semantic search using embeddings"""
        if self.embedding_model is None or self.embeddings is None:
            # Fallback to keyword search
            return self._simple_keyword_search(query, self.chunks, top_k)
        
        # Compute query embedding
        query_embedding = self.embedding_model.encode([query], show_progress_bar=False)[0]
        
        # Compute cosine similarity
        similarities = np.dot(self.embeddings, query_embedding) / (
            np.linalg.norm(self.embeddings, axis=1) * np.linalg.norm(query_embedding)
        )
        
        # Get top_k indices
        top_indices = np.argsort(similarities)[::-1][:top_k]
        
        # Return chunks with similarity scores
        results = []
        for idx in top_indices:
            if similarities[idx] > 0.3:  # Minimum similarity threshold
                chunk = self.chunks[idx].copy()
                chunk['similarity'] = float(similarities[idx])
                results.append(chunk)
        
        return results
    
    def upload_markdown(self, content: str, source_name: str = "strategy.md"):
        """Upload and process a markdown file"""
        # Chunk the content
        new_chunks = self._chunk_markdown(content, source_name)
        
        # Add to existing chunks
        start_idx = len(self.chunks)
        self.chunks.extend(new_chunks)
        
        # Compute embeddings for new chunks
        if self.embedding_model is not None:
            new_texts = [chunk['text'] for chunk in new_chunks]
            new_embeddings = self._compute_embeddings(new_texts)
            
            if self.embeddings is not None:
                self.embeddings = np.vstack([self.embeddings, new_embeddings])
            else:
                self.embeddings = new_embeddings
            
            self._save_embeddings(self.embeddings)
        
        # Update metadata
        self.metadata[source_name] = {
            'chunk_count': len(new_chunks),
            'start_idx': start_idx,
            'end_idx': len(self.chunks) - 1,
            'uploaded_at': pd.Timestamp.now().isoformat()
        }
        
        # Save all data
        self._save_chunks()
        self._save_metadata()
        
        return len(new_chunks)
    
    def search(self, query: str, top_k: int = 3) -> List[Dict]:
        """Search for relevant chunks"""
        if not self.chunks:
            return []
        
        return self._semantic_search(query, top_k)
    
    def get_relevant_context(self, query: str, max_chunks: int = 3, max_chars: int = 2000) -> str:
        """
        Get relevant context chunks formatted for AI prompt
        Token-efficient: only includes relevant sections
        """
        results = self.search(query, top_k=max_chunks)
        
        if not results:
            return ""
        
        context_parts = ["## STRATEGY INSTRUCTIONS (Relevant Sections):"]
        
        total_chars = 0
        for chunk in results:
            chunk_text = f"\n### {chunk['title']}\n{chunk['text']}\n"
            if total_chars + len(chunk_text) > max_chars:
                break
            context_parts.append(chunk_text)
            total_chars += len(chunk_text)
        
        return "\n".join(context_parts)
    
    def get_all_context(self) -> str:
        """Get all chunks (for full context when needed)"""
        if not self.chunks:
            return ""
        
        context_parts = ["## STRATEGY INSTRUCTIONS:"]
        for chunk in self.chunks:
            context_parts.append(f"\n### {chunk['title']}\n{chunk['text']}\n")
        
        return "\n".join(context_parts)
    
    def clear_all(self):
        """Clear all stored instructions"""
        self.chunks = []
        self.embeddings = None
        self.metadata = {}
        
        # Delete files
        if self.chunks_file.exists():
            self.chunks_file.unlink()
        if self.embeddings_file.exists():
            self.embeddings_file.unlink()
        if self.metadata_file.exists():
            self.metadata_file.unlink()
    
    def get_stats(self) -> Dict:
        """Get statistics about stored instructions"""
        return {
            'total_chunks': len(self.chunks),
            'total_sources': len(self.metadata),
            'has_embeddings': self.embeddings is not None,
            'sources': list(self.metadata.keys())
        }


# Singleton instance
_strategy_instructions_instance = None

def get_strategy_instructions() -> StrategyInstructions:
    """Get singleton instance of StrategyInstructions"""
    global _strategy_instructions_instance
    if _strategy_instructions_instance is None:
        _strategy_instructions_instance = StrategyInstructions()
    return _strategy_instructions_instance
