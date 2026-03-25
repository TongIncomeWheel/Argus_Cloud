"""
Strategy Instructions UI Module
Provides UI for uploading and managing strategy markdown files
"""

import streamlit as st
from strategy_instructions import get_strategy_instructions
from pathlib import Path


def render_strategy_instructions():
    """Render the Strategy Instructions page"""
    
    st.header("📚 Strategy Instructions")
    st.markdown("Upload markdown files containing your trading strategy, risk parameters, and execution rules. "
                "The AI assistant will automatically reference relevant sections when answering questions.")
    
    strategy_instructions = get_strategy_instructions()
    
    # Display current stats
    stats = strategy_instructions.get_stats()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Chunks", stats['total_chunks'])
    with col2:
        st.metric("Uploaded Files", stats['total_sources'])
    with col3:
        st.metric("Embeddings", "✅ Active" if stats['has_embeddings'] else "⚠️ Keyword Only")
    
    if stats['sources']:
        st.info(f"📄 Current files: {', '.join(stats['sources'])}")
    
    st.divider()
    
    # Upload section
    st.subheader("📤 Upload Strategy Document")
    
    uploaded_file = st.file_uploader(
        "Choose a markdown file",
        type=['md', 'txt'],
        help="Upload a markdown file containing your trading strategy, risk parameters, or execution rules"
    )
    
    if uploaded_file is not None:
        # Read file content
        content = uploaded_file.read().decode('utf-8')
        
        # Show preview
        with st.expander("📄 Preview Upload", expanded=True):
            st.text_area("File Content", content, height=300, disabled=True)
        
        # Upload button
        if st.button("✅ Upload & Process", type="primary", use_container_width=True):
            try:
                with st.spinner("Processing markdown file..."):
                    chunk_count = strategy_instructions.upload_markdown(
                        content=content,
                        source_name=uploaded_file.name
                    )
                
                st.success(f"✅ Successfully uploaded! Processed {chunk_count} sections.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error uploading file: {str(e)}")
    
    st.divider()
    
    # View stored chunks
    st.subheader("📋 Stored Instructions")
    
    chunks = strategy_instructions.chunks
    if not chunks:
        st.info("No strategy instructions uploaded yet. Upload a markdown file above to get started.")
    else:
        # Group by source
        sources = {}
        for chunk in chunks:
            source = chunk.get('source', 'Unknown')
            if source not in sources:
                sources[source] = []
            sources[source].append(chunk)
        
        for source, source_chunks in sources.items():
            with st.expander(f"📄 {source} ({len(source_chunks)} sections)", expanded=False):
                for chunk in source_chunks:
                    st.markdown(f"### {chunk['title']}")
                    st.text(chunk['text'][:500] + ("..." if len(chunk['text']) > 500 else ""))
                    st.caption(f"Chunk ID: {chunk['chunk_id']}")
                    st.divider()
    
    st.divider()
    
    # Test search
    st.subheader("🔍 Test Semantic Search")
    test_query = st.text_input(
        "Enter a test query to see which sections would be retrieved:",
        placeholder="e.g., 'risk management rules' or 'position sizing'"
    )
    
    if test_query:
        results = strategy_instructions.search(test_query, top_k=3)
        if results:
            st.success(f"Found {len(results)} relevant sections:")
            for i, result in enumerate(results, 1):
                with st.expander(f"Result {i}: {result['title']} (Similarity: {result.get('similarity', 'N/A'):.2f})"):
                    st.markdown(f"**Source:** {result['source']}")
                    st.text(result['text'])
        else:
            st.warning("No relevant sections found for this query.")
    
    st.divider()
    
    # Management section
    st.subheader("⚙️ Management")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🗑️ Clear All Instructions", use_container_width=True):
            if st.session_state.get('confirm_clear', False):
                strategy_instructions.clear_all()
                st.success("✅ All instructions cleared.")
                st.session_state.confirm_clear = False
                st.rerun()
            else:
                st.session_state.confirm_clear = True
                st.warning("⚠️ Click again to confirm deletion of all strategy instructions.")
    
    with col2:
        if st.button("🔄 Refresh View", use_container_width=True):
            st.rerun()
    
    # Instructions
    st.divider()
    st.subheader("💡 How It Works")
    
    st.markdown("""
    **Token-Efficient Strategy Reference:**
    
    1. **Upload**: Upload your strategy markdown file (risk rules, execution parameters, etc.)
    2. **Processing**: The system automatically chunks your document into semantic sections
    3. **Embeddings**: Each section is converted to embeddings for fast semantic search
    4. **AI Integration**: When you ask the AI assistant a question, it automatically retrieves only the most relevant sections (typically 2-3 chunks, ~2000 chars)
    5. **Context Injection**: Relevant sections are included in the AI's context, making responses more accurate and aligned with your strategy
    
    **Benefits:**
    - ✅ Token-efficient: Only relevant sections are included (not the entire document)
    - ✅ Fast: Semantic search finds relevant sections instantly
    - ✅ Accurate: AI responses are aligned with your specific strategy and risk parameters
    - ✅ Persistent: Instructions are stored locally and persist across sessions
    
    **Tips:**
    - Use clear section headers (##, ###) in your markdown for better chunking
    - Include keywords related to your strategy in section titles
    - Update your strategy document as your rules evolve
    """)
