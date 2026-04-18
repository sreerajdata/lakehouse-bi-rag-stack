"""
Enterprise Data Lakehouse - AI Chatbot (RAG + Llama 3 via Ollama)
Streamlit UI | LangChain | Milvus vector store | Trino SQL
"""

import os
import streamlit as st
from langchain_community.llms import Ollama
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.vectorstores import Milvus
from langchain.chains import RetrievalQA, ConversationalRetrievalChain
from langchain.memory import ConversationBufferWindowMemory
from langchain.prompts import PromptTemplate
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain.agents.agent_types import AgentType
import pandas as pd

# Config
OLLAMA_URL   = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
MILVUS_HOST  = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT  = int(os.getenv("MILVUS_PORT", 19530))
TRINO_HOST   = os.getenv("TRINO_HOST", "trino")
TRINO_PORT   = int(os.getenv("TRINO_PORT", 8080))

TRINO_URI    = f"trino://admin@{TRINO_HOST}:{TRINO_PORT}/iceberg"

# Streamlit Page Setup
st.set_page_config(
    page_title="Enterprise Lakehouse AI Assistant",
    page_icon="🏭",
    layout="wide"
)

st.title("🏭 Enterprise Data Lakehouse AI Assistant")
st.caption("Powered by Llama 3 (on-prem) | RAG | Milvus | Trino")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuration")
    mode = st.radio(
        "Assistant Mode",
        ["📊 SQL Analytics", "📄 Document RAG", "🔄 Hybrid"],
        index=2
    )
    llm_model = st.selectbox("LLM Model", ["llama3", "llama3:8b", "mistral"])
    temperature = st.slider("Temperature", 0.0, 1.0, 0.1, 0.05)
    top_k_docs  = st.slider("Top-K Documents (RAG)", 1, 10, 4)
    st.divider()
    st.markdown("**Data Layers**")
    st.success("🟢 Bronze: Raw ingestion")
    st.info("🔵 Silver: Cleaned & enriched")
    st.warning("🟡 Gold: Domain marts")


# LLM Initialization
@st.cache_resource
def get_llm(model: str, temp: float):
    return Ollama(base_url=OLLAMA_URL, model=model, temperature=temp)

@st.cache_resource
def get_embeddings():
    return OllamaEmbeddings(base_url=OLLAMA_URL, model="llama3")

@st.cache_resource
def get_vector_store():
    embeddings = get_embeddings()
    return Milvus(
        embedding_function=embeddings,
        collection_name="enterprise_documents",
        connection_args={"host": MILVUS_HOST, "port": MILVUS_PORT},
    )

@st.cache_resource
def get_sql_agent(_llm):
    db = SQLDatabase.from_uri(
        TRINO_URI,
        include_tables=["gold_manufacturing_oee_mart", "gold_compliance_capa_mart"],
        sample_rows_in_table_info=3
    )
    return create_sql_agent(
        llm=_llm,
        db=db,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        handle_parsing_errors=True,
    )


# RAG Prompt Template
RAG_PROMPT = PromptTemplate(
    input_variables=["context", "question"],
    template="""You are an expert data analyst for the manufacturing enterprise.
You have access to documents, SOPs, batch records, and manufacturing data.
Answer questions accurately based on the provided context.
If you cannot find the answer in the context, say so clearly.
Always cite your sources when possible.

Context:
{context}

Question: {question}

Answer:"""
)


# Chat Interface
if "messages" not in st.session_state:
    st.session_state.messages = []
if "memory" not in st.session_state:
    st.session_state.memory = ConversationBufferWindowMemory(
        memory_key="chat_history", return_messages=True, k=5
    )

# Display chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Sample queries
with st.expander("💡 Example Queries"):
    cols = st.columns(2)
    examples = [
        "What is the OEE for machine MCH-001 this week?",
        "Show me all open CAPAs older than 30 days",
        "Which products had the most quality test failures last month?",
        "What does 21 CFR Part 11 require for audit trails?",
        "Summarize the batch record for batch BATCH-123456",
        "What was the average yield percentage for shift A last week?",
    ]
    for i, ex in enumerate(examples):
        if cols[i % 2].button(ex, key=f"ex_{i}"):
            st.session_state.pending_query = ex

# Chat input
query = st.chat_input("Ask about manufacturing data, SOPs, or batch records...")
if "pending_query" in st.session_state:
    query = st.session_state.pop("pending_query")

if query:
    st.session_state.messages.append({"role": "user", "content": query})
    with st.chat_message("user"):
        st.markdown(query)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            llm = get_llm(llm_model, temperature)
            response_text = ""

            try:
                if mode == "📊 SQL Analytics":
                    agent = get_sql_agent(llm)
                    result = agent.run(query)
                    response_text = result

                elif mode == "📄 Document RAG":
                    vector_store = get_vector_store()
                    qa_chain = RetrievalQA.from_chain_type(
                        llm=llm,
                        chain_type="stuff",
                        retriever=vector_store.as_retriever(
                            search_kwargs={"k": top_k_docs}
                        ),
                        chain_type_kwargs={"prompt": RAG_PROMPT},
                        return_source_documents=True,
                    )
                    result = qa_chain({"query": query})
                    response_text = result["result"]
                    if result.get("source_documents"):
                        response_text += "\n\n**Sources:**\n"
                        for doc in result["source_documents"]:
                            src = doc.metadata.get("source", "Unknown")
                            response_text += f"- {src}\n"

                else:  # Hybrid
                    # Try SQL first for data questions, fall back to RAG
                    if any(kw in query.lower() for kw in
                           ["oee", "batch", "machine", "production", "yield",
                            "quality", "capa", "inventory", "shift", "plant"]):
                        try:
                            agent = get_sql_agent(llm)
                            response_text = agent.run(query)
                        except Exception:
                            vector_store = get_vector_store()
                            qa_chain = RetrievalQA.from_chain_type(
                                llm=llm, retriever=vector_store.as_retriever()
                            )
                            response_text = qa_chain.run(query)
                    else:
                        vector_store = get_vector_store()
                        qa_chain = RetrievalQA.from_chain_type(
                            llm=llm, retriever=vector_store.as_retriever(
                                search_kwargs={"k": top_k_docs}
                            ),
                            chain_type_kwargs={"prompt": RAG_PROMPT},
                        )
                        response_text = qa_chain.run(query)

            except Exception as e:
                response_text = f"⚠️ Error: {str(e)}\n\nPlease ensure Ollama and Milvus are running."

            st.markdown(response_text)
            st.session_state.messages.append(
                {"role": "assistant", "content": response_text}
            )
