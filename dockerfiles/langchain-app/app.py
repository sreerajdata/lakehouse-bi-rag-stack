"""
Enterprise Data Lakehouse AI Assistant
Streamlit UI | LangChain | Milvus vector store | Trino SQL
"""

import os
from html import escape
from pathlib import Path

import boto3
import httpx
import streamlit as st
from botocore.config import Config as BotoConfig
from langchain.agents.agent_types import AgentType
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.llms import Ollama
from langchain_community.utilities import SQLDatabase
from langchain_community.vectorstores import Milvus
from pymilvus import Collection, connections, utility

from utils.index_documents import index_documents

OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
CHAT_MODEL = os.getenv("CHAT_MODEL", "llama3")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", 19530))
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", 8080))
DOCS_BUCKET = os.getenv("DOCS_BUCKET", "lakehouse-docs")
SEAWEEDFS_ENDPOINT = os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333")
SEAWEEDFS_ACCESS_KEY = os.getenv("SEAWEEDFS_ACCESS_KEY", "admin")
SEAWEEDFS_SECRET_KEY = os.getenv("SEAWEEDFS_SECRET_KEY", "admin123")
TIKA_URL = os.getenv("TIKA_URL", "http://tika:9998")
COLLECTION_NAME = os.getenv("MILVUS_COLLECTION", "enterprise_documents")
SEED_FILES = [
    Path("/workspace/data/source/mes_events.csv"),
    Path("/workspace/data/source/trackwise_deviations.csv"),
    Path("/workspace/data/source/sop_documents.csv"),
    Path("/workspace/data/source/sap_ecc_orders.csv"),
]

TRINO_URI = f"trino://admin@{TRINO_HOST}:{TRINO_PORT}/iceberg"

st.set_page_config(
    page_title="Enterprise Lakehouse AI Assistant",
    page_icon="AI",
    layout="wide",
)

st.markdown(
    """
    <style>
        :root {
            --app-bg: #f6f8fb;
            --panel: #ffffff;
            --panel-soft: #eef4f8;
            --ink: #172033;
            --muted: #617086;
            --line: #d9e2ec;
            --brand: #0f766e;
            --brand-strong: #115e59;
            --accent: #2563eb;
            --warn: #b7791f;
        }

        .stApp {
            background: var(--app-bg);
            color: var(--ink);
        }

        .block-container {
            max-width: 1280px;
            padding-top: 1.5rem;
            padding-bottom: 5rem;
        }

        [data-testid="stSidebar"] {
            background: #0f172a;
            border-right: 1px solid #1e293b;
        }

        [data-testid="stSidebar"] * {
            color: #e5edf8;
        }

        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] .stCaptionContainer {
            color: #c6d3e1;
        }

        .hero {
            border: 1px solid var(--line);
            background: linear-gradient(135deg, #ffffff 0%, #eef7f5 54%, #edf4ff 100%);
            border-radius: 8px;
            padding: 1.25rem 1.4rem;
            margin-bottom: 1rem;
        }

        .eyebrow {
            color: var(--brand-strong);
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .hero h1 {
            color: var(--ink);
            font-size: 2rem;
            line-height: 1.15;
            letter-spacing: 0;
            margin: 0;
        }

        .hero p {
            color: var(--muted);
            font-size: 0.98rem;
            margin: 0.55rem 0 0;
            max-width: 820px;
        }

        .status-card {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 0.85rem 0.95rem;
            min-height: 92px;
        }

        .status-label {
            color: var(--muted);
            font-size: 0.78rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
            text-transform: uppercase;
        }

        .status-value {
            color: var(--ink);
            font-size: 1.45rem;
            font-weight: 750;
            line-height: 1.15;
        }

        .status-note {
            color: var(--muted);
            font-size: 0.82rem;
            margin-top: 0.35rem;
        }

        .section-title {
            color: var(--ink);
            font-size: 1rem;
            font-weight: 750;
            margin: 1.2rem 0 0.4rem;
        }

        .source-block {
            border-left: 3px solid var(--brand);
            background: #f8fbfd;
            border-radius: 0 8px 8px 0;
            padding: 0.65rem 0.8rem;
            margin: 0.5rem 0;
        }

        .source-meta {
            color: var(--brand-strong);
            font-size: 0.78rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }

        .source-excerpt {
            color: #334155;
            font-size: 0.88rem;
            margin: 0;
        }

        div.stButton > button {
            border-radius: 8px;
            border: 1px solid var(--line);
            background: #ffffff;
            color: var(--ink);
            min-height: 2.5rem;
        }

        div.stButton > button:hover {
            border-color: var(--brand);
            color: var(--brand-strong);
        }

        .stChatMessage {
            border-radius: 8px;
            border: 1px solid #e4eaf1;
            background: #ffffff;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def normalize_excerpt(text: str, limit: int = 280) -> str:
    compact = " ".join((text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[:limit].rstrip() + "..."


def build_source_entries(source_documents):
    entries = []
    seen = set()

    for index, doc in enumerate(source_documents, start=1):
        metadata = doc.metadata or {}
        source_name = metadata.get("filename") or metadata.get("source_key") or metadata.get("source") or "Unknown"
        unique_key = (
            source_name,
            metadata.get("chunk_index"),
            normalize_excerpt(doc.page_content, 180),
        )
        if unique_key in seen:
            continue
        seen.add(unique_key)

        entries.append(
            {
                "ref": index,
                "source": source_name,
                "excerpt": normalize_excerpt(doc.page_content),
                "chunk_index": metadata.get("chunk_index"),
                "extraction_method": metadata.get("extraction_method"),
            }
        )
    return entries


def format_answer_with_citations(answer_text: str, source_entries):
    if not source_entries:
        return answer_text
    markers = " ".join(f"[{entry['ref']}]" for entry in source_entries[:3])
    return f"{answer_text}\n\n{markers}"


def render_sources(source_entries):
    if not source_entries:
        return
    st.markdown('<div class="section-title">Sources</div>', unsafe_allow_html=True)
    for entry in source_entries:
        chunk_suffix = ""
        if entry["chunk_index"] is not None:
            chunk_suffix = f" | chunk {entry['chunk_index']}"
        method_suffix = ""
        if entry["extraction_method"]:
            method_suffix = f" | {entry['extraction_method']}"
        source_meta = escape(f"[{entry['ref']}] {entry['source']}{chunk_suffix}{method_suffix}")
        excerpt = escape(entry["excerpt"])
        st.markdown(
            f"""
            <div class="source-block">
                <div class="source-meta">{source_meta}</div>
                <p class="source-excerpt">{excerpt}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_message(message):
    st.markdown(message["content"])
    if message.get("sources"):
        render_sources(message["sources"])


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=SEAWEEDFS_ENDPOINT,
        aws_access_key_id=SEAWEEDFS_ACCESS_KEY,
        aws_secret_access_key=SEAWEEDFS_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists():
    s3 = get_s3_client()
    buckets = {bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])}
    if DOCS_BUCKET not in buckets:
        s3.create_bucket(Bucket=DOCS_BUCKET)


def list_bucket_objects():
    ensure_bucket_exists()
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=DOCS_BUCKET)
    return response.get("Contents", [])


def upload_object(key: str, body: bytes, content_type: str = "application/octet-stream"):
    ensure_bucket_exists()
    s3 = get_s3_client()
    s3.put_object(Bucket=DOCS_BUCKET, Key=key, Body=body, ContentType=content_type)


def ensure_ollama_model(model_name: str):
    with httpx.Client(timeout=300.0) as client:
        tags = client.get(f"{OLLAMA_URL}/api/tags").json().get("models", [])
        installed = {item["name"] for item in tags}
        if model_name in installed:
            return False
        response = client.post(f"{OLLAMA_URL}/api/pull", json={"name": model_name})
        response.raise_for_status()
        return True


def get_installed_models():
    try:
        with httpx.Client(timeout=20.0) as client:
            tags = client.get(f"{OLLAMA_URL}/api/tags")
            tags.raise_for_status()
            return sorted(item["name"] for item in tags.json().get("models", []))
    except Exception:
        return []


def get_collection_count():
    try:
        connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)
        if COLLECTION_NAME not in utility.list_collections():
            return 0
        collection = Collection(COLLECTION_NAME)
        collection.load()
        return collection.num_entities
    except Exception:
        return 0


@st.cache_resource
def get_llm(model: str, temp: float):
    return Ollama(base_url=OLLAMA_URL, model=model, temperature=temp)


@st.cache_resource
def get_embeddings(model_name: str):
    return OllamaEmbeddings(base_url=OLLAMA_URL, model=model_name)


@st.cache_resource
def get_vector_store(embedding_model: str):
    embeddings = get_embeddings(embedding_model)
    return Milvus(
        embedding_function=embeddings,
        collection_name=COLLECTION_NAME,
        connection_args={"host": MILVUS_HOST, "port": MILVUS_PORT},
    )


@st.cache_resource
def get_sql_agent(_llm):
    db = SQLDatabase.from_uri(
        TRINO_URI,
        include_tables=["gold_manufacturing_oee_mart", "gold_compliance_capa_mart"],
        sample_rows_in_table_info=3,
    )
    return create_sql_agent(
        llm=_llm,
        db=db,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        handle_parsing_errors=True,
    )


def seed_demo_corpus():
    uploaded = []
    for path in SEED_FILES:
        if not path.exists():
            continue
        upload_object(path.name, path.read_bytes(), "text/plain; charset=utf-8")
        uploaded.append(path.name)
    return uploaded


def render_status_card(label: str, value, note: str):
    st.markdown(
        f"""
        <div class="status-card">
            <div class="status-label">{label}</div>
            <div class="status-value">{value}</div>
            <div class="status-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def yes_no(value: bool) -> str:
    return "Ready" if value else "Needs setup"


RAG_PROMPT = PromptTemplate(
    input_variables=["context", "question"],
    template="""You are an expert manufacturing and compliance analyst.
Answer using only the supplied context.
If the context does not contain the answer, say that clearly.
When possible, mention the source documents you used.

Context:
{context}

Question: {question}

Answer:""",
)

installed_models = get_installed_models()
bucket_objects = list_bucket_objects()
entity_count = get_collection_count()
chat_ready = CHAT_MODEL in installed_models
embedding_ready = EMBEDDING_MODEL in installed_models
selected_mode = st.session_state.get("assistant_mode", "Hybrid")

st.markdown(
    """
    <div class="hero">
        <div class="eyebrow">Manufacturing intelligence workspace</div>
        <h1>Enterprise Data Lakehouse AI Assistant</h1>
        <p>Ask operational questions across Trino analytics and indexed SOP, deviation, and batch documents backed by Milvus retrieval.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

metric_cols = st.columns(4)
with metric_cols[0]:
    render_status_card("Assistant mode", selected_mode, "Routes data and document questions")
with metric_cols[1]:
    render_status_card("Indexed chunks", entity_count, COLLECTION_NAME)
with metric_cols[2]:
    render_status_card("Bucket objects", len(bucket_objects), DOCS_BUCKET)
with metric_cols[3]:
    render_status_card("Models", yes_no(chat_ready and embedding_ready), f"{CHAT_MODEL} / {EMBEDDING_MODEL}")

with st.sidebar:
    st.header("Assistant Setup")
    mode = st.radio(
        "Assistant Mode",
        ["SQL Analytics", "Document RAG", "Hybrid"],
        index=2,
        key="assistant_mode",
    )
    llm_model = st.selectbox("LLM Model", [CHAT_MODEL, "llama3:8b", "mistral"], index=0)
    temperature = st.slider("Temperature", 0.0, 1.0, 0.1, 0.05)
    top_k_docs = st.slider("Top-K Documents (RAG)", 1, 10, 4)

    st.divider()
    st.subheader("System Status")
    st.write(f"Chat model: {yes_no(llm_model in installed_models)}")
    st.write(f"Embedding model: {yes_no(embedding_ready)}")
    st.write(f"Bucket objects: {len(bucket_objects)}")
    st.write(f"Indexed chunks: {entity_count}")

    if st.button(f"Pull embedding model: {EMBEDDING_MODEL}"):
        pulled = ensure_ollama_model(EMBEDDING_MODEL)
        st.success("Embedding model ready." if pulled else "Embedding model already installed.")
        st.cache_resource.clear()
        st.rerun()

    if st.button("Seed demo corpus"):
        uploaded = seed_demo_corpus()
        if uploaded:
            st.success(f"Uploaded {len(uploaded)} demo files to {DOCS_BUCKET}.")
        else:
            st.warning("No local seed files were available.")
        st.rerun()

    if st.button("Index bucket into Milvus"):
        ensure_ollama_model(EMBEDDING_MODEL)
        index_documents(bucket=DOCS_BUCKET, collection=COLLECTION_NAME, tika_url=TIKA_URL)
        st.success("Document indexing completed.")
        st.cache_resource.clear()
        st.rerun()

    uploaded_files = st.file_uploader(
        "Upload documents for RAG",
        type=["pdf", "txt", "md", "csv", "json"],
        accept_multiple_files=True,
    )
    if st.button("Store uploaded documents", disabled=not uploaded_files):
        for uploaded_file in uploaded_files or []:
            upload_object(uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type or "application/octet-stream")
        st.success(f"Stored {len(uploaded_files or [])} files in {DOCS_BUCKET}.")
        st.rerun()

    st.divider()
    st.markdown("**Lakehouse Layers**")
    st.success("Bronze | Raw ingestion")
    st.info("Silver | Cleaned and enriched")
    st.warning("Gold | Domain marts")


if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        render_message(msg)

st.markdown('<div class="section-title">Example Queries</div>', unsafe_allow_html=True)
cols = st.columns(3)
examples = [
    "What is the OEE for machine MCH-003 this week?",
    "Show open and in-review deviations by product code.",
    "Which batches had temperature excursion deviations this month?",
    "Summarize production issues for Paracetamol 500mg batches.",
    "Which machines recorded WARNING status events most often?",
    "Compare deviation counts across Amoxicillin, Metformin, and Omeprazole products.",
]
for i, example in enumerate(examples):
    if cols[i % 3].button(example, key=f"ex_{i}", use_container_width=True):
        st.session_state.pending_query = example

query = st.chat_input("Ask about documents, SOP metadata, or manufacturing data...")
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
            source_entries = []

            try:
                if mode == "SQL Analytics":
                    agent = get_sql_agent(llm)
                    response_text = agent.run(query)

                elif mode == "Document RAG":
                    if get_collection_count() == 0:
                        response_text = (
                            "No document embeddings are loaded yet. "
                            "Use the sidebar to seed or upload documents, then run 'Index bucket into Milvus'."
                        )
                    else:
                        vector_store = get_vector_store(EMBEDDING_MODEL)
                        qa_chain = RetrievalQA.from_chain_type(
                            llm=llm,
                            chain_type="stuff",
                            retriever=vector_store.as_retriever(search_kwargs={"k": top_k_docs}),
                            chain_type_kwargs={"prompt": RAG_PROMPT},
                            return_source_documents=True,
                        )
                        result = qa_chain({"query": query})
                        source_entries = build_source_entries(result.get("source_documents") or [])
                        response_text = format_answer_with_citations(result["result"], source_entries)

                else:
                    if any(
                        keyword in query.lower()
                        for keyword in ["oee", "batch", "machine", "production", "yield", "quality", "capa", "inventory", "shift", "plant"]
                    ):
                        try:
                            agent = get_sql_agent(llm)
                            response_text = agent.run(query)
                        except Exception:
                            if get_collection_count() == 0:
                                raise
                            vector_store = get_vector_store(EMBEDDING_MODEL)
                            qa_chain = RetrievalQA.from_chain_type(
                                llm=llm,
                                chain_type="stuff",
                                retriever=vector_store.as_retriever(search_kwargs={"k": top_k_docs}),
                                chain_type_kwargs={"prompt": RAG_PROMPT},
                                return_source_documents=True,
                            )
                            result = qa_chain({"query": query})
                            source_entries = build_source_entries(result.get("source_documents") or [])
                            response_text = format_answer_with_citations(result["result"], source_entries)
                    else:
                        if get_collection_count() == 0:
                            response_text = (
                                "No document embeddings are loaded yet. "
                                "Use the sidebar to seed or upload documents, then run 'Index bucket into Milvus'."
                            )
                        else:
                            vector_store = get_vector_store(EMBEDDING_MODEL)
                            qa_chain = RetrievalQA.from_chain_type(
                                llm=llm,
                                chain_type="stuff",
                                retriever=vector_store.as_retriever(search_kwargs={"k": top_k_docs}),
                                chain_type_kwargs={"prompt": RAG_PROMPT},
                                return_source_documents=True,
                            )
                            result = qa_chain({"query": query})
                            source_entries = build_source_entries(result.get("source_documents") or [])
                            response_text = format_answer_with_citations(result["result"], source_entries)

            except Exception as e:
                response_text = f"Error: {str(e)}"

            st.markdown(response_text)
            render_sources(source_entries)
            st.session_state.messages.append({"role": "assistant", "content": response_text})
            if source_entries:
                st.session_state.messages[-1]["sources"] = source_entries
