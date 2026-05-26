"""
Enterprise Data Lakehouse — Document QA Chain
Standalone QA chain: question → Milvus retrieval → Llama 3 synthesis → answer with citations.

Returns:
    {answer: str, sources: list[str], confidence: float}
"""

import os
import logging
from typing import Dict, Any, Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("document-qa")

OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))

QA_PROMPT_TEMPLATE = """You are an expert pharmaceutical data analyst for the manufacturing enterprise.
Answer the question based ONLY on the provided context from internal documents.
If you cannot find the answer in the context, clearly state that the information is not available.
Always cite the source documents when possible.
Rate your confidence in the answer from 0.0 to 1.0.

Context:
{context}

Question: {question}

Provide your answer in the following format:
ANSWER: [Your detailed answer here]
CONFIDENCE: [0.0 to 1.0]
SOURCES: [List of source documents used]"""


def answer_question(
    question: str,
    source_filter: Optional[str] = None,
    top_k: int = 4,
    collection: str = "enterprise_documents",
) -> Dict[str, Any]:
    """
    Answer a question using RAG (Retrieval Augmented Generation).

    Args:
        question: The user's question
        source_filter: Optional filter by source filename
        top_k: Number of document chunks to retrieve
        collection: Milvus collection name

    Returns:
        Dict with keys: answer, sources, confidence
    """
    from langchain_community.llms import Ollama
    from langchain_community.embeddings import OllamaEmbeddings
    from langchain_community.vectorstores import Milvus

    llm = Ollama(base_url=OLLAMA_URL, model="llama3", temperature=0.1)
    embeddings = OllamaEmbeddings(base_url=OLLAMA_URL, model=EMBEDDING_MODEL)

    try:
        vector_store = Milvus(
            embedding_function=embeddings,
            collection_name=collection,
            connection_args={"host": MILVUS_HOST, "port": MILVUS_PORT},
        )
    except Exception as e:
        logger.error(f"Failed to connect to Milvus: {e}")
        return {
            "answer": f"Unable to connect to document store: {str(e)}",
            "sources": [],
            "confidence": 0.0,
        }

    search_kwargs = {"k": top_k}

    try:
        if source_filter:
            docs = vector_store.similarity_search(
                question,
                k=top_k,
                expr=f'filename == "{source_filter}"',
            )
        else:
            docs = vector_store.similarity_search(question, **search_kwargs)
    except Exception as e:
        logger.error(f"Milvus search failed: {e}")
        return {
            "answer": f"Document search failed: {str(e)}",
            "sources": [],
            "confidence": 0.0,
        }

    if not docs:
        return {
            "answer": "No relevant documents found for this question.",
            "sources": [],
            "confidence": 0.0,
        }

    context_parts = []
    sources = set()
    for doc in docs:
        context_parts.append(doc.page_content)
        source = doc.metadata.get("filename", doc.metadata.get("source", "Unknown"))
        sources.add(source)

    context = "\n\n---\n\n".join(context_parts)

    prompt = QA_PROMPT_TEMPLATE.format(context=context, question=question)

    try:
        response = llm.invoke(prompt)
    except Exception as e:
        logger.error(f"LLM generation failed: {e}")
        return {
            "answer": f"LLM generation failed: {str(e)}",
            "sources": list(sources),
            "confidence": 0.0,
        }

    answer = response
    confidence = 0.5

    if "ANSWER:" in response:
        try:
            answer_part = response.split("ANSWER:")[1]
            if "CONFIDENCE:" in answer_part:
                answer = answer_part.split("CONFIDENCE:")[0].strip()
                conf_part = answer_part.split("CONFIDENCE:")[1]
                if "SOURCES:" in conf_part:
                    conf_str = conf_part.split("SOURCES:")[0].strip()
                else:
                    conf_str = conf_part.strip()
                try:
                    confidence = float(conf_str)
                    confidence = max(0.0, min(1.0, confidence))
                except ValueError:
                    confidence = 0.5
            else:
                answer = answer_part.strip()
        except (IndexError, ValueError):
            pass

    return {
        "answer": answer,
        "sources": sorted(list(sources)),
        "confidence": round(confidence, 2),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Document QA — ask questions about enterprise documents")
    parser.add_argument("question", help="Question to answer")
    parser.add_argument("--source", default=None, help="Filter by source document filename")
    parser.add_argument("--top-k", type=int, default=4, help="Number of chunks to retrieve")
    args = parser.parse_args()

    result = answer_question(args.question, source_filter=args.source, top_k=args.top_k)
    print(f"\n{'='*60}")
    print(f"Question: {args.question}")
    print(f"{'='*60}")
    print(f"Answer: {result['answer']}")
    print(f"Confidence: {result['confidence']}")
    print(f"Sources: {', '.join(result['sources']) if result['sources'] else 'None'}")
