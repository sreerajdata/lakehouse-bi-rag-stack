"""
Trino SQL Tool for LangChain
Custom LangChain tool that converts natural language questions to SQL,
executes against the gold layer via Trino, and returns formatted results.

Safety: Only SELECT statements allowed — no DDL/DML.
"""

import os
import re
import logging
from typing import Optional

from langchain.tools import BaseTool
from langchain_community.llms import Ollama

logger = logging.getLogger("trino-tool")

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")

GOLD_TABLES = {
    "gold_manufacturing_oee_mart": "OEE scores, availability, performance, quality rate per machine/shift/day",
    "gold_compliance_capa_mart": "CAPA closure rates, overdue CAPAs, compliance RAG status by department/month",
    "gold_sap_inventory_mart": "Stock levels, receipts, issues, scrap, turnover ratio by material/plant/month",
    "gold_quality_risk_mart": "Batch risk scores, pass rates, deviation counts, release status by product/batch",
    "gold_training_compliance_mart": "Training completion rates, GMP compliance scores, overdue trainings by department/month",
    "gold_supply_chain_mart": "Vendor scores, delivery rates, rejection rates, lead times by vendor/material/month",
}

SQL_GENERATION_PROMPT = """You are a SQL expert for a pharmaceutical manufacturing data lakehouse.
Generate a Trino SQL query to answer the user's question.

Available tables in the 'iceberg.gold' schema:
{table_descriptions}

Rules:
- Use ONLY SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE.
- Always qualify table names with schema: iceberg.gold.<table_name>
- Use LIMIT 100 unless the user asks for all results
- For date filtering, use CURRENT_DATE and DATE_ADD/DATE_DIFF functions
- Round decimal values to 2 decimal places
- Return ONLY the SQL query, no explanations

Question: {question}

SQL:"""


class TrinoQueryTool(BaseTool):
    """LangChain tool for querying Trino gold layer tables via natural language."""

    name: str = "trino_query"
    description: str = (
        "Query the Data Lakehouse gold layer tables using natural language. "
        "Useful for questions about OEE, inventory, quality, CAPAs, training, "
        "and vendor performance. Returns formatted query results."
    )

    def _validate_sql(self, sql: str) -> bool:
        """Ensure only SELECT statements are allowed — no DDL/DML."""
        sql_upper = sql.strip().upper()
        forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
                      "TRUNCATE", "GRANT", "REVOKE", "MERGE"]
        for keyword in forbidden:
            if re.search(rf'\b{keyword}\b', sql_upper):
                logger.warning(f"Blocked forbidden SQL keyword: {keyword}")
                return False
        if not sql_upper.startswith("SELECT"):
            return False
        return True

    def _generate_sql(self, question: str) -> str:
        """Use LLM to convert natural language to SQL."""
        table_desc = "\n".join(
            [f"  - {name}: {desc}" for name, desc in GOLD_TABLES.items()]
        )
        prompt = SQL_GENERATION_PROMPT.format(
            table_descriptions=table_desc,
            question=question,
        )
        llm = Ollama(base_url=OLLAMA_URL, model="llama3", temperature=0.0)
        sql = llm.invoke(prompt).strip()

        sql = sql.replace("```sql", "").replace("```", "").strip()
        if sql.startswith("SQL:"):
            sql = sql[4:].strip()

        return sql

    def _execute_sql(self, sql: str) -> str:
        """Execute SQL against Trino and return formatted results."""
        from sqlalchemy import create_engine, text

        engine = create_engine(f"trino://admin@{TRINO_HOST}:{TRINO_PORT}/iceberg/gold")

        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql))
                columns = list(result.keys())
                rows = result.fetchall()

                if not rows:
                    return "No data found for this query."

                header = " | ".join(columns)
                separator = "-|-".join(["-" * len(col) for col in columns])
                data_rows = []
                for row in rows[:100]:
                    data_rows.append(" | ".join([str(val) for val in row]))

                formatted = f"{header}\n{separator}\n" + "\n".join(data_rows)
                return f"Query returned {len(rows)} rows:\n\n{formatted}"

        except Exception as e:
            return f"Query execution error: {str(e)}"

    def _run(self, query: str) -> str:
        """Run the tool: NL → SQL → Execute → Format."""
        try:
            sql = self._generate_sql(query)
            logger.info(f"Generated SQL: {sql}")

            if not self._validate_sql(sql):
                return "⚠️ Generated query contains disallowed operations. Only SELECT queries are permitted."

            result = self._execute_sql(sql)
            return f"**SQL Executed:**\n```sql\n{sql}\n```\n\n**Results:**\n{result}"

        except Exception as e:
            logger.error(f"Trino tool error: {e}")
            return f"Error processing query: {str(e)}"

    async def _arun(self, query: str) -> str:
        """Async version — falls back to sync."""
        return self._run(query)
