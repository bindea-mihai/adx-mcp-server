#!/usr/bin/env python

import os
import json
import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

import dotenv
from fastmcp import FastMCP
from azure.identity import DefaultAzureCredential, WorkloadIdentityCredential, ClientSecretCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

# Set up logging
logger = logging.getLogger(__name__)

dotenv.load_dotenv()
mcp = FastMCP("Azure Data Explorer MCP")

class TransportType(str, Enum):
    """Supported MCP server transport types."""

    STDIO = "stdio"
    HTTP = "http"
    SSE = "sse"

    @classmethod
    def values(cls) -> list[str]:
        """Get all valid transport values."""
        return [transport.value for transport in cls]

@dataclass
class MCPServerConfig:
    """Global Configuration for MCP."""
    mcp_server_transport: TransportType = None
    mcp_bind_host: str = None
    mcp_bind_port: int = None

    def __post_init__(self):
        """Validate mcp configuration."""
        if not self.mcp_server_transport:
            raise ValueError("MCP SERVER TRANSPORT is required")
        if not self.mcp_bind_host:
            raise ValueError(f"MCP BIND HOST is required")
        if not self.mcp_bind_port:
            raise ValueError(f"MCP BIND PORT is required")

@dataclass
class ADXConfig:
    cluster_url: str
    database: str
    # Optional Custom MCP Server Configuration
    mcp_server_config: Optional[MCPServerConfig] = None

config = ADXConfig(
    cluster_url=os.environ.get("ADX_CLUSTER_URL", ""),
    database=os.environ.get("ADX_DATABASE", ""),
    mcp_server_config=MCPServerConfig(
        mcp_server_transport=os.environ.get("ADX_MCP_SERVER_TRANSPORT", "stdio").lower(),
        mcp_bind_host=os.environ.get("ADX_MCP_BIND_HOST", "127.0.0.1"),
        mcp_bind_port=int(os.environ.get("ADX_MCP_BIND_PORT", "8080"))
    )
)

# NOTE: N8N Parameter Filtering
# N8N sometimes sends a 'toolCallId' parameter that is not expected by tools.
# FastMCP 2.11.3+ does not support middleware for parameter filtering.
# If N8N compatibility issues arise, consider:
# 1. Upgrading to a FastMCP version with middleware support
# 2. Implementing parameter filtering at the tool level using **kwargs
# 3. Configuring N8N to not send the toolCallId parameter

def get_kusto_client() -> KustoClient:
    # Get authentication credentials from environment variables
    tenant_id = os.environ.get('AZURE_TENANT_ID')
    client_id = os.environ.get('AZURE_CLIENT_ID')
    client_secret = os.environ.get('AZURE_CLIENT_SECRET')
    token_file_path = os.environ.get('ADX_TOKEN_FILE_PATH', '/var/run/secrets/azure/tokens/azure-identity-token')
    
    # Priority 1: Service Principal with Client Secret
    if tenant_id and client_id and client_secret:
        print(f"Using ClientSecretCredential with client_id: {client_id}")
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
    # Priority 2: Workload Identity (Kubernetes)
    elif tenant_id and client_id and not client_secret:
        print(f"Using WorkloadIdentityCredential with client_id: {client_id}")
        try:
            credential = WorkloadIdentityCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                token_file_path=token_file_path
            )
        except Exception as e:
            print(f"Error initializing WorkloadIdentityCredential: {str(e)}")
            print("Falling back to DefaultAzureCredential")
            credential = DefaultAzureCredential()
    # Priority 3: Default Azure Credential (Managed Identity, Azure CLI, etc.)
    else:
        print("Using DefaultAzureCredential (Managed Identity, Azure CLI, etc.)")
        credential = DefaultAzureCredential()
    
    kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
        connection_string=config.cluster_url,
        credential=credential
    )
    return KustoClient(kcsb)

def format_query_results(result_set) -> List[Dict[str, Any]]:
    if not result_set or not result_set.primary_results:
        return []
    
    primary_result = result_set.primary_results[0]
    columns = [col.column_name for col in primary_result.columns]
    
    formatted_results = []
    for row in primary_result.rows:
        record = {}
        for i, value in enumerate(row):
            record[columns[i]] = value
        formatted_results.append(record)
    
    return formatted_results

@mcp.tool(description="Executes a Kusto Query Language (KQL) query against the configured Azure Data Explorer database and returns the results as a list of dictionaries.")
async def execute_query(query: str) -> List[Dict[str, Any]]:
    if not config.cluster_url or not config.database:
        raise ValueError("Azure Data Explorer configuration is missing. Please set ADX_CLUSTER_URL and ADX_DATABASE environment variables.")
    
    client = get_kusto_client()
    result_set = client.execute(config.database, query)
    return format_query_results(result_set)

@mcp.tool(description="Retrieves a list of all tables available in the configured Azure Data Explorer database, including their names, folders, and database associations.")
async def list_tables() -> List[Dict[str, Any]]:
    if not config.cluster_url or not config.database:
        raise ValueError("Azure Data Explorer configuration is missing. Please set ADX_CLUSTER_URL and ADX_DATABASE environment variables.")
    
    client = get_kusto_client()
    query = ".show tables | project TableName, Folder, DatabaseName"
    result_set = client.execute(config.database, query)
    return format_query_results(result_set)

@mcp.tool(description="Retrieves the schema information for a specified table in the Azure Data Explorer database, including column names, data types, and other schema-related metadata.")
async def get_table_schema(table_name: str) -> List[Dict[str, Any]]:
    if not config.cluster_url or not config.database:
        raise ValueError("Azure Data Explorer configuration is missing. Please set ADX_CLUSTER_URL and ADX_DATABASE environment variables.")
    
    client = get_kusto_client()
    query = f"{table_name} | getschema"
    result_set = client.execute(config.database, query)
    return format_query_results(result_set)

@mcp.tool(description="Retrieves a random sample of rows from the specified table in the Azure Data Explorer database. The sample_size parameter controls how many rows to return (default: 10).")
async def sample_table_data(table_name: str, sample_size: int = 10) -> List[Dict[str, Any]]:
    if not config.cluster_url or not config.database:
        raise ValueError("Azure Data Explorer configuration is missing. Please set ADX_CLUSTER_URL and ADX_DATABASE environment variables.")
    
    client = get_kusto_client()
    query = f"{table_name} | sample {sample_size}"
    result_set = client.execute(config.database, query)
    return format_query_results(result_set)

@mcp.tool(description="Retrieves table details including TotalRowCount, HotExtentSize")
async def get_table_details(table_name: str) -> List[Dict[str, Any]]:
    if not config.cluster_url or not config.database:
        raise ValueError("Azure Data Explorer configuration is missing. Please set ADX_CLUSTER_URL and ADX_DATABASE environment variables.")
    
    client = get_kusto_client()
    query = f".show table {table_name} details"
    result_set = client.execute(config.database, query)
    return format_query_results(result_set)


if __name__ == "__main__":
    print(f"Starting Azure Data Explorer MCP Server...")
    mcp.run()
