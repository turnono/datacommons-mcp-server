"""
DataCommons MCP Server for Smithery
Provides access to DataCommons statistical data through MCP protocol
"""

import os
from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field
from smithery.decorators import smithery
from datacommons_mcp.server import FastMCP as DataCommonsFastMCP


# Configuration schema for DataCommons API key
class ConfigSchema(BaseModel):
    dc_api_key: str = Field(..., description="DataCommons API key for accessing statistical data")


@smithery.server(config_schema=ConfigSchema)
def create_server():
    """Create and configure the DataCommons MCP server."""
    
    # Create the DataCommons MCP server
    server = DataCommonsFastMCP("DataCommons MCP Server")
    
    return server
