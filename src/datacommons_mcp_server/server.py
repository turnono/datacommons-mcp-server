"""
DataCommons MCP Server for Smithery
Provides access to DataCommons statistical data through MCP protocol
"""

import os
from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field
from smithery.decorators import smithery
from datacommons_mcp.server import search_indicators, get_observations, validate_child_place_types


# Configuration schema for DataCommons API key
class ConfigSchema(BaseModel):
    dc_api_key: str = Field(..., description="DataCommons API key for accessing statistical data")


@smithery.server(config_schema=ConfigSchema)
def create_server():
    """Create and configure the DataCommons MCP server."""
    
    # Create a FastMCP server
    server = FastMCP("DataCommons MCP Server")
    
    # Add DataCommons tools
    @server.tool()
    def search_indicators_tool(query: str, ctx: Context) -> str:
        """Search for available variables and topics in DataCommons."""
        # Set API key from session config
        session_config = ctx.session_config
        if session_config and hasattr(session_config, 'dc_api_key'):
            os.environ['DC_API_KEY'] = session_config.dc_api_key
        
        # Call the DataCommons search function
        result = search_indicators(query)
        return str(result)
    
    @server.tool()
    def get_observations_tool(variable_dcid: str, place_dcid: str, ctx: Context) -> str:
        """Fetch statistical data for a given variable and place."""
        # Set API key from session config
        session_config = ctx.session_config
        if session_config and hasattr(session_config, 'dc_api_key'):
            os.environ['DC_API_KEY'] = session_config.dc_api_key
        
        # Call the DataCommons observations function
        result = get_observations(variable_dcid, place_dcid)
        return str(result)
    
    @server.tool()
    def validate_child_place_types_tool(parent_place: str, child_place_type: str, ctx: Context) -> str:
        """Validate child place types for a given parent place."""
        # Set API key from session config
        session_config = ctx.session_config
        if session_config and hasattr(session_config, 'dc_api_key'):
            os.environ['DC_API_KEY'] = session_config.dc_api_key
        
        # Call the DataCommons validation function
        result = validate_child_place_types(parent_place, child_place_type)
        return str(result)
    
    return server
