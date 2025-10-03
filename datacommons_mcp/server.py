# Copyright 2025 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Server module for the DC MCP server.
"""

import asyncio
import logging
import types
from typing import Union, get_args, get_origin

from mcp.server.fastmcp import Context, FastMCP
from pydantic import ValidationError
from smithery.decorators import smithery
from pydantic import BaseModel, Field

import datacommons_mcp.settings as settings
from datacommons_mcp.clients import create_dc_client
from datacommons_mcp.data_models.charts import (
    CHART_CONFIG_MAP,
    DataCommonsChartConfig,
    HierarchyLocation,
    MultiPlaceLocation,
    SinglePlaceLocation,
    SingleVariableChart,
)
from datacommons_mcp.data_models.observations import (
    ObservationDateType,
    ObservationToolResponse,
)
from datacommons_mcp.data_models.search import (
    SearchResponse,
)
from datacommons_mcp.services import (
    get_observations as get_observations_service,
)
from datacommons_mcp.services import (
    search_indicators as search_indicators_service,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration schema for Smithery
class ConfigSchema(BaseModel):
    dc_api_key: str = Field(..., description="Data Commons API key for accessing the Data Commons API")

@smithery.server(config_schema=ConfigSchema)
def create_server():
    """Create and return a FastMCP server instance with session config."""
    
    server = FastMCP(name="DataCommons MCP Server")

    @server.tool()
    async def get_observations(
        variable_dcid: str,
        place_dcid: str,
        child_place_type: str | None = None,
        source_override: str | None = None,
        date: str = ObservationDateType.LATEST.value,
        date_range_start: str | None = None,
        date_range_end: str | None = None,
        ctx: Context = None,
    ) -> ObservationToolResponse:
        """
        Get observations for a variable and place from Data Commons.

        This tool retrieves statistical observations (data points) for a specific
        variable (statistical measure) and place (geographic location) from the
        Data Commons knowledge graph.

        Args:
            variable_dcid: The DCID of the statistical variable to get data for
            place_dcid: The DCID of the place to get data for
            child_place_type: Optional child place type to get data for
            source_override: Optional source to override the default data source
            date: Date to get data for (default: latest available)
            date_range_start: Start date for date range queries
            date_range_end: End date for date range queries
            ctx: Smithery context for accessing session configuration

        Returns:
            ObservationToolResponse containing the requested observations
        """
        # Get session configuration
        session_config = ctx.session_config if ctx else None
        api_key = session_config.dc_api_key if session_config else None
        
        if not api_key:
            raise ValueError("DC_API_KEY is required. Please configure it in your session settings.")
        
        # Create DC client with the API key
        dc_settings = settings.BaseDCSettings(DC_API_KEY=api_key)
        dc_client = create_dc_client(dc_settings)
        
        # Call the real get_observations service
        return await get_observations_service(
            client=dc_client,
            variable_dcid=variable_dcid,
            place_dcid=place_dcid,
            child_place_type=child_place_type,
            source_override=source_override,
            date=date,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )

    @server.tool()
    async def search_indicators(
        query: str,
        places: list[str] | None = None,
        per_search_limit: int = 10,
        include_topics: bool = True,
        maybe_bilateral: bool = False,
        ctx: Context = None,
    ) -> SearchResponse:
        """
        Search for data indicators and topics in Data Commons.

        This tool searches the Data Commons knowledge graph for statistical
        variables (indicators) and topics that match the given query. It can
        optionally filter results by specific places.

        Args:
            query: Search query string
            places: Optional list of place DCIDs to filter results
            per_search_limit: Maximum number of results per search (default: 10)
            include_topics: Whether to include topic results (default: True)
            maybe_bilateral: Whether to include bilateral indicators (default: False)
            ctx: Smithery context for accessing session configuration

        Returns:
            SearchResponse containing matching indicators and topics
        """
        # Get session configuration
        session_config = ctx.session_config if ctx else None
        api_key = session_config.dc_api_key if session_config else None
        
        if not api_key:
            raise ValueError("DC_API_KEY is required. Please configure it in your session settings.")
        
        # Create DC client with the API key
        dc_settings = settings.BaseDCSettings(DC_API_KEY=api_key)
        dc_client = create_dc_client(dc_settings)
        
        # Call the real search_indicators service
        return await search_indicators_service(
            client=dc_client,
            query=query,
            places=places,
            per_search_limit=per_search_limit,
            include_topics=include_topics,
            maybe_bilateral=maybe_bilateral,
        )

    return server
