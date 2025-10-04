# Use Python 3.11 slim image (Debian-based, supports sh shell)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Copy package configuration first for better Docker layer caching
COPY pyproject.toml ./

# Install the package and dependencies
RUN pip install --no-cache-dir -e .

# Copy the rest of the application
COPY . .

# Expose port 8000 for the Streamable HTTP server
EXPOSE 8000

# Start the MCP server using the CLI
CMD ["python", "-m", "datacommons_mcp.cli", "serve", "http", "--host", "0.0.0.0", "--port", "8000"]
