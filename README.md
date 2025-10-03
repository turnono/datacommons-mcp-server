# DataCommons MCP Server

A Model Context Protocol (MCP) server for accessing Data Commons API data.

## Features

- Search for indicators and topics
- Get observations and data
- Support for various data formats and chart configurations
- HTTP and stdio transport modes

## Installation

### Using pip

```bash
pip install -r requirements.txt
pip install -e .
```

### Using Docker

```bash
docker build -t datacommons-mcp .
docker run -p 8000:8000 datacommons-mcp
```

## Usage

### CLI Commands

Start the server in HTTP mode:

```bash
python -m datacommons_mcp.cli serve http --host 0.0.0.0 --port 8000
```

Start the server in stdio mode:

```bash
python -m datacommons_mcp.cli serve stdio
```

### Environment Variables

- `GOOGLE_API_KEY`: Your Google API key for Data Commons access

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

Format code:

```bash
black .
isort .
```

## License

MIT License
ok
